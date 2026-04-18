"""
isolane/engine/rocksdb_client.py

RocksDB column family handle registry.
"""

import os
import threading
from typing import Optional

try:
    import rocksdb
    ROCKSDB_AVAILABLE = True
except ImportError:
    ROCKSDB_AVAILABLE = False


class RocksDBClient:
    """
    Thread-safe RocksDB client with column family isolation per namespace.
    """

    def __init__(self, path: str):
        self.path    = path
        self._lock   = threading.Lock()
        self._cf_handles: dict[str, any]  = {}
        self._db: Optional[any]           = None
        self._memory_fallback: dict[str, dict[str, bytes]] = {}
        self._use_rocksdb = ROCKSDB_AVAILABLE

        if self._use_rocksdb:
            self._init_rocksdb()
        else:
            print(
                "[rocksdb] rocksdb package not found — "
                "using in-memory fallback (dev/test only)"
            )

    # ── Initialisation ────────────────────────────────────────────

    def _init_rocksdb(self) -> None:
        """
        Open RocksDB with all existing column families.
        Called once at startup.
        """
        os.makedirs(self.path, exist_ok=True)

        opts = rocksdb.Options()
        opts.create_if_missing             = True
        opts.create_missing_column_families = True

        # Discover existing column families
        try:
            existing_cfs = rocksdb.list_column_families(
                self.path.encode(), opts
            )
        except Exception:
            existing_cfs = [b"default"]

        cf_names      = [cf.decode() for cf in existing_cfs]
        cf_descriptors = [
            rocksdb.ColumnFamilyDescriptor(
                cf.encode(),
                rocksdb.ColumnFamilyOptions()
            )
            for cf in cf_names
        ]

        self._db, cf_handles = rocksdb.DB.open_for_read_write_with_cf(
            self.path.encode(),
            opts,
            cf_descriptors,
        )

        # Map CF name → handle
        for name, handle in zip(cf_names, cf_handles):
            self._cf_handles[name] = handle

    def _get_or_create_cf(self, namespace: str) -> any:
        """
        Return the column family handle for a namespace.
   
        """
        cf_name = f"ns_{namespace}"

        if cf_name in self._cf_handles:
            return self._cf_handles[cf_name]

        with self._lock:
            # Double-check after acquiring lock
            if cf_name in self._cf_handles:
                return self._cf_handles[cf_name]

            if self._use_rocksdb:
                # Create column family
                handle = self._db.create_column_family(
                    cf_name.encode(),
                    rocksdb.ColumnFamilyOptions(),
                )
                self._cf_handles[cf_name] = handle
                print(f"[rocksdb] Column family '{cf_name}' created")
            else:
                # In-memory fallback
                self._memory_fallback[cf_name] = {}
                self._cf_handles[cf_name]      = cf_name

            return self._cf_handles[cf_name]

    # ── Startup assertion ─────────────────────────────────────────

    def assert_namespace(self, namespace: str) -> None:
        """
        Assert that a column family exists for this namespace.

        """
        if not self._use_rocksdb:
            cf_name = f"ns_{namespace}"
            if cf_name not in self._memory_fallback:
                self._memory_fallback[cf_name] = {}
            print(f"[rocksdb] namespace '{namespace}' asserted (in-memory)")
            return

        if self._db is None:
            raise RuntimeError(
                "RocksDB is not initialised. "
                "Check that the data directory is accessible."
            )

        self._get_or_create_cf(namespace)
        print(f"[rocksdb] namespace '{namespace}' asserted OK")

    # ── Core operations ───────────────────────────────────────────


    def put(self, namespace: str, key: str, value: bytes) -> None:
        """
        Write a value to the namespace's column family.
        key and value are strings/bytes scoped to this namespace only.
        """
        if self._use_rocksdb:
            cf = self._get_or_create_cf(namespace)
            self._db.put(key.encode(), value, cf)
        else:
            cf_name = f"ns_{namespace}"
            if cf_name not in self._memory_fallback:
                self._memory_fallback[cf_name] = {}
            self._memory_fallback[cf_name][key] = value

    def get(self, namespace: str, key: str) -> Optional[bytes]:
        """
        Read a value from the namespace's column family.
        Returns None if the key does not exist.
        """
        if self._use_rocksdb:
            cf  = self._get_or_create_cf(namespace)
            val = self._db.get(key.encode(), cf)
            return val
        else:
            cf_name = f"ns_{namespace}"
            return self._memory_fallback.get(cf_name, {}).get(key)

    def delete(self, namespace: str, key: str) -> None:
        """Delete a key from the namespace's column family."""
        if self._use_rocksdb:
            cf = self._get_or_create_cf(namespace)
            self._db.delete(key.encode(), cf)
        else:
            cf_name = f"ns_{namespace}"
            self._memory_fallback.get(cf_name, {}).pop(key, None)

    def get_str(self, namespace: str, key: str) -> Optional[str]:
        """Convenience: get and decode a UTF-8 string value."""
        val = self.get(namespace, key)
        return val.decode("utf-8") if val is not None else None

    def put_str(self, namespace: str, key: str, value: str) -> None:
        """Convenience: encode and put a UTF-8 string value."""
        self.put(namespace, key, value.encode("utf-8"))

    # ── Offset tracking ───────────────────────────────────────────

    def get_last_offset(self, namespace: str, pipeline_id: str) -> Optional[str]:
        """
        Get the last successfully processed Redis Streams message ID
        for a pipeline. Used to resume processing after a restart.
        """
        return self.get_str(namespace, f"offset:{pipeline_id}")

    def set_last_offset(
        self, namespace: str, pipeline_id: str, offset: str
    ) -> None:
        """Record the last successfully processed offset for a pipeline."""
        self.put_str(namespace, f"offset:{pipeline_id}", offset)

    # ── Schema cache ──────────────────────────────────────────────

    def get_schema(self, namespace: str, pipeline_id: str) -> Optional[bytes]:
        """
        Get the cached schema for a pipeline (serialised as JSON bytes).
        Used to detect schema drift between batches.
        """
        return self.get(namespace, f"schema:{pipeline_id}")

    def set_schema(
        self, namespace: str, pipeline_id: str, schema_json: bytes
    ) -> None:
        """Cache the current schema for a pipeline."""
        self.put(namespace, f"schema:{pipeline_id}", schema_json)

    # ── Dimension cache ───────────────────────────────────────────

    def get_dim(self, namespace: str, dim_key: str) -> Optional[bytes]:
        """
        Get a cached dimension value (e.g. customer lookup).
        Key format: {dim_name}:{natural_key_value}
        """
        return self.get(namespace, f"dim:{dim_key}")

    def set_dim(
        self, namespace: str, dim_key: str, value: bytes
    ) -> None:
        """Cache a dimension value."""
        self.put(namespace, f"dim:{dim_key}", value)

    # ── Cleanup ───────────────────────────────────────────────────

    def close(self) -> None:
        """Close the RocksDB connection. Call on worker shutdown."""
        if self._use_rocksdb and self._db is not None:
            self._db.close()
            self._db = None
            print("[rocksdb] Connection closed")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ── Module-level singleton ────────────────────────────────────────
# One RocksDB client per worker process.
# Initialised once and shared across all batch processing calls.

_client: Optional[RocksDBClient] = None


def get_client() -> RocksDBClient:
    """
    Return the module-level RocksDB client singleton.
    Initialises it on first call using ROCKSDB_PATH env var.
    """
    global _client
    if _client is None:
        path    = os.environ.get("ROCKSDB_PATH", "./data/rocksdb")
        _client = RocksDBClient(path)
    return _client


def close_client() -> None:
    """Close the singleton client. Call on process shutdown."""
    global _client
    if _client is not None:
        _client.close()
        _client = None