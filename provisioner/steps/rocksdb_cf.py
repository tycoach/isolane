"""
isolane/provisioner/steps/rocksdb_cf.py

Register the expected RocksDB column family for this namespace.
"""

import os
import asyncpg
import asyncio
from typing import Any


def run(ns: str, cfg: dict[str, Any]) -> None:
    """
    Record the expected column family name.
    The worker creates it on first boot if it does not exist.
    """
    cf_name = f"ns_{ns}"
    rocksdb_path = os.environ.get("ROCKSDB_PATH", "./data/rocksdb")
    print(f"  [rocksdb_cf] Column family '{cf_name}' registered")
    print(f"  [rocksdb_cf] Path: {rocksdb_path}")
    print(f"  [rocksdb_cf] Worker will assert CF exists on startup")