"""
isolane/provisioner/state.py

Provisioning log state machine.

Every provisioning step goes through this module before and after
execution. The state machine guarantees:

  - Each step is recorded BEFORE execution (PENDING)
  - On success the step is marked DONE with its input hash
  - On failure the step is marked FAILED with the error message
  - Re-running skips DONE steps whose input hash matches
  - Re-running re-executes DONE steps whose input hash differs (drift)
  - Re-running retries FAILED steps from where they stopped

Input hashing:
  The input hash is SHA-256 of the step inputs serialised as
  canonical JSON (keys sorted, ASCII-safe). If a DONE step's
  recorded hash differs from the current inputs, the step is
  re-executed to correct drift.
"""

import hashlib
import json
from dataclasses import dataclass
from typing import Literal, Optional
from uuid import UUID

import asyncpg

Decision = Literal["run", "retry", "rerun", "skip"]


@dataclass
class StepRecord:
    step_name:     str
    step_order:    int
    status:        str
    input_hash:    Optional[str]
    error_message: Optional[str]


class StepState:
    """
    Manages provisioning_log reads and writes for one tenant.
    Passed into every step function so steps can record their
    own status without knowing about the DB schema.
    """

    def __init__(self, tenant_id: UUID, conn: asyncpg.Connection):
        self.tenant_id = tenant_id
        self.conn      = conn

    async def should_run(
        self,
        step_name: str,
        inputs:    dict,
    ) -> tuple[Decision, str]:
        """
        Determine whether a step should run and return its input hash.

        Returns (decision, input_hash):
          "skip"   — DONE, inputs unchanged. Do nothing.
          "rerun"  — DONE but inputs changed (drift). Re-execute.
          "retry"  — FAILED or PENDING. Re-execute.
          "run"    — No record yet. First execution.
        """
        input_hash = _hash(inputs)
        record     = await self._fetch(step_name)

        if record is None:
            return "run", input_hash

        if record.status == "DONE":
            if record.input_hash == input_hash:
                return "skip", input_hash
            return "rerun", input_hash

        return "retry", input_hash

    async def mark_pending(
        self,
        step_name:  str,
        step_order: int,
        input_hash: str,
    ) -> None:
        """Record PENDING before executing. Safe to call on re-runs."""
        await self.conn.execute(
            """
            INSERT INTO provisioning_log
                (tenant_id, step_name, step_order, status, input_hash, started_at)
            VALUES ($1, $2, $3, 'PENDING', $4, NOW())
            ON CONFLICT (tenant_id, step_name) DO UPDATE SET
                status        = 'PENDING',
                input_hash    = EXCLUDED.input_hash,
                started_at    = NOW(),
                error_message = NULL,
                completed_at  = NULL
            """,
            self.tenant_id, step_name, step_order, input_hash,
        )

    async def mark_done(self, step_name: str, input_hash: str) -> None:
        await self.conn.execute(
            """
            UPDATE provisioning_log
            SET status = 'DONE', input_hash = $1, completed_at = NOW()
            WHERE tenant_id = $2 AND step_name = $3
            """,
            input_hash, self.tenant_id, step_name,
        )

    async def mark_failed(self, step_name: str, error: str) -> None:
        await self.conn.execute(
            """
            UPDATE provisioning_log
            SET status = 'FAILED', error_message = $1, completed_at = NOW()
            WHERE tenant_id = $2 AND step_name = $3
            """,
            error, self.tenant_id, step_name,
        )

    async def all_steps(self) -> list[StepRecord]:
        rows = await self.conn.fetch(
            """
            SELECT step_name, step_order, status, input_hash, error_message
            FROM provisioning_log
            WHERE tenant_id = $1
            ORDER BY step_order ASC
            """,
            self.tenant_id,
        )
        return [StepRecord(**dict(r)) for r in rows]

    async def is_complete(self) -> bool:
        steps = await self.all_steps()
        return len(steps) > 0 and all(s.status == "DONE" for s in steps)

    async def _fetch(self, step_name: str) -> Optional[StepRecord]:
        row = await self.conn.fetchrow(
            """
            SELECT step_name, step_order, status, input_hash, error_message
            FROM provisioning_log
            WHERE tenant_id = $1 AND step_name = $2
            """,
            self.tenant_id, step_name,
        )
        return StepRecord(**dict(row)) if row else None


def _hash(inputs: dict) -> str:
    """SHA-256 of canonical JSON. Same inputs always produce same hash."""
    canonical = json.dumps(inputs, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(canonical.encode()).hexdigest()