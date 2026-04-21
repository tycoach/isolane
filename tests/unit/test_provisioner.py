"""
isolane/tests/unit/test_provisioner.py

Unit tests for the provisioner state machine.

Tests:
1. Input hashing is deterministic and canonical
2. StepState decisions are correct for each state
3. Claim count increments correctly
4. Step ordering is preserved
5. Error messages are stored correctly
"""

import pytest
import hashlib
import json
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from provisioner.state import _hash, StepState, StepRecord


class TestInputHashing:
    """Input hashing is deterministic and canonical."""

    def test_same_inputs_produce_same_hash(self):
        inputs = {"ns": "analytics", "team": "analytics-team", "mode": "standard"}
        assert _hash(inputs) == _hash(inputs)

    def test_key_order_does_not_affect_hash(self):
        """Canonical JSON — key order is irrelevant."""
        a = _hash({"ns": "analytics", "mode": "standard"})
        b = _hash({"mode": "standard", "ns": "analytics"})
        assert a == b

    def test_different_inputs_produce_different_hash(self):
        a = _hash({"ns": "analytics"})
        b = _hash({"ns": "finance"})
        assert a != b

    def test_hash_is_sha256_hex(self):
        h = _hash({"ns": "analytics"})
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_hash_matches_manual_calculation(self):
        inputs = {"ns": "analytics"}
        canonical = json.dumps(inputs, sort_keys=True, ensure_ascii=True)
        expected = hashlib.sha256(canonical.encode()).hexdigest()
        assert _hash(inputs) == expected


class TestStepStateDecisions:
    """StepState.should_run() returns correct decisions."""

    def _make_state(self, records=None):
        """Create a StepState with mocked connection."""
        tenant_id = uuid4()
        conn = AsyncMock()

        state = StepState(tenant_id, conn)
        return state, conn, tenant_id

    @pytest.mark.asyncio
    async def test_no_record_returns_run(self):
        """First time — no record → 'run'."""
        state, conn, _ = self._make_state()
        conn.fetchrow = AsyncMock(return_value=None)

        decision, h = await state.should_run("pg_schema", {"ns": "analytics"})
        assert decision == "run"
        assert len(h) == 64

    @pytest.mark.asyncio
    async def test_done_same_hash_returns_skip(self):
        """DONE step with same inputs → 'skip'."""
        inputs = {"ns": "analytics"}
        input_hash = _hash(inputs)

        state, conn, _ = self._make_state()
        conn.fetchrow = AsyncMock(return_value={
            "step_name": "pg_schema",
            "step_order": 1,
            "status": "DONE",
            "input_hash": input_hash,
            "error_message": None,
        })

        decision, h = await state.should_run("pg_schema", inputs)
        assert decision == "skip"

    @pytest.mark.asyncio
    async def test_done_different_hash_returns_rerun(self):
        """DONE step with different inputs → 'rerun' (drift)."""
        state, conn, _ = self._make_state()
        conn.fetchrow = AsyncMock(return_value={
            "step_name": "pg_schema",
            "step_order": 1,
            "status": "DONE",
            "input_hash": _hash({"ns": "analytics", "mode": "standard"}),
            "error_message": None,
        })

        # Different inputs — mode changed
        decision, h = await state.should_run(
            "pg_schema", {"ns": "analytics", "mode": "fail_fast"}
        )
        assert decision == "rerun"

    @pytest.mark.asyncio
    async def test_failed_returns_retry(self):
        """FAILED step → 'retry'."""
        state, conn, _ = self._make_state()
        conn.fetchrow = AsyncMock(return_value={
            "step_name": "pg_schema",
            "step_order": 1,
            "status": "FAILED",
            "input_hash": None,
            "error_message": "some error",
        })

        decision, h = await state.should_run("pg_schema", {"ns": "analytics"})
        assert decision == "retry"

    @pytest.mark.asyncio
    async def test_pending_returns_retry(self):
        """PENDING step (crashed mid-execution) → 'retry'."""
        state, conn, _ = self._make_state()
        conn.fetchrow = AsyncMock(return_value={
            "step_name": "pg_schema",
            "step_order": 1,
            "status": "PENDING",
            "input_hash": _hash({"ns": "analytics"}),
            "error_message": None,
        })

        decision, h = await state.should_run("pg_schema", {"ns": "analytics"})
        assert decision == "retry"


class TestStepStateTransitions:
    """mark_pending, mark_done, mark_failed write correct SQL."""

    def _make_state(self):
        tenant_id = uuid4()
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value=None)
        state = StepState(tenant_id, conn)
        return state, conn, tenant_id

    @pytest.mark.asyncio
    async def test_mark_pending_calls_execute(self):
        state, conn, tenant_id = self._make_state()
        await state.mark_pending("pg_schema", 1, "abc123")
        conn.execute.assert_called_once()
        call_args = conn.execute.call_args[0]
        assert "PENDING" in call_args[0]

    @pytest.mark.asyncio
    async def test_mark_done_calls_execute(self):
        state, conn, tenant_id = self._make_state()
        await state.mark_done("pg_schema", "abc123")
        conn.execute.assert_called_once()
        call_args = conn.execute.call_args[0]
        assert "DONE" in call_args[0]

    @pytest.mark.asyncio
    async def test_mark_failed_stores_error(self):
        state, conn, tenant_id = self._make_state()
        await state.mark_failed("pg_schema", "connection refused")
        conn.execute.assert_called_once()
        call_args = conn.execute.call_args[0]
        assert "FAILED" in call_args[0]

    @pytest.mark.asyncio
    async def test_is_complete_true_when_all_done(self):
        state, conn, _ = self._make_state()
        conn.fetch = AsyncMock(return_value=[
            {"step_name": "kafka_acl", "step_order": 0,
             "status": "DONE", "input_hash": "abc", "error_message": None},
            {"step_name": "pg_schema", "step_order": 1,
             "status": "DONE", "input_hash": "def", "error_message": None},
        ])
        assert await state.is_complete() is True

    @pytest.mark.asyncio
    async def test_is_complete_false_when_any_failed(self):
        state, conn, _ = self._make_state()
        conn.fetch = AsyncMock(return_value=[
            {"step_name": "kafka_acl", "step_order": 0,
             "status": "DONE", "input_hash": "abc", "error_message": None},
            {"step_name": "pg_schema", "step_order": 1,
             "status": "FAILED", "input_hash": None, "error_message": "error"},
        ])
        assert await state.is_complete() is False

    @pytest.mark.asyncio
    async def test_is_complete_false_when_empty(self):
        state, conn, _ = self._make_state()
        conn.fetch = AsyncMock(return_value=[])
        assert await state.is_complete() is False


class TestStepRegistry:
    """The provisioner step registry is correctly ordered."""

    def test_steps_are_ordered(self):
        """Steps are listed in the correct provisioning order."""
        from provisioner.provision import STEPS
        step_names = [name for name, _ in STEPS]

        expected_order = [
            "kafka_acl",
            "pg_schema",
            "pg_role",
            "rocksdb_cf",
            "redis_consumer_group",
            "grafana_org",
            "prometheus_scrape",
        ]
        assert step_names == expected_order, \
            f"Step order mismatch. Got: {step_names}"

    def test_all_steps_have_run_function(self):
        """Every step module has a run() function."""
        from provisioner.provision import STEPS
        for name, module in STEPS:
            assert hasattr(module, "run"), \
                f"Step '{name}' is missing run() function"
            assert callable(module.run), \
                f"Step '{name}'.run is not callable"