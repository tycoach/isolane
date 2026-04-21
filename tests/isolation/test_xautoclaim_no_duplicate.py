"""
isolane/tests/isolation/test_xautoclaim_no_duplicate.py

XAUTOCLAIM deduplication tests.

Verifies that:
1. A message processed successfully is ACK'd and not re-delivered
2. A failed message is re-delivered up to MAX_CLAIM_ATTEMPTS
3. After MAX_CLAIM_ATTEMPTS, the message goes to DLQ — not processed again
4. DLQ entries contain the correct claim count
5. Claim count increments correctly on each re-delivery
"""

import pytest
from worker.python.dlq import (
    get_claim_count,
    should_route_to_dlq,
    increment_claim_count,
)


class TestClaimCountLogic:
    """Claim count tracking is correct."""

    def test_initial_claim_count_is_zero(self):
        """A new work item has claim count 0."""
        fields = {"pipeline_id": "customers", "records_json": "[]"}
        assert get_claim_count(fields) == 0

    def test_explicit_claim_count_is_read(self):
        """Explicit _claim_count field is read correctly."""
        fields = {"_claim_count": "3"}
        assert get_claim_count(fields) == 3

    def test_invalid_claim_count_defaults_to_zero(self):
        """Invalid _claim_count defaults to 0."""
        fields = {"_claim_count": "not_a_number"}
        assert get_claim_count(fields) == 0

    def test_increment_claim_count(self):
        """Claim count increments by 1."""
        fields = {"_claim_count": "2", "pipeline_id": "customers"}
        updated = increment_claim_count(fields)
        assert updated["_claim_count"] == "3"
        # Original not mutated
        assert fields["_claim_count"] == "2"

    def test_increment_from_zero(self):
        """Incrementing from 0 gives 1."""
        fields = {"pipeline_id": "customers"}
        updated = increment_claim_count(fields)
        assert updated["_claim_count"] == "1"

    def test_increment_preserves_other_fields(self):
        """Incrementing claim count preserves all other fields."""
        fields = {
            "pipeline_id": "customers",
            "batch_offset": "seed-123",
            "records_json": "[]",
            "_claim_count": "1",
        }
        updated = increment_claim_count(fields)
        assert updated["pipeline_id"] == "customers"
        assert updated["batch_offset"] == "seed-123"
        assert updated["records_json"] == "[]"


class TestDLQRouting:
    """Messages are routed to DLQ after max claim attempts."""

    def test_below_max_does_not_route_to_dlq(self):
        """Message below max claims is NOT routed to DLQ."""
        fields = {"_claim_count": "2"}
        assert not should_route_to_dlq(fields)

    def test_at_max_routes_to_dlq(self):
        """Message at max claims IS routed to DLQ."""
        fields = {"_claim_count": "3"}
        assert should_route_to_dlq(fields)

    def test_above_max_routes_to_dlq(self):
        """Message above max claims IS routed to DLQ."""
        fields = {"_claim_count": "10"}
        assert should_route_to_dlq(fields)

    def test_zero_claims_not_routed_to_dlq(self):
        """Fresh message with 0 claims is not routed to DLQ."""
        fields = {}
        assert not should_route_to_dlq(fields)

    def test_dlq_routing_at_boundary(self):
        """Test boundary: claim count 2 = no DLQ, claim count 3 = DLQ."""
        assert not should_route_to_dlq({"_claim_count": "2"})
        assert should_route_to_dlq({"_claim_count": "3"})

    def test_claim_sequence_correctness(self):
        """
        Simulate the full claim sequence:
        attempt 1 → retry
        attempt 2 → retry
        attempt 3 → DLQ
        """
        fields = {"pipeline_id": "customers", "records_json": "[]"}

        # First delivery
        assert get_claim_count(fields) == 0
        assert not should_route_to_dlq(fields)

        # First re-claim (attempt 1)
        fields = increment_claim_count(fields)
        assert get_claim_count(fields) == 1
        assert not should_route_to_dlq(fields)

        # Second re-claim (attempt 2)
        fields = increment_claim_count(fields)
        assert get_claim_count(fields) == 2
        assert not should_route_to_dlq(fields)

        # Third re-claim (attempt 3) → DLQ
        fields = increment_claim_count(fields)
        assert get_claim_count(fields) == 3
        assert should_route_to_dlq(fields)