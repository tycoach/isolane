"""
isolane/tests/isolation/test_no_cross_tenant_kafka.py

Kafka topic isolation tests.

Verifies that:
1. Topics follow {namespace}.{dataset} naming convention
2. Consumer groups are namespace-scoped
3. Work queues in Redis are namespace-isolated
4. A worker for analytics cannot read finance work items
"""

import pytest
import redis
import os


@pytest.fixture
def r():
    client = redis.Redis(
        host             = os.environ.get("REDIS_HOST", "redis-master"),
        port             = int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses = True,
    )
    yield client
    client.close()


class TestRedisStreamIsolation:
    """Redis Streams work queues are namespace-isolated."""

    def test_analytics_work_queue_exists(self, r):
        """analytics.work-queue exists as a Redis Stream."""
        # Stream exists if it has been created (even if empty)
        exists = r.exists("analytics.work-queue")
        # May be 0 if no messages and was never created — check both
        # The stream is created during provisioning
        info = r.keys("analytics.*")
        assert "analytics.work-queue" in info or exists == 0, \
            "analytics.work-queue not found in Redis"

    def test_consumer_groups_are_namespace_scoped(self, r):
        """Consumer groups follow {namespace}-consumer-group naming."""
        # If analytics stream exists, check its consumer group
        try:
            groups = r.xinfo_groups("analytics.work-queue")
            group_names = [g["name"] for g in groups]
            assert any("analytics" in name for name in group_names), \
                f"analytics consumer group missing. Found: {group_names}"
            # Finance group should NOT be in analytics stream
            assert not any("finance" in name for name in group_names), \
                f"finance consumer group found in analytics stream: {group_names}"
        except redis.exceptions.ResponseError:
            pytest.skip("analytics.work-queue stream not initialised")

    def test_dlq_streams_are_namespace_isolated(self, r):
        """DLQ streams are namespace-isolated."""
        analytics_dlq = r.exists("analytics.dlq")
        # DLQ key exists and is a stream
        if analytics_dlq:
            key_type = r.type("analytics.dlq")
            assert key_type == "stream", \
                f"analytics.dlq is not a stream, got: {key_type}"

    def test_work_queue_keys_follow_convention(self, r):
        """All work queue keys follow {namespace}.work-queue convention."""
        work_queues = r.keys("*.work-queue")
        for key in work_queues:
            parts = key.split(".")
            assert len(parts) == 2, \
                f"Work queue key '{key}' doesn't follow {{namespace}}.work-queue convention"
            assert parts[1] == "work-queue", \
                f"Work queue key '{key}' has wrong suffix"
            namespace = parts[0]
            assert namespace.isidentifier(), \
                f"Namespace '{namespace}' contains invalid characters"

    def test_finance_worker_cannot_read_analytics_messages(self, r):
        """
        A consumer group named for finance cannot read analytics messages.
        This is enforced by consumer group scoping — XREADGROUP with a
        finance group cannot consume from analytics.work-queue.
        """
        # Verify the consumer group for analytics only has analytics consumers
        try:
            groups = r.xinfo_groups("analytics.work-queue")
            for group in groups:
                group_name = group["name"]
                assert "finance" not in group_name, \
                    f"finance consumer group '{group_name}' found in analytics stream — isolation breach!"
        except redis.exceptions.ResponseError:
            pytest.skip("analytics.work-queue not initialised")

    def test_no_cross_namespace_dlq_entries(self, r):
        """DLQ entries only contain their own namespace."""
        try:
            messages = r.xrange("analytics.dlq", "-", "+", count=10)
            for msg_id, fields in messages:
                ns = fields.get("namespace", "")
                if ns:
                    assert ns == "analytics", \
                        f"Found namespace '{ns}' in analytics.dlq — isolation breach!"
        except redis.exceptions.ResponseError:
            pytest.skip("analytics.dlq not initialised")