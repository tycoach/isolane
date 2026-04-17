"""
isolane/provisioner/steps/kafka_acl.py

Create Kafka service account ACL rules for a namespace.
"""

import os
import subprocess
from typing import Any


def run(ns: str, cfg: dict[str, Any]) -> None:
    """
    Idempotent — Kafka ACLs are safe to re-apply.
    Re-running adds if missing, leaves existing rules untouched.
    """
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    principal = f"User:{ns}-svc"

    # Topic prefix ACLs
    for operation in ("Write", "Read"):
        _kafka_acl(
            bootstrap,
            principal=principal,
            operation=operation,
            resource_type="topic",
            resource_name=f"{ns}.",
            pattern_type="PREFIXED",
        )

    # Consumer group ACL
    _kafka_acl(
        bootstrap,
        principal=principal,
        operation="Describe",
        resource_type="group",
        resource_name=f"{ns}-consumer-group",
        pattern_type="LITERAL",
    )

    print(f"  [kafka_acl] ACLs set for principal {principal}")


def _kafka_acl(
    bootstrap:     str,
    principal:     str,
    operation:     str,
    resource_type: str,
    resource_name: str,
    pattern_type:  str,
) -> None:
    cmd = [
        "kafka-acls",
        "--bootstrap-server", bootstrap,
        "--add",
        "--allow-principal", principal,
        "--operation", operation,
        f"--{resource_type}", resource_name,
        "--resource-pattern-type", pattern_type,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"kafka-acls failed for {operation} on {resource_name}: "
            f"{result.stderr.strip()}"
        )