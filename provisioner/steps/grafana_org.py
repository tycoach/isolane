"""
isolane/provisioner/steps/grafana_org.py

Create a Grafana organisation for this namespace.
"""

import os
import json
import base64
import urllib.request
import urllib.error
from typing import Any


def run(ns: str, cfg: dict[str, Any]) -> None:
    grafana_url  = os.environ.get("GRAFANA_URL", "http://localhost:3000")
    grafana_user = os.environ.get("GRAFANA_USER", "admin")
    grafana_pass = os.environ.get("GRAFANA_PASSWORD", "admin")
    auth         = base64.b64encode(
        f"{grafana_user}:{grafana_pass}".encode()
    ).decode()
    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Basic {auth}",
    }

    org_name = f"{ns}-org"

    # Create org
    org_id = _create_org(grafana_url, headers, org_name)
    print(f"  [grafana_org] Org '{org_name}' ready (id={org_id})")

    # Add Prometheus datasource scoped to this namespace
    _add_datasource(grafana_url, headers, org_id, ns)
    print(f"  [grafana_org] Prometheus datasource added for '{ns}'")


def _create_org(url: str, headers: dict, name: str) -> int:
    """Create org, return its ID. Idempotent — returns existing ID if present."""
    try:
        req  = urllib.request.Request(
            f"{url}/api/orgs",
            data=json.dumps({"name": name}).encode(),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())["orgId"]
    except urllib.error.HTTPError as e:
        if e.code == 409:
            # Already exists — fetch the ID
            req = urllib.request.Request(
                f"{url}/api/orgs/name/{urllib.request.quote(name)}",
                headers=headers,
            )
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())["id"]
        raise


def _add_datasource(url: str, headers: dict, org_id: int, ns: str) -> None:
    """Add a Prometheus datasource to the org, scoped by namespace label."""
    prom_url = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
    ds = {
        "orgId":  org_id,
        "name":   "Prometheus",
        "type":   "prometheus",
        "url":    prom_url,
        "access": "proxy",
        "isDefault": True,
        "jsonData": {
            "httpMethod":       "POST",
            "customQueryParameters": f"tenant={ns}",
        },
    }
    # Use org-scoped API endpoint
    req = urllib.request.Request(
        f"{url}/api/datasources",
        data=json.dumps(ds).encode(),
        headers={**headers, "X-Grafana-Org-Id": str(org_id)},
        method="POST",
    )
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        if e.code == 409:
            pass  # datasource already exists
        else:
            raise