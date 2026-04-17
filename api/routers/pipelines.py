"""
isolane/api/routers/pipelines.py

Pipeline management endpoints. Namespace-scoped.

POST   /{namespace}/pipelines                        — create pipeline
GET    /{namespace}/pipelines                        — list pipelines
GET    /{namespace}/pipelines/{pipeline_id}          — get one
PATCH  /{namespace}/pipelines/{pipeline_id}          — update config
DELETE /{namespace}/pipelines/{pipeline_id}          — delete
GET    /{namespace}/pipelines/{pipeline_id}/runs     — run history
GET    /{namespace}/pipelines/{pipeline_id}/runs/{id} — single run
"""

import yaml
from datetime import datetime
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status

from api.middleware.namespace_guard import (
    require_namespace_match,
    require_namespace_engineer,
)
from api.auth.jwt import TokenPayload
from api.db.connection import get_pool, set_tenant_context
from api.schemas.pipeline import (
    PipelineCreate,
    PipelineUpdate,
    PipelineResponse,
    PipelineListResponse,
    PipelineRunResponse,
    PipelineRunListResponse,
)

router = APIRouter(tags=["pipelines"])


# ── Helpers ───────────────────────────────────────────────────────

def _pipeline_to_response(row: dict, config: dict) -> PipelineResponse:
    """
    Merge DB row with parsed YAML config into a PipelineResponse.
    """
    from api.schemas.pipeline import DBTConfig, FieldConfig

    dbt_raw    = config.get("dbt")
    fields_raw = config.get("fields", {})

    return PipelineResponse(
        pipeline_id         = row["pipeline_id"],
        kafka_topic         = row["kafka_topic"],
        natural_key         = row["natural_key"],
        scd_type            = row["scd_type"],
        edge_case_mode      = config.get("edge_case_mode", "quarantine"),
        null_threshold      = config.get("null_threshold", 0.05),
        late_arrival_window = config.get("late_arrival_window", "24h"),
        duplicate_window    = config.get("duplicate_window", "30m"),
        xautoclaim_ms       = row["xautoclaim_ms"],
        dbt                 = DBTConfig(**dbt_raw) if dbt_raw else None,
        fields              = {
            k: FieldConfig(**v) for k, v in fields_raw.items()
        },
        created_at          = row["created_at"],
        updated_at          = row["updated_at"],
    )


def _validate_topic_namespace(kafka_topic: str, namespace: str) -> None:
    """
    Enforce that the kafka_topic prefix matches the URL namespace.
    """
    prefix = kafka_topic.split(".")[0]
    if prefix != namespace:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"kafka_topic '{kafka_topic}' prefix '{prefix}' does not match "
                f"namespace '{namespace}'. Topics must follow {{namespace}}.{{dataset}}."
            ),
        )


# ── Routes ────────────────────────────────────────────────────────

@router.post(
    "/{namespace}/pipelines",
    response_model=PipelineResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_pipeline(
    namespace: str,
    body:      PipelineCreate,
    payload:   TokenPayload = Depends(require_namespace_engineer),
):
    """
    Register a new pipeline for this namespace.
    Engineer role or above required.
    Validates kafka_topic prefix matches namespace.
    """
    _validate_topic_namespace(body.kafka_topic, namespace)

    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await set_tenant_context(conn, namespace)

            # Check pipeline_id uniqueness within namespace
            tenant_id = await conn.fetchval(
                "SELECT id FROM tenants WHERE namespace = $1", namespace
            )
            if tenant_id is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Namespace '{namespace}' not found",
                )

            existing = await conn.fetchval(
                "SELECT id FROM pipelines WHERE tenant_id=$1 AND pipeline_id=$2",
                tenant_id, body.pipeline_id,
            )
            if existing:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"Pipeline '{body.pipeline_id}' already exists "
                        f"in namespace '{namespace}'"
                    ),
                )

            # Serialise operational config to YAML for storage
            config = {
                "edge_case_mode":      body.edge_case_mode,
                "null_threshold":      body.null_threshold,
                "late_arrival_window": body.late_arrival_window,
                "duplicate_window":    body.duplicate_window,
                "dbt":    body.dbt.model_dump() if body.dbt else None,
                "fields": {
                    k: v.model_dump() for k, v in body.fields.items()
                },
            }
            config_yaml = yaml.dump(config, default_flow_style=False)

            row = await conn.fetchrow(
                """
                INSERT INTO pipelines
                    (tenant_id, pipeline_id, kafka_topic, natural_key,
                     scd_type, config_yaml, xautoclaim_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING pipeline_id, kafka_topic, natural_key, scd_type,
                          xautoclaim_ms, config_yaml, created_at, updated_at
                """,
                tenant_id,
                body.pipeline_id,
                body.kafka_topic,
                body.natural_key,
                body.scd_type,
                config_yaml,
                body.xautoclaim_ms,
            )

    return _pipeline_to_response(dict(row), config)


@router.get("/{namespace}/pipelines", response_model=PipelineListResponse)
async def list_pipelines(
    namespace: str,
    payload:   TokenPayload = Depends(require_namespace_match),
):
    """List all pipelines in the namespace. All roles."""
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await set_tenant_context(conn, namespace)
            tenant_id = await conn.fetchval(
                "SELECT id FROM tenants WHERE namespace = $1", namespace
            )
            if tenant_id is None:
                raise HTTPException(status_code=404,
                                    detail=f"Namespace '{namespace}' not found")

            rows = await conn.fetch(
                """
                SELECT pipeline_id, kafka_topic, natural_key, scd_type,
                       xautoclaim_ms, config_yaml, created_at, updated_at
                FROM pipelines
                WHERE tenant_id = $1
                ORDER BY created_at DESC
                """,
                tenant_id,
            )

    pipelines = [
        _pipeline_to_response(dict(r), yaml.safe_load(r["config_yaml"]))
        for r in rows
    ]
    return PipelineListResponse(pipelines=pipelines, total=len(pipelines))


@router.get(
    "/{namespace}/pipelines/{pipeline_id}",
    response_model=PipelineResponse,
)
async def get_pipeline(
    namespace:   str,
    pipeline_id: str,
    payload:     TokenPayload = Depends(require_namespace_match),
):
    """Get a single pipeline. All roles."""
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await set_tenant_context(conn, namespace)
            tenant_id = await conn.fetchval(
                "SELECT id FROM tenants WHERE namespace = $1", namespace
            )
            row = await conn.fetchrow(
                """
                SELECT pipeline_id, kafka_topic, natural_key, scd_type,
                       xautoclaim_ms, config_yaml, created_at, updated_at
                FROM pipelines
                WHERE tenant_id = $1 AND pipeline_id = $2
                """,
                tenant_id, pipeline_id,
            )

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline '{pipeline_id}' not found in namespace '{namespace}'",
        )
    return _pipeline_to_response(dict(row), yaml.safe_load(row["config_yaml"]))


@router.patch(
    "/{namespace}/pipelines/{pipeline_id}",
    response_model=PipelineResponse,
)
async def update_pipeline(
    namespace:   str,
    pipeline_id: str,
    body:        PipelineUpdate,
    payload:     TokenPayload = Depends(require_namespace_engineer),
):
    """Update mutable pipeline config. Engineer role or above."""
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await set_tenant_context(conn, namespace)
            tenant_id = await conn.fetchval(
                "SELECT id FROM tenants WHERE namespace = $1", namespace
            )
            row = await conn.fetchrow(
                """
                SELECT pipeline_id, kafka_topic, natural_key, scd_type,
                       xautoclaim_ms, config_yaml, created_at, updated_at
                FROM pipelines
                WHERE tenant_id = $1 AND pipeline_id = $2
                """,
                tenant_id, pipeline_id,
            )
            if row is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Pipeline '{pipeline_id}' not found",
                )

            # Merge updates into existing config
            config = yaml.safe_load(row["config_yaml"])
            if body.edge_case_mode is not None:
                config["edge_case_mode"] = body.edge_case_mode
            if body.null_threshold is not None:
                config["null_threshold"] = body.null_threshold
            if body.late_arrival_window is not None:
                config["late_arrival_window"] = body.late_arrival_window
            if body.duplicate_window is not None:
                config["duplicate_window"] = body.duplicate_window
            if body.dbt is not None:
                config["dbt"] = body.dbt.model_dump()
            if body.fields is not None:
                config["fields"] = {
                    k: v.model_dump() for k, v in body.fields.items()
                }

            new_xautoclaim = body.xautoclaim_ms or row["xautoclaim_ms"]
            new_config_yaml = yaml.dump(config, default_flow_style=False)

            updated = await conn.fetchrow(
                """
                UPDATE pipelines
                SET config_yaml   = $1,
                    xautoclaim_ms = $2
                WHERE tenant_id = $3 AND pipeline_id = $4
                RETURNING pipeline_id, kafka_topic, natural_key, scd_type,
                          xautoclaim_ms, config_yaml, created_at, updated_at
                """,
                new_config_yaml, new_xautoclaim, tenant_id, pipeline_id,
            )

    return _pipeline_to_response(dict(updated), config)


@router.delete(
    "/{namespace}/pipelines/{pipeline_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_pipeline(
    namespace:   str,
    pipeline_id: str,
    payload:     TokenPayload = Depends(require_namespace_engineer),
):
    """
    Delete a pipeline config. Engineer role or above.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await set_tenant_context(conn, namespace)
            tenant_id = await conn.fetchval(
                "SELECT id FROM tenants WHERE namespace = $1", namespace
            )
            result = await conn.execute(
                "DELETE FROM pipelines WHERE tenant_id=$1 AND pipeline_id=$2",
                tenant_id, pipeline_id,
            )
    if result == "DELETE 0":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline '{pipeline_id}' not found in namespace '{namespace}'",
        )


@router.get(
    "/{namespace}/pipelines/{pipeline_id}/runs",
    response_model=PipelineRunListResponse,
)
async def list_pipeline_runs(
    namespace:   str,
    pipeline_id: str,
    status_filter: Optional[str] = Query(
        default=None,
        alias="status",
        description="Filter by status: running | done | failed | quarantined",
    ),
    limit:  int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    payload: TokenPayload = Depends(require_namespace_match),
):
    """Run history for a pipeline. All roles. Ordered newest first."""
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await set_tenant_context(conn, namespace)
            tenant_id = await conn.fetchval(
                "SELECT id FROM tenants WHERE namespace = $1", namespace
            )
            params     = [tenant_id, pipeline_id]
            conditions = ["tenant_id = $1", "pipeline_id = $2"]
            idx        = 3

            if status_filter:
                conditions.append(f"status = ${idx}")
                params.append(status_filter)
                idx += 1

            where = "WHERE " + " AND ".join(conditions)

            rows = await conn.fetch(
                f"""
                SELECT id, pipeline_id, batch_offset, stream_message_id,
                       status, records_in, records_out, records_quarantined,
                       started_at, completed_at, error_message
                FROM pipeline_runs
                {where}
                ORDER BY started_at DESC
                LIMIT ${idx} OFFSET ${idx + 1}
                """,
                *params, limit, offset,
            )
            total = await conn.fetchval(
                f"SELECT COUNT(*) FROM pipeline_runs {where}",
                *params,
            )

    runs = [PipelineRunResponse(**dict(r)) for r in rows]
    return PipelineRunListResponse(runs=runs, total=total)


@router.get(
    "/{namespace}/pipelines/{pipeline_id}/runs/{run_id}",
    response_model=PipelineRunResponse,
)
async def get_pipeline_run(
    namespace:   str,
    pipeline_id: str,
    run_id:      UUID,
    payload:     TokenPayload = Depends(require_namespace_match),
):
    """Get a single pipeline run by ID."""
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await set_tenant_context(conn, namespace)
            tenant_id = await conn.fetchval(
                "SELECT id FROM tenants WHERE namespace = $1", namespace
            )
            row = await conn.fetchrow(
                """
                SELECT id, pipeline_id, batch_offset, stream_message_id,
                       status, records_in, records_out, records_quarantined,
                       started_at, completed_at, error_message
                FROM pipeline_runs
                WHERE tenant_id = $1 AND pipeline_id = $2 AND id = $3
                """,
                tenant_id, pipeline_id, run_id,
            )

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{run_id}' not found",
        )
    return PipelineRunResponse(**dict(row))