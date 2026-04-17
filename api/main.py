"""
isolane/api/main.py

FastAPI application entry point.

Startup sequence:
  1. Load environment config
  2. Create PostgreSQL connection pool
  3. Register middleware (CORS, request logging)
  4. Mount all routers
  5. Expose /openapi.json and /docs (dev only)

Shutdown sequence:
  1. Drain in-flight requests (FastAPI handles this)
  2. Close PostgreSQL pool gracefully

All routes follow one of three access patterns:
  - Public:            /health/*, /auth/jwks, /auth/login, /auth/refresh
  - Platform operator: /tenants/*, /users, /audit
  - Namespace-scoped:  /{namespace}/*  — JWT namespace claim enforced
"""

import os
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.db.connection import create_pool, close_pool
from api.routers import health, audit, tenants, pipelines, quarantine

# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("isolane.api")


# ── Lifespan — startup and shutdown ──────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Runs startup logic before the app begins serving,
    and shutdown logic after the last request completes.
    """
    # ── Startup ───────────────────────────────────────────────
    logger.info("isolane API starting up")

    pool = await create_pool()
    logger.info(
        "PostgreSQL pool ready",
        extra={"min_size": pool.get_min_size(), "max_size": pool.get_max_size()},
    )

    yield  # App serves requests between here and shutdown

    # ── Shutdown ──────────────────────────────────────────────
    logger.info("isolane API shutting down — closing DB pool")
    await close_pool()
    logger.info("Shutdown complete")


# ── App instantiation ─────────────────────────────────────────────
app = FastAPI(
    title="isolane",
    description=(
        "Multi-tenant data pipeline isolation framework. "
        "Shared infrastructure, isolated blast radius."
    ),
    version=os.environ.get("APP_VERSION", "dev"),
    docs_url="/docs" if os.environ.get("ENV") != "production" else None,
    redoc_url="/redoc" if os.environ.get("ENV") != "production" else None,
    openapi_url="/openapi.json" if os.environ.get("ENV") != "production" else None,
    lifespan=lifespan,
)


# ── CORS ──────────────────────────────────────────────────────────
_allowed_origins = os.environ.get(
    "CORS_ALLOWED_ORIGINS",
    "http://localhost:3000,http://localhost:8501",
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _allowed_origins],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
    expose_headers=["X-Request-ID", "X-Token-Expired"],
)


# ── Request logging middleware ────────────────────────────────────
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """
    Log every request with method, path, status, and duration.
    Skips /health/live to avoid log noise from probes.
    """
    if request.url.path == "/health/live":
        return await call_next(request)

    start    = time.monotonic()
    response = await call_next(request)
    duration = round((time.monotonic() - start) * 1000, 2)

    logger.info(
        "%s %s %s %.2fms",
        request.method,
        request.url.path,
        response.status_code,
        duration,
    )
    return response


# ── Request ID propagation ────────────────────────────────────────
@app.middleware("http")
async def propagate_request_id(request: Request, call_next):
    """
    Echo back the X-Request-ID header if the client sent one.
    Useful for tracing a request through logs.
    """
    request_id = request.headers.get("X-Request-ID", "")
    response   = await call_next(request)
    if request_id:
        response.headers["X-Request-ID"] = request_id
    return response


# ── Global exception handlers ─────────────────────────────────────
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """
    Catch any unhandled exception and return a generic 500.
    Logs the full traceback for debugging without leaking
    internal details to the caller.
    """
    logger.exception(
        "Unhandled exception on %s %s",
        request.method,
        request.url.path,
    )
    return JSONResponse(
        status_code=500,
        content={
            "detail": "An internal error occurred. Check server logs.",
        },
    )


# ── Router registration ───────────────────────────────────────────
#
# Order matters for FastAPI route matching:
#   1. /health/* — no auth, always first so probes never hit auth middleware
#   2. /auth/*   — login, refresh, logout, JWKS — no namespace
#   3. /tenants, /users, /audit — platform operator
#   4. /{namespace}/* — tenant-scoped, namespace guard applied per route
#
app.include_router(health.router)
app.include_router(tenants.router)    # includes /auth/* and /users routes
app.include_router(audit.router)
app.include_router(pipelines.router)
app.include_router(quarantine.router)


# ── Root ──────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
async def root():
    return {
        "service": "isolane",
        "version": os.environ.get("APP_VERSION", "dev"),
        "docs":    "/docs",
        "health":  "/health/ready",
    }


# ── Entrypoint for local dev ──────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000)),
        reload=os.environ.get("ENV") == "development",
        log_level="info",
    )