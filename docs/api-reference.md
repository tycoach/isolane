# isolane API Reference

The isolane management API is a FastAPI application running on port 8000.
All routes except `/health/*`, `/auth/login`, `/auth/refresh`, and `/auth/jwks`
require a valid RS256 JWT Bearer token.

Base URL: `http://localhost:8000`

---

## Authentication

isolane uses RS256 JWT tokens. Two token types are issued on login:

| Token | TTL | Purpose |
|---|---|---|
| `access_token` | 15 minutes | Authenticate API requests |
| `refresh_token` | 30 days | Obtain new access tokens without re-login |

The `access_token` carries the following claims:

```json
{
  "sub":  "user-uuid",
  "email": "user@company.com",
  "role": "platform_operator | engineer | viewer",
  "ns":   "analytics",
  "sid":  "session-uuid",
  "iss":  "isolane",
  "exp":  1234567890,
  "iat":  1234567890
}
```

The `ns` (namespace) claim is enforced by the namespace guard middleware.
A token issued for `analytics` cannot access `/finance/*` routes.

---

## Endpoints

### Health

#### `GET /health/live`
Liveness probe. Returns 200 if the API process is running.

```json
{"status": "ok"}
```

#### `GET /health/ready`
Readiness probe. Returns 200 if Postgres and Redis are reachable.

```json
{"status": "ok", "postgres": "ok", "redis": "ok"}
```

---

### Auth

#### `POST /auth/login`

Authenticate and receive a token pair.

**Request:**
```json
{
  "email": "admin@yourcompany.com",
  "password": "yourpassword"
}
```

**Response `200`:**
```json
{
  "access_token":  "eyJ...",
  "refresh_token": "550e8400-e29b-41d4-a716-446655440000",
  "token_type":    "bearer",
  "expires_in":    900
}
```

**Response `401`:**
```json
{"detail": "Invalid credentials"}
```

---

#### `POST /auth/refresh`

Exchange a refresh token for a new access token.
The refresh token is unchanged — same token is returned.

**Request:**
```json
{
  "refresh_token": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Response `200`:**
```json
{
  "access_token":  "eyJ...",
  "refresh_token": "550e8400-e29b-41d4-a716-446655440000",
  "token_type":    "bearer",
  "expires_in":    900
}
```

**Response `401`:**
```json
{"detail": "Invalid refresh token"}
```

---

#### `POST /auth/logout`

Revoke the current session. Requires Bearer token.

**Response `200`:**
```json
{"status": "ok", "message": "Session revoked"}
```

---

#### `GET /auth/jwks`

Public JWKS endpoint. Used by downstream services to verify JWT signatures.
No authentication required.

**Response `200`:**
```json
{
  "keys": [
    {
      "kty": "RSA",
      "use": "sig",
      "alg": "RS256",
      "kid": "isolane-2026",
      "n":   "...",
      "e":   "AQAB"
    }
  ]
}
```

---

### Tenants

All tenant routes require `platform_operator` role.

#### `GET /tenants`

List all provisioned tenants.

**Response `200`:**
```json
[
  {
    "id":         "uuid",
    "namespace":  "analytics",
    "team_name":  "analytics-team",
    "mode":       "standard",
    "created_at": "2026-04-19T00:00:00Z"
  }
]
```

---

#### `POST /tenants`

Register a new tenant. Does not provision — use `make tenant` for provisioning.

**Request:**
```json
{
  "namespace":  "analytics",
  "team_name":  "analytics-team",
  "mode":       "standard"
}
```

**Response `201`:**
```json
{
  "id":        "uuid",
  "namespace": "analytics",
  "team_name": "analytics-team",
  "mode":      "standard"
}
```

**Response `409`:**
```json
{"detail": "Namespace 'analytics' already exists"}
```

---

#### `GET /tenants/{namespace}`

Get a single tenant by namespace.

**Response `200`:**
```json
{
  "id":         "uuid",
  "namespace":  "analytics",
  "team_name":  "analytics-team",
  "mode":       "standard",
  "created_at": "2026-04-19T00:00:00Z"
}
```

---

### Pipelines

Requires `platform_operator` or `engineer` role with matching namespace claim.

#### `GET /{namespace}/pipelines`

List all pipelines for a namespace.

**Response `200`:**
```json
[
  {
    "id":          "uuid",
    "pipeline_id": "customers",
    "namespace":   "analytics",
    "config":      {...},
    "created_at":  "2026-04-19T00:00:00Z"
  }
]
```

---

#### `GET /{namespace}/pipelines/{pipeline_id}/runs`

List recent pipeline runs for a pipeline.

Query params:
- `limit` — number of runs to return (default: 20, max: 100)
- `status` — filter by status: `done`, `failed`, `running`

**Response `200`:**
```json
[
  {
    "id":                  "uuid",
    "pipeline_id":         "customers",
    "status":              "done",
    "records_in":          21,
    "records_out":         20,
    "records_quarantined": 1,
    "error_message":       null,
    "started_at":          "2026-04-19T22:33:00Z",
    "completed_at":        "2026-04-19T22:33:48Z"
  }
]
```

---

### Quarantine

Requires `platform_operator` or `engineer` role.

#### `GET /{namespace}/quarantine`

List quarantined records for a namespace.

Query params:
- `pipeline_id` — filter by pipeline
- `limit` — records to return (default: 50, max: 500)
- `reason` — filter by quarantine reason

**Response `200`:**
```json
[
  {
    "id":          "uuid",
    "pipeline_id": "customers",
    "record":      {"customer_id": null, "email": "..."},
    "reason":      "null_natural_key",
    "batch_offset": "etl-run-001",
    "quarantined_at": "2026-04-19T22:33:00Z"
  }
]
```

---

### Audit

Requires `platform_operator` role.

#### `GET /audit`

List audit log entries.

Query params:
- `namespace` — filter by namespace
- `event_type` — filter by event type
- `limit` — entries to return (default: 50)

**Response `200`:**
```json
[
  {
    "id":         "uuid",
    "event_type": "token_revocation",
    "namespace":  "analytics",
    "user_id":    "uuid",
    "detail":     {"reason": "logout"},
    "created_at": "2026-04-19T00:00:00Z"
  }
]
```

---

## Error responses

All errors follow the same shape:

```json
{"detail": "Human-readable error message"}
```

| Status | Meaning |
|---|---|
| 400 | Bad request — invalid input |
| 401 | Unauthenticated — missing or invalid token |
| 403 | Forbidden — valid token but wrong role or namespace |
| 404 | Not found |
| 409 | Conflict — resource already exists |
| 422 | Validation error — request body failed schema validation |
| 500 | Internal server error |

---

## Role matrix

| Route | `platform_operator` | `engineer` | `viewer` |
|---|---|---|---|
| `GET /health/*` | ✓ | ✓ | ✓ |
| `POST /auth/login` | ✓ | ✓ | ✓ |
| `GET /auth/jwks` | ✓ | ✓ | ✓ |
| `GET /tenants` | ✓ | — | — |
| `POST /tenants` | ✓ | — | — |
| `GET /{ns}/pipelines` | ✓ | ✓ (own ns) | ✓ (own ns) |
| `GET /{ns}/pipelines/*/runs` | ✓ | ✓ (own ns) | ✓ (own ns) |
| `GET /{ns}/quarantine` | ✓ | ✓ (own ns) | — |
| `GET /audit` | ✓ | — | — |

---

## cURL examples

```bash
# Login
TOKEN=$(curl -s -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@isolane.dev","password":"Isolane2026!"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# List tenants
curl -s http://localhost:8000/tenants \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# List pipeline runs
curl -s "http://localhost:8000/analytics/pipelines/customers/runs?limit=5" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# List quarantined records
curl -s "http://localhost:8000/analytics/quarantine?pipeline_id=customers" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```
