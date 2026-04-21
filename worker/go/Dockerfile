# ── Build stage ───────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build

COPY worker/go/go.mod worker/go/go.sum* ./
RUN go mod download

COPY worker/go/ .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s" \
    -o /worker \
    ./cmd/worker

# ── Runtime stage ─────────────────────────────────────────────────
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python engine dependencies
COPY worker/python/requirements.txt .
RUN pip install --no-cache-dir psycopg2-binary==2.9.9 && \
    pip install --no-cache-dir -r requirements.txt

# Copy Go binary
COPY --from=builder /worker /usr/local/bin/worker

# Copy Python engine
COPY api/ ./api/
COPY engine/ ./engine/
COPY worker/python/ ./worker/python/
COPY provisioner/ ./provisioner/
COPY config/ ./config/
COPY dbt/ ./dbt/

RUN touch api/__init__.py api/db/__init__.py api/auth/__init__.py \
         engine/__init__.py \
         worker/__init__.py worker/python/__init__.py \
         provisioner/__init__.py provisioner/steps/__init__.py

RUN useradd -m -u 1001 isolane && chown -R isolane:isolane /app
USER isolane

EXPOSE 9090

CMD ["/usr/local/bin/worker"]