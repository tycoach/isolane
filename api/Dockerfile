FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the full project so imports like api.db.connection resolve
COPY api/ ./api/
COPY provisioner/ ./provisioner/
COPY metadata/ ./metadata/
COPY monitoring/ ./monitoring/
COPY scripts/ ./scripts/
COPY engine/ ./engine/

# Create __init__.py files to make packages importable
RUN touch api/__init__.py \
         api/db/__init__.py \
         api/auth/__init__.py \
         api/middleware/__init__.py \
         api/routers/__init__.py \
         api/schemas/__init__.py \
         provisioner/__init__.py \
         provisioner/steps/__init__.py \
         engine/__init__.py

# Non-root user
RUN useradd -m -u 1001 isolane && chown -R isolane:isolane /app
USER isolane

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health/live')"

CMD ["uvicorn", "api.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--log-level", "info"]