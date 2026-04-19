FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY worker/python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

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

CMD ["python3", "-m", "worker.python.consumer"]