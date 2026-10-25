# One image for api, worker and beat.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY pytest.ini ruff.toml ./
COPY src/ ./src/
COPY tests/ ./tests/
COPY scripts/ ./scripts/

# Work from the repo root, so `pytest` finds pytest.ini and tests/.
# PYTHONPATH=/app/src makes `config` and `invoices` importable for gunicorn and celery.
EXPOSE 8000
