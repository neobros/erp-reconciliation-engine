"""HTTP views. Thin: no decisions here (PLANNING.md section 2)."""
import os
import uuid

from django.db import connection
from django.http import HttpResponse, JsonResponse
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, CollectorRegistry, generate_latest
from rest_framework.decorators import api_view
from rest_framework.response import Response

from invoices.observability import PipelineCollector
from invoices.services import ingest as ingest_service
from invoices.services import reporting
from invoices.services.redis_conn import get_redis

MAX_BATCH = 500
MAX_WINDOW_HOURS = 168


def healthz(request):
    """Liveness: the process is up. Checks no dependencies on purpose."""
    return JsonResponse({"status": "ok"})


def readyz(request):
    """Readiness: Postgres and Redis both answer. 503 names whichever failed."""
    checks = {}
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT 1")
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {exc.__class__.__name__}"
    try:
        get_redis().ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc.__class__.__name__}"

    ready = all(v == "ok" for v in checks.values())
    return JsonResponse(
        {"status": "ready" if ready else "not_ready", "checks": checks},
        status=200 if ready else 503,
    )


@api_view(["POST"])
def ingest(request):
    """POST /invoices: a JSON array of 1-500 invoices. Never calls the ERP or Celery."""
    trace_id = (request.headers.get("X-Request-ID") or str(uuid.uuid4()))[:64]

    if ingest_service.backlog_too_big():
        return Response({"error": "backlog_full", "trace_id": trace_id},
                        status=429, headers={"Retry-After": "30"})

    records = request.data
    if not isinstance(records, list) or not 1 <= len(records) <= MAX_BATCH:
        return Response({"error": f"body must be a JSON array of 1-{MAX_BATCH} invoices",
                         "trace_id": trace_id}, status=400)

    return Response(ingest_service.ingest_batch(records, trace_id), status=202)


@api_view(["GET"])
def report_summary(request):
    """GET /reports/summary?window_hours=1: counts, match rate, stuck rows, lag, DLQ."""
    try:
        hours = int(request.query_params.get("window_hours", 1))
    except ValueError:
        hours = 0
    if not 1 <= hours <= MAX_WINDOW_HOURS:
        return Response({"error": f"window_hours must be 1-{MAX_WINDOW_HOURS}"}, status=400)
    return Response(reporting.summary(hours))


_pipeline_collector = PipelineCollector()


def metrics(request):
    """Prometheus text format. gunicorn runs 4 processes, so counters are summed across
    them with prometheus_client's multiprocess mode; backlog gauges are read at scrape time."""
    if os.getenv("PROMETHEUS_MULTIPROC_DIR"):
        from prometheus_client import multiprocess

        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
    else:  # local runserver: one process
        registry = CollectorRegistry()
        registry.register(_RegistryProxy())
    registry.register(_pipeline_collector)
    return HttpResponse(generate_latest(registry), content_type=CONTENT_TYPE_LATEST)


class _RegistryProxy:
    """Exposes the default (single-process) registry inside a fresh registry."""

    def collect(self):
        yield from REGISTRY.collect()
