"""Logs and metrics (PLANNING.md 5.3, 5.4).

* Logs: structlog, one JSON object per line on stdout. `trace_id` and `invoice_id` are bound
  with contextvars, so every line about an invoice carries them.
* Metrics: counters/histograms below are recorded where things happen. The backlog gauges
  are computed at scrape time (PipelineCollector) from the counter tables, a partial index
  and Redis, so they are always current and cost almost nothing.
"""
import logging
import sys

import structlog
from prometheus_client import Counter, Histogram
from prometheus_client.core import GaugeMetricFamily

ERP_REQUEST_SECONDS = Histogram(
    "erp_request_seconds", "ERP call duration, including failures",
    buckets=(0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 2.5, 5.0),
)
ERP_RESPONSES = Counter("erp_responses_total", "ERP responses by code", ["code"])
ERP_BREAKER_OPEN = Counter("erp_breaker_open_total", "Times breaker-lite paused all workers")
LIMITER_WAIT = Histogram(
    "limiter_wait_seconds", "Time a worker slept waiting for an ERP slot",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0),
)
RETRIES_SCHEDULED = Counter("retries_scheduled_total", "Retries scheduled", ["reason"])
DEAD_LETTERS = Counter("dead_letters_total", "Invoices dead-lettered", ["reason"])
OUTCOMES = Counter("invoices_outcome_total", "Final outcomes", ["status", "reason"])
CACHE_LOOKUPS = Counter(
    "cache_lookups_total", "PO cache lookups: hit_decided | hit_undecided | miss", ["result"]
)
INGESTED = Counter(
    "invoices_ingested_total", "Ingest results: accepted | duplicate | invalid | already_seen",
    ["result"],
)


class PipelineCollector:
    """Gauges read at scrape time: queue depth, backlog by status, lag."""

    def collect(self):
        from invoices.models import UNFINISHED_STATUSES
        from invoices.services import reporting

        counts = reporting.status_counts()
        lag = reporting.lag(counts)

        depth = GaugeMetricFamily("queue_depth", "Invoice ids waiting in the Redis queue")
        depth.add_metric([], reporting.queue_depth())
        yield depth

        backlog = GaugeMetricFamily("backlog", "Unfinished invoices by status", labels=["status"])
        for status in UNFINISHED_STATUSES:
            backlog.add_metric([status], counts[status])
        yield backlog

        for name, help_text in (
            ("oldest_unfinished_age_seconds", "Age of the oldest unfinished invoice"),
            ("estimated_drain_seconds", "Backlog / completions per second (-1 = unknown)"),
        ):
            gauge = GaugeMetricFamily(name, help_text)
            gauge.add_metric([], lag[name])
            yield gauge


def configure_logging() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.WriteLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per ERP call is enough
