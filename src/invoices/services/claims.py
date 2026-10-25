"""Every status change after ingest, in raw SQL (PLANNING.md 3.4, 3.7).

Rules that keep the outcome exactly-once:
  * a worker only owns a row after `claim` set its lease_token;
  * every later write by that worker is guarded by `WHERE lease_token = <mine>`, so a
    worker whose lease expired (and was handed to someone else) changes nothing;
  * each function is one transaction that also updates the counter tables.

Lock order in every transaction: invoice row(s) first, then counter rows (counters.py).
"""
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import structlog
from django.conf import settings
from django.db import connection, transaction

from invoices.models import Status
from invoices.services import counters

log = structlog.get_logger()


@dataclass(frozen=True)
class ClaimedInvoice:
    pk: int
    invoice_id: str
    vendor_id: str
    po_number: str
    amount: Decimal
    currency: str
    trace_id: str
    attempts: int
    rate_limited_count: int
    lease_token: uuid.UUID


@dataclass(frozen=True)
class ResultData:
    matched: bool
    decided_by: str  # "erp" | "cache"
    po_amount: Decimal | None = None
    po_currency: str | None = None
    erp_response: dict | None = None


_CLAIM = """
UPDATE invoice
SET status = 'PROCESSING', lease_token = %(tok)s,
    lease_until = now() + make_interval(secs => %(lease)s), updated_at = now()
WHERE id = %(id)s AND status = 'QUEUED'
RETURNING id, invoice_id, vendor_id, po_number, amount, currency, trace_id,
          attempts, rate_limited_count
"""


def claim(invoice_pk: int) -> ClaimedInvoice | None:
    """Take a QUEUED row. None means another worker has it, or it is already finished."""
    token = uuid.uuid4()
    with transaction.atomic(), connection.cursor() as cur:
        cur.execute(_CLAIM, {"tok": token, "lease": settings.LEASE_SECONDS, "id": invoice_pk})
        row = cur.fetchone()
        if row is None:
            return None
        counters.on_transition(Status.QUEUED, Status.PROCESSING)
    return ClaimedInvoice(*row, lease_token=token)


def _guarded_update(c: ClaimedInvoice, new_status: str, set_sql: str, params: dict) -> bool:
    """UPDATE ... WHERE lease_token = mine; on success also move the counters."""
    sql = (f"UPDATE invoice SET status = %(status)s, lease_token = NULL, lease_until = NULL, "
           f"updated_at = now(), {set_sql} WHERE id = %(id)s AND lease_token = %(tok)s")
    with connection.cursor() as cur:
        cur.execute(sql, {**params, "status": new_status, "id": c.pk, "tok": c.lease_token})
        if cur.rowcount != 1:
            log.warning("invoice.lease_lost", invoice_id=c.invoice_id, wanted=new_status)
            return False
    counters.on_transition(Status.PROCESSING, new_status)
    return True


def finish(c: ClaimedInvoice, status: str, reason: str | None, result: ResultData) -> bool:
    """Final outcome (MATCHED / UNMATCHED) plus its one reconciliation_result row."""
    with transaction.atomic():
        if not _guarded_update(c, status, "reason = %(reason)s, finished_at = now()",
                               {"reason": reason}):
            return False
        with connection.cursor() as cur:
            # UNIQUE(invoice_id) is the last line of defence against a second result.
            cur.execute(
                "INSERT INTO reconciliation_result (invoice_id, matched, reason, po_amount, "
                "po_currency, decided_by, erp_response) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [c.pk, result.matched, reason, result.po_amount, result.po_currency,
                 result.decided_by,
                 json.dumps(result.erp_response) if result.erp_response is not None else None],
            )
    return True


def reschedule(c: ClaimedInvoice, delay_s: float, *, count_attempt: bool,
               count_rate_limited: bool, error: str) -> bool:
    """Back to the dispatcher in `delay_s` seconds. The task then simply ends."""
    with transaction.atomic():
        return _guarded_update(
            c, Status.RETRY_SCHEDULED,
            "next_attempt_at = now() + make_interval(secs => %(delay)s), "
            "attempts = attempts + %(a)s, rate_limited_count = rate_limited_count + %(r)s, "
            "last_error = %(error)s",
            {"delay": delay_s, "a": int(count_attempt), "r": int(count_rate_limited),
             "error": error},
        )


def dead_letter(c: ClaimedInvoice, reason: str, error: str, *, count_attempt: bool = False
                ) -> bool:
    with transaction.atomic():
        return _guarded_update(
            c, Status.DEAD_LETTERED,
            "reason = %(reason)s, last_error = %(error)s, dead_lettered_at = now(), "
            "finished_at = now(), attempts = attempts + %(a)s",
            {"reason": reason, "error": error, "a": int(count_attempt)},
        )


def record_attempt(invoice_pk: int, attempt_no: int, started_at: datetime, duration_ms: int,
                   http_status: int | None, error: str | None, error_class: str,
                   trace_id: str) -> None:
    """Audit row per ERP call. Not lease-guarded: it records what happened, not a decision."""
    with connection.cursor() as cur:
        cur.execute(
            "INSERT INTO invoice_attempt (invoice_id, attempt_no, started_at, duration_ms, "
            "http_status, error, error_class, trace_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            [invoice_pk, attempt_no, started_at, duration_ms, http_status, error,
             error_class, trace_id],
        )


# --- Dispatcher side (PLANNING.md 3.7) ---

_POISON = """
UPDATE invoice
SET status = 'DEAD_LETTERED', reason = 'poison', crash_count = crash_count + 1,
    dead_lettered_at = now(), finished_at = now(), lease_token = NULL, lease_until = NULL,
    last_error = 'lease expired ' || (crash_count + 1) || ' times', updated_at = now()
WHERE status = 'PROCESSING' AND lease_until < now() AND crash_count + 1 >= %(max_crashes)s
RETURNING id
"""

_PICK_DUE = """
WITH due AS (
  SELECT id, status FROM invoice
  WHERE status = 'PENDING'
     OR (status = 'RETRY_SCHEDULED' AND next_attempt_at <= now())
     OR (status IN ('QUEUED', 'PROCESSING') AND lease_until < now())
  ORDER BY updated_at
  LIMIT %(n)s
  FOR UPDATE SKIP LOCKED
)
UPDATE invoice i
SET status = 'QUEUED', lease_token = NULL,
    lease_until = now() + make_interval(secs => %(queue_lease)s),
    crash_count = i.crash_count + CASE WHEN due.status = 'PROCESSING' THEN 1 ELSE 0 END,
    updated_at = now()
FROM due WHERE i.id = due.id
RETURNING i.id, i.trace_id, due.status
"""


def dead_letter_poison() -> int:
    """Rows whose lease expired MAX_CRASHES times: they keep killing workers."""
    with transaction.atomic(), connection.cursor() as cur:
        cur.execute(_POISON, {"max_crashes": settings.MAX_CRASHES})
        n = len(cur.fetchall())
        if n:
            counters.record_transitions([(Status.PROCESSING, Status.DEAD_LETTERED)] * n)
    return n


def requeue_dead_letters(invoice_ids: list[str] | None = None, reason: str | None = None,
                         since_seconds: int | None = None, dry_run: bool = False) -> int:
    """Operator action: DEAD_LETTERED -> PENDING with fresh counters. InvoiceAttempt
    history is kept. At least one filter is required (no accidental 'requeue everything')."""
    if not (invoice_ids or reason or since_seconds):
        raise ValueError("give at least one of invoice_ids, reason, since_seconds")
    where, params = ["status = 'DEAD_LETTERED'"], []
    if invoice_ids:
        where.append("invoice_id = ANY(%s)")
        params.append(list(invoice_ids))
    if reason:
        where.append("reason = %s")
        params.append(reason)
    if since_seconds:
        where.append("dead_lettered_at >= now() - make_interval(secs => %s)")
        params.append(since_seconds)
    cond = " AND ".join(where)

    with transaction.atomic(), connection.cursor() as cur:
        if dry_run:
            cur.execute(f"SELECT count(*) FROM invoice WHERE {cond}", params)
            return cur.fetchone()[0]
        cur.execute(
            f"UPDATE invoice SET status = 'PENDING', attempts = 0, crash_count = 0, "
            f"rate_limited_count = 0, reason = NULL, next_attempt_at = NULL, "
            f"dead_lettered_at = NULL, finished_at = NULL, lease_token = NULL, "
            f"lease_until = NULL, updated_at = now() WHERE {cond} RETURNING id",
            params,
        )
        n = len(cur.fetchall())
        if n:
            counters.record_transitions([(Status.DEAD_LETTERED, Status.PENDING)] * n)
    return n


def pick_due(n: int) -> list[tuple[int, str]]:
    """Mark up to n due rows QUEUED; returns (pk, trace_id). Enqueue only after commit."""
    with transaction.atomic(), connection.cursor() as cur:
        cur.execute(_PICK_DUE, {"n": n, "queue_lease": settings.QUEUE_LEASE_SECONDS})
        rows = cur.fetchall()
        counters.record_transitions([(old, Status.QUEUED) for _, _, old in rows])
    return [(pk, trace_id) for pk, trace_id, _ in rows]
