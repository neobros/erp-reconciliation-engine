"""The worker flow for one invoice (PLANNING.md 3.6). tasks.reconcile only calls process()."""
import random
import time
from decimal import Decimal

import structlog
from django.conf import settings
from django.utils import timezone

from invoices.models import Status
from invoices.observability import (
    CACHE_LOOKUPS,
    DEAD_LETTERS,
    ERP_RESPONSES,
    OUTCOMES,
    RETRIES_SCHEDULED,
)
from invoices.services import claims, matching, po_cache, rate_limiter, reporting
from invoices.services.claims import ClaimedInvoice, ResultData
from invoices.services.classify import Kind, Outcome, backoff_seconds, classify
from invoices.services.erp_client import ErpClient

log = structlog.get_logger()
_erp = ErpClient()

_ERROR_CLASS = {Kind.MATCHED: "ok", Kind.UNMATCHED: "ok", Kind.RATE_LIMITED: "rate_limited",
                Kind.TRANSIENT: "transient", Kind.PERMANENT: "permanent"}


def process(invoice_pk: int) -> None:
    c = claims.claim(invoice_pk)
    if c is None:
        return  # someone else has it, or it is already finished: a duplicate delivery
    structlog.contextvars.bind_contextvars(trace_id=c.trace_id, invoice_id=c.invoice_id)
    lock_token = None
    try:
        log.info("invoice.claimed", attempts=c.attempts)

        # 1. The cache can say no (never yes).
        facts = po_cache.get(c.po_number)
        decision = matching.decide_from_cache(c, facts)
        CACHE_LOOKUPS.labels(
            result="miss" if facts is None else "hit_decided" if decision else "hit_undecided"
        ).inc()
        if decision:
            _finish_from_cache(c, *decision, facts)
            return

        # 2. Single-flight per unknown PO: the others come back in 2 s and read the cache.
        lock_token = po_cache.try_lock(c.po_number)
        if lock_token is None and facts is None:
            _reschedule(c, 2, "po_lock")
            return

        # 3. Global rate limit, then the ERP.
        try:
            rate_limiter.acquire_erp_slot()
        except rate_limiter.RateLimitBusy:
            _reschedule(c, 2 + random.uniform(0, 3), "limiter_busy")
            return
        outcome = _call_erp(c)
        rate_limiter.record_health(outcome)
        po_cache.learn(c.po_number, c.vendor_id, outcome)
        _apply(c, outcome)
    except Exception:
        # A bug on our side. Counted as a transient attempt so it is retried, then
        # dead-lettered. If this write fails too, the lease expires and the dispatcher
        # re-queues the row (crash_count + 1).
        log.exception("invoice.internal_error")
        _transient(c, "internal_error")
    finally:
        if lock_token:
            po_cache.release_lock(c.po_number, lock_token)
        structlog.contextvars.unbind_contextvars("trace_id", "invoice_id")


def _call_erp(c: ClaimedInvoice) -> Outcome:
    started_at, t0 = timezone.now(), time.monotonic()
    response, exc = None, None
    try:
        response = _erp.match(c.po_number, c.vendor_id, c.amount, c.currency, c.trace_id)
    except Exception as e:  # classify() decides what it means
        exc = e
    outcome = classify(response, exc)
    duration_ms = int((time.monotonic() - t0) * 1000)

    code = str(response.status_code) if response is not None else outcome.reason
    ERP_RESPONSES.labels(code=code).inc()
    log.info("erp.call", status=code, duration_ms=duration_ms, kind=outcome.kind.value)
    claims.record_attempt(
        c.pk, c.attempts + c.rate_limited_count + 1, started_at, duration_ms,
        response.status_code if response is not None else None,
        type(exc).__name__ if exc else None, _ERROR_CLASS[outcome.kind], c.trace_id,
    )
    return outcome


def _apply(c: ClaimedInvoice, outcome: Outcome) -> None:
    if outcome.kind is Kind.MATCHED:
        status, reason = matching.check_currency(c, outcome.body)  # the ERP ignores currency
        _finish(c, status, reason, outcome.body)
    elif outcome.kind is Kind.UNMATCHED:
        _finish(c, Status.UNMATCHED, outcome.reason, outcome.body)
    elif outcome.kind is Kind.RATE_LIMITED:
        # The ERP never looked at this invoice, so it is not this invoice's failure.
        rate_limiter.start_cooldown(outcome.retry_after)
        if c.rate_limited_count + 1 >= settings.MAX_RATE_LIMITED:
            _transient(c, outcome.reason)
        else:
            _reschedule(c, outcome.retry_after, outcome.reason, count_rate_limited=True)
    elif outcome.kind is Kind.TRANSIENT:
        _transient(c, outcome.reason)
    else:
        _dead_letter(c, "permanent_error", outcome.reason)


def _decimal(value) -> Decimal | None:
    return Decimal(str(value)) if value not in (None, "") else None


def _finish(c: ClaimedInvoice, status: str, reason: str | None, body: dict) -> None:
    result = ResultData(matched=status == Status.MATCHED, decided_by="erp",
                        po_amount=_decimal(body.get("po_amount")),
                        po_currency=body.get("currency"), erp_response=body)
    if claims.finish(c, status, reason, result):
        _done(status, reason, decided_by="erp")


def _finish_from_cache(c: ClaimedInvoice, status: str, reason: str, facts: dict) -> None:
    result = ResultData(matched=False, decided_by="cache",
                        po_amount=_decimal(facts.get("po_amount")),
                        po_currency=facts.get("currency"))
    if claims.finish(c, status, reason, result):
        _done(status, reason, decided_by="cache")


def _done(status: str, reason: str | None, **fields) -> None:
    OUTCOMES.labels(status=status, reason=reason or "").inc()
    reporting.note_completion()
    log.info("invoice.outcome", status=status, reason=reason, **fields)


def _transient(c: ClaimedInvoice, reason: str) -> None:
    if c.attempts + 1 >= settings.MAX_ATTEMPTS:
        _dead_letter(c, "max_attempts", reason, count_attempt=True)
    else:
        _reschedule(c, backoff_seconds(c.attempts + 1), reason, count_attempt=True)


def _reschedule(c: ClaimedInvoice, delay: float, reason: str, *, count_attempt: bool = False,
                count_rate_limited: bool = False) -> None:
    if claims.reschedule(c, delay, count_attempt=count_attempt,
                         count_rate_limited=count_rate_limited, error=reason):
        RETRIES_SCHEDULED.labels(reason=reason).inc()
        log.info("invoice.retry_scheduled", delay=round(delay, 2), reason=reason)


def _dead_letter(c: ClaimedInvoice, reason: str, error: str, *, count_attempt: bool = False
                 ) -> None:
    if claims.dead_letter(c, reason, error, count_attempt=count_attempt):
        DEAD_LETTERS.labels(reason=reason).inc()
        _done(Status.DEAD_LETTERED, reason, last_error=error)
