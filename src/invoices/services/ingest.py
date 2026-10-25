"""Ingest: validate, fingerprint, insert (PLANNING.md Step 2).

Each invoice gets its own short transaction. A batch-wide transaction would hold row and
counter locks for the whole batch, so two overlapping batches could deadlock. Failure
semantics stay simple: if a batch dies halfway, the upstream re-sends it and the rows
already written come back as `already_seen`.
"""
import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

import structlog
from django.conf import settings
from django.db import IntegrityError, connection, transaction

from invoices.models import FINGERPRINT_CONSTRAINT, Status
from invoices.observability import INGESTED
from invoices.services import counters

log = structlog.get_logger()

MAX_AMOUNT = Decimal("999999999999.99")  # numeric(14,2)
STRING_FIELDS = ("invoice_id", "invoice_number", "vendor_id", "po_number", "currency",
                 "source", "cost_center")
MAX_STRING = 64


@dataclass(frozen=True)
class CleanInvoice:
    invoice_id: str
    invoice_number: str
    vendor_id: str
    po_number: str
    currency: str
    source: str
    cost_center: str
    amount: Decimal | None


class Rejected(Exception):
    """The record has no usable invoice_id, so there is no key to store it under."""


def _text(raw: dict, key: str) -> str:
    value = raw.get(key)
    if value is None:
        return ""
    return (value if isinstance(value, str) else str(value)).strip()


def _amount(value) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        amount = Decimal(str(value))
    except InvalidOperation:
        return None
    if not amount.is_finite():
        return None
    return amount.quantize(Decimal("0.01"))


def validate(raw) -> tuple[CleanInvoice, str | None]:
    """Return the cleaned record and a reason code, or None if it is valid.

    Raises Rejected when there is no invoice_id (no row can be stored).
    """
    if not isinstance(raw, dict):
        raise Rejected("record is not a JSON object")
    fields = {key: _text(raw, key) for key in STRING_FIELDS}
    if not fields["invoice_id"]:
        raise Rejected("missing invoice_id")
    if len(fields["invoice_id"]) > MAX_STRING:
        raise Rejected("invoice_id longer than 64 characters")

    amount = _amount(raw.get("amount"))
    if amount is not None and abs(amount) > MAX_AMOUNT:
        clean = CleanInvoice(**fields, amount=None)
        return clean, "amount_too_large"
    clean = CleanInvoice(**fields, amount=amount)

    if any(len(v) > MAX_STRING for v in fields.values()):
        return clean, "field_too_long"
    if not clean.vendor_id:
        return clean, "blank_vendor"
    if not clean.po_number:
        return clean, "missing_po"
    if clean.amount is None:
        return clean, "missing_amount"
    if clean.amount <= 0:
        return clean, "non_positive_amount"
    if clean.currency not in settings.ALLOWED_CURRENCIES:
        return clean, "bad_currency"
    if not clean.invoice_number:
        return clean, "missing_invoice_number"
    return clean, None


def fingerprint(clean: CleanInvoice) -> str:
    """Our own content key. We never trust the client's content_hash."""
    basis = "|".join([clean.vendor_id, clean.po_number, f"{clean.amount:.2f}",
                      clean.currency, clean.invoice_number])
    return hashlib.sha256(basis.encode()).hexdigest()


_INSERT = """
INSERT INTO invoice (invoice_id, fingerprint, duplicate_of_id, invoice_number, vendor_id,
                     po_number, amount, currency, source, cost_center, raw, status, reason,
                     trace_id)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
ON CONFLICT (invoice_id) DO NOTHING
RETURNING id
"""
# ON CONFLICT names invoice_id on purpose: a fingerprint conflict must still raise,
# so it can be stored as a DUPLICATE instead of being silently dropped.


def _insert(cur, clean: CleanInvoice, raw_json: str, trace_id: str, *, status: str,
            fp: str | None, reason: str | None = None, duplicate_of: int | None = None
            ) -> int | None:
    cur.execute(_INSERT, [
        clean.invoice_id, fp, duplicate_of, clean.invoice_number, clean.vendor_id,
        clean.po_number, clean.amount, clean.currency, clean.source, clean.cost_center,
        raw_json, status, reason, trace_id,
    ])
    row = cur.fetchone()
    return row[0] if row else None


def _constraint_name(exc: IntegrityError) -> str | None:
    diag = getattr(exc.__cause__, "diag", None)
    return getattr(diag, "constraint_name", None)


def ingest_one(raw: dict, trace_id: str) -> str:
    """Store one record. Returns accepted | duplicate | invalid | already_seen."""
    clean, reason = validate(raw)
    raw_json = json.dumps(raw)

    with transaction.atomic(), connection.cursor() as cur:
        if reason:
            status, result, fp = Status.INVALID, "invalid", None
            new_id = _insert(cur, clean, raw_json, trace_id, status=status, fp=None,
                             reason=reason)
        else:
            fp = fingerprint(clean)
            try:
                with transaction.atomic():  # savepoint: a fingerprint clash rolls back only this
                    status, result = Status.PENDING, "accepted"
                    new_id = _insert(cur, clean, raw_json, trace_id, status=status, fp=fp)
            except IntegrityError as exc:
                if _constraint_name(exc) != FINGERPRINT_CONSTRAINT:
                    raise
                # The unique index only raises once the other row is committed, so it is
                # visible to this new statement.
                cur.execute(
                    "SELECT id FROM invoice WHERE fingerprint = %s "
                    "AND duplicate_of_id IS NULL AND status <> 'INVALID'",
                    [fp],
                )
                original_id = cur.fetchone()[0]
                status, result = Status.DUPLICATE, "duplicate"
                new_id = _insert(cur, clean, raw_json, trace_id, status=status, fp=fp,
                                 duplicate_of=original_id)

        if new_id is None:
            return "already_seen"
        counters.on_transition(None, status)
        return result


_LOG_EVENT = {"accepted": "invoice.received", "duplicate": "invoice.duplicate",
              "invalid": "invoice.invalid", "already_seen": "invoice.already_seen"}


def ingest_batch(records: list, trace_id: str) -> dict:
    summary = {"accepted": 0, "duplicate": 0, "invalid": 0, "already_seen": 0,
               "rejected": [], "trace_id": trace_id}
    for index, raw in enumerate(records):
        try:
            result = ingest_one(raw, trace_id)
        except Rejected as exc:
            summary["rejected"].append({"index": index, "error": str(exc)})
            log.info("invoice.rejected", trace_id=trace_id, index=index, error=str(exc))
            continue
        summary[result] += 1
        INGESTED.labels(result=result).inc()
        log.info(_LOG_EVENT[result], trace_id=trace_id, invoice_id=raw.get("invoice_id"))
    return summary


def backlog_too_big() -> bool:
    """Backpressure: refuse new work when this many invoices are still waiting."""
    waiting = counters.get_count(Status.PENDING, Status.RETRY_SCHEDULED)
    return waiting > settings.PENDING_HARD_LIMIT
