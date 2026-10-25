"""Data model (PLANNING.md section 4).

Ingest and the worker write these tables with raw SQL (services/), so every column the
SQL leaves out has a database-level default (`db_default`), not only a Python default.
"""
from django.db import models
from django.db.models import Q
from django.db.models.functions import Now


class Status(models.TextChoices):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    MATCHED = "MATCHED"
    UNMATCHED = "UNMATCHED"
    INVALID = "INVALID"
    DUPLICATE = "DUPLICATE"
    DEAD_LETTERED = "DEAD_LETTERED"


FINAL_STATUSES = frozenset(
    {Status.MATCHED, Status.UNMATCHED, Status.INVALID, Status.DUPLICATE, Status.DEAD_LETTERED}
)
UNFINISHED_STATUSES = [Status.PENDING, Status.QUEUED, Status.PROCESSING, Status.RETRY_SCHEDULED]

FINGERPRINT_CONSTRAINT = "uniq_invoice_fingerprint_original"


class Invoice(models.Model):
    invoice_id = models.CharField(max_length=64, unique=True)  # Guard 1
    fingerprint = models.CharField(max_length=64, null=True)  # Guard 2 (NULL when INVALID)
    duplicate_of = models.ForeignKey(
        "self", null=True, on_delete=models.PROTECT, related_name="duplicates"
    )

    invoice_number = models.CharField(max_length=64, blank=True)
    vendor_id = models.CharField(max_length=64, blank=True)
    po_number = models.CharField(max_length=64, blank=True)
    currency = models.CharField(max_length=64, blank=True)
    source = models.CharField(max_length=64, blank=True)
    cost_center = models.CharField(max_length=64, blank=True)
    amount = models.DecimalField(max_digits=14, decimal_places=2, null=True)  # never float
    raw = models.JSONField()

    status = models.CharField(max_length=20, choices=Status.choices)
    reason = models.CharField(max_length=64, null=True)

    attempts = models.IntegerField(default=0, db_default=0)
    crash_count = models.IntegerField(default=0, db_default=0)
    rate_limited_count = models.IntegerField(default=0, db_default=0)
    next_attempt_at = models.DateTimeField(null=True)
    lease_token = models.UUIDField(null=True)  # Guard 3
    lease_until = models.DateTimeField(null=True)
    last_error = models.TextField(blank=True, default="", db_default="")
    trace_id = models.CharField(max_length=64)

    created_at = models.DateTimeField(db_default=Now())
    updated_at = models.DateTimeField(db_default=Now())
    finished_at = models.DateTimeField(null=True)
    dead_lettered_at = models.DateTimeField(null=True)

    class Meta:
        db_table = "invoice"
        constraints = [
            models.CheckConstraint(
                condition=Q(status__in=Status.values), name="invoice_status_valid"
            ),
            # One original per fingerprint. DUPLICATE rows point at it via duplicate_of.
            models.UniqueConstraint(
                fields=["fingerprint"],
                condition=Q(duplicate_of__isnull=True) & ~Q(status=Status.INVALID),
                name=FINGERPRINT_CONSTRAINT,
            ),
        ]
        indexes = [
            models.Index(
                fields=["status", "next_attempt_at"],
                condition=Q(status__in=[Status.PENDING, Status.RETRY_SCHEDULED]),
                name="invoice_dispatch_idx",
            ),
            models.Index(
                fields=["lease_until"],
                condition=Q(status__in=[Status.QUEUED, Status.PROCESSING]),
                name="invoice_lease_idx",
            ),
            models.Index(
                fields=["updated_at"],
                condition=Q(status__in=UNFINISHED_STATUSES),
                name="invoice_stuck_idx",
            ),
            models.Index(
                fields=["created_at"],
                condition=Q(status__in=UNFINISHED_STATUSES),
                name="invoice_lag_idx",
            ),
            models.Index(
                fields=["status", "dead_lettered_at"],
                condition=Q(status=Status.DEAD_LETTERED),
                name="invoice_dlq_idx",
            ),
        ]


class ReconciliationResult(models.Model):
    """One row per reconciled invoice. matched=True rows are the 'ready for payment' list."""

    invoice = models.OneToOneField(Invoice, on_delete=models.PROTECT, related_name="result")
    matched = models.BooleanField()
    reason = models.CharField(max_length=64, null=True)
    po_amount = models.DecimalField(max_digits=14, decimal_places=2, null=True)
    po_currency = models.CharField(max_length=8, null=True)
    decided_by = models.CharField(
        max_length=8, choices=[("erp", "erp"), ("cache", "cache")]
    )
    erp_response = models.JSONField(null=True)
    created_at = models.DateTimeField(db_default=Now())

    class Meta:
        db_table = "reconciliation_result"


class InvoiceAttempt(models.Model):
    """One row per ERP call, kept forever for audit (also after a dead-letter requeue)."""

    invoice = models.ForeignKey(Invoice, on_delete=models.PROTECT, related_name="erp_attempts")
    attempt_no = models.IntegerField()
    started_at = models.DateTimeField()
    duration_ms = models.IntegerField()
    http_status = models.IntegerField(null=True)
    error = models.CharField(max_length=128, null=True)
    error_class = models.CharField(
        max_length=16,
        choices=[(c, c) for c in ("transient", "permanent", "rate_limited", "ok")],
    )
    trace_id = models.CharField(max_length=64)

    class Meta:
        db_table = "invoice_attempt"


class StatusCount(models.Model):
    """Live count per status, changed in the same transaction as each status change."""

    status = models.CharField(max_length=20, primary_key=True)
    count = models.BigIntegerField(default=0, db_default=0)

    class Meta:
        db_table = "status_count"


class OutcomeHourly(models.Model):
    """Final outcomes per hour, for 'match rate this hour' without scanning invoices."""

    hour = models.DateTimeField()
    outcome = models.CharField(max_length=20)
    count = models.BigIntegerField(default=0, db_default=0)

    class Meta:
        db_table = "outcome_hourly"
        constraints = [
            models.UniqueConstraint(fields=["hour", "outcome"], name="uniq_outcome_hour")
        ]
