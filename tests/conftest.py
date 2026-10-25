"""Shared fixtures. Tests run against the real Postgres and Redis from docker compose:
the constraints and the Lua script are what is being tested, so they are not mocked.
Redis uses database 15 so a running pipeline (database 0) is not disturbed."""
import re
from decimal import Decimal

import pytest

# PLANNING.md 5.1: the PO every case uses, and the base invoice each case changes.
PO = {"po_number": "PO-001234", "vendor_id": "V-1003", "po_amount": 4200.00,
      "currency": "USD", "status": "OPEN"}
BASE_INVOICE = {
    "invoice_id": "INV-000000123", "invoice_number": "1003-48217", "vendor_id": "V-1003",
    "vendor_name": "Cornerstone Electric", "po_number": "PO-001234", "amount": 4210.55,
    "currency": "USD", "cost_center": "CC-014", "source": "property-erp",
    "issued_at": "2025-09-14T08:22:00Z", "submitted_at": "2025-09-20T08:22:00Z",
}
MATCHED_BODY = {"matched": True, **PO, "match_confidence": "exact"}


@pytest.fixture(autouse=True)
def redis_db(settings):
    from invoices.services import rate_limiter
    from invoices.services.redis_conn import get_redis

    settings.REDIS_URL = re.sub(r"/\d+$", "/15", settings.REDIS_URL)
    get_redis.cache_clear()
    rate_limiter._script.cache_clear()
    r = get_redis()
    r.flushdb()
    yield r
    r.flushdb()
    get_redis.cache_clear()
    rate_limiter._script.cache_clear()


def invoice(**changes) -> dict:
    return {**BASE_INVOICE, **changes}


def ingest(**changes) -> int:
    """Insert one invoice through the real ingest path; returns its primary key."""
    from invoices.models import Invoice
    from invoices.services.ingest import ingest_one

    raw = invoice(**changes)
    ingest_one(raw, trace_id="test")
    return Invoice.objects.get(invoice_id=raw["invoice_id"]).pk


def dispatch_now() -> list[int]:
    """Act as the dispatcher: make retries due now, then mark due rows QUEUED."""
    from django.db import connection

    from invoices.services import claims

    with connection.cursor() as cur:
        cur.execute("UPDATE invoice SET next_attempt_at = now() "
                    "WHERE status = 'RETRY_SCHEDULED'")
    return [pk for pk, _ in claims.pick_due(100)]


@pytest.fixture
def erp(settings):
    """respx router for the ERP match endpoint (no network)."""
    import respx

    with respx.mock(base_url=settings.ERP_BASE_URL, assert_all_called=False) as router:
        yield router.get("/erp/purchase-orders/match")


@pytest.fixture
def dec():
    return Decimal
