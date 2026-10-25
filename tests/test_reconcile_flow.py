"""Test 8: the worker flow end to end against a mocked ERP (respx), real Postgres + Redis.
Each `dispatch_now(); process(pk)` pair is one trip round the pipeline."""
import httpx
import pytest

from invoices.models import Invoice, InvoiceAttempt, ReconciliationResult, Status
from invoices.services.reconcile import process
from tests.conftest import MATCHED_BODY, dispatch_now, ingest

pytestmark = pytest.mark.django_db(transaction=True)

R503 = httpx.Response(503, json={"error": "upstream_unavailable"})
R429 = httpx.Response(429, json={"error": "rate_limited"}, headers={"Retry-After": "0.05"})
R_MATCH = httpx.Response(200, json=MATCHED_BODY)


def run_once(pk):
    assert pk in dispatch_now()
    process(pk)
    return Invoice.objects.get(pk=pk)


def test_happy_path_matches_once(erp):
    """Case 1."""
    erp.mock(return_value=R_MATCH)
    pk = ingest()
    row = run_once(pk)
    assert row.status == Status.MATCHED
    result = ReconciliationResult.objects.get(invoice_id=pk)
    assert (result.matched, result.decided_by) == (True, "erp")
    assert str(result.po_amount) == "4200.00"


def test_503_six_times_is_dead_lettered_with_the_reason(erp, settings):
    """Case 11."""
    erp.mock(return_value=R503)
    pk = ingest()
    for attempt in range(1, settings.MAX_ATTEMPTS):
        row = run_once(pk)
        assert (row.status, row.attempts, row.last_error) == (
            Status.RETRY_SCHEDULED, attempt, "http_503")
    row = run_once(pk)
    assert (row.status, row.reason, row.last_error) == (
        Status.DEAD_LETTERED, "max_attempts", "http_503")
    assert row.attempts == settings.MAX_ATTEMPTS
    assert InvoiceAttempt.objects.filter(invoice_id=pk).count() == settings.MAX_ATTEMPTS
    assert not ReconciliationResult.objects.filter(invoice_id=pk).exists()


def test_429s_never_count_as_attempts(erp, redis_db):
    """Case 10: three 429s, then a match -> MATCHED with attempts == 0."""
    erp.side_effect = [R429, R429, R429, R_MATCH]
    pk = ingest()
    for _ in range(3):
        row = run_once(pk)
        assert row.status == Status.RETRY_SCHEDULED
        assert redis_db.exists("erp:cooldown")  # every worker pauses, not just this one
    row = run_once(pk)
    assert (row.status, row.attempts, row.rate_limited_count) == (Status.MATCHED, 0, 3)


def test_timeout_is_retried_then_matches(erp):
    """Case 9 with a timeout instead of a 503."""
    erp.side_effect = [httpx.ReadTimeout("hang"), R_MATCH]
    pk = ingest()
    row = run_once(pk)
    assert (row.status, row.attempts, row.last_error) == (Status.RETRY_SCHEDULED, 1, "timeout")
    row = run_once(pk)
    assert row.status == Status.MATCHED
    assert list(InvoiceAttempt.objects.filter(invoice_id=pk).order_by("id")
                .values_list("error_class", flat=True)) == ["transient", "ok"]


def test_permanent_error_is_dead_lettered_at_once(erp):
    erp.mock(return_value=httpx.Response(400, json={"detail": "bad request"}))
    row = run_once(ingest())
    assert (row.status, row.reason, row.last_error) == (
        Status.DEAD_LETTERED, "permanent_error", "http_400")


def test_currency_mismatch_is_unmatched_although_the_erp_matched(erp):
    """Case 5."""
    erp.mock(return_value=R_MATCH)  # PO is USD
    row = run_once(ingest(currency="EUR"))
    assert (row.status, row.reason) == (Status.UNMATCHED, "currency_mismatch")
    assert ReconciliationResult.objects.get(invoice_id=row.pk).matched is False


def test_cache_rejects_a_known_wrong_vendor_but_never_approves(erp):
    """Case 3: A teaches the cache the PO's vendor; B is rejected locally; C still asks."""
    erp.side_effect = [
        httpx.Response(200, json={"matched": False, "reason": "vendor_mismatch",
                                  "po_number": "PO-001234", "expected_vendor": "V-1003"}),
        R_MATCH,
    ]
    a = run_once(ingest(invoice_id="INV-A", vendor_id="V-1007"))
    assert (a.status, a.reason) == (Status.UNMATCHED, "vendor_mismatch")
    assert erp.call_count == 1

    b = run_once(ingest(invoice_id="INV-B", vendor_id="V-1007", amount=999.0,
                        invoice_number="X-2"))
    assert (b.status, b.reason) == (Status.UNMATCHED, "vendor_mismatch")
    assert erp.call_count == 1  # no ERP call
    assert ReconciliationResult.objects.get(invoice_id=b.pk).decided_by == "cache"

    c = run_once(ingest(invoice_id="INV-C", amount=4199.00, invoice_number="X-3"))
    assert c.status == Status.MATCHED
    assert erp.call_count == 2  # consistent facts: only the ERP can say yes


def test_unknown_po_is_single_flight(erp, redis_db):
    """Case 13: while one worker holds the PO lock, another reschedules without an attempt."""
    erp.mock(return_value=R_MATCH)
    redis_db.set("po:lock:PO-001234", "someone-else", px=5000)
    row = run_once(ingest())
    assert (row.status, row.attempts, row.last_error) == (Status.RETRY_SCHEDULED, 0, "po_lock")
    assert erp.call_count == 0
