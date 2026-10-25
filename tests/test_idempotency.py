"""Tests 1-2: the same invoice, or the same bill under a new ID, is stored once."""
import threading

import pytest
from django.db import connection
from django.test import Client

from invoices.models import Invoice, Status, StatusCount
from tests.conftest import invoice

pytestmark = pytest.mark.django_db(transaction=True)


def _post(records):
    return Client().post("/invoices", records, content_type="application/json")


def _counts_match_rows():
    actual = {s: Invoice.objects.filter(status=s).count() for s in Status.values}
    counted = {s: 0 for s in Status.values}
    counted.update(dict(StatusCount.objects.values_list("status", "count")))
    return actual == counted


def test_20_concurrent_posts_of_the_same_invoice_store_one_row():
    """Case 7: at-least-once upstream sends the same invoice 20 times at once."""
    barrier = threading.Barrier(20)
    results, errors = [], []

    def send():
        try:
            barrier.wait()
            body = _post([invoice()]).json()
            results.append(next(k for k in ("accepted", "already_seen") if body[k] == 1))
        except Exception as exc:  # surfaced below; a thread must not die silently
            errors.append(exc)
        finally:
            connection.close()

    threads = [threading.Thread(target=send) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert sorted(results) == ["accepted"] + ["already_seen"] * 19
    assert Invoice.objects.filter(invoice_id="INV-000000123").count() == 1
    assert _counts_match_rows()


def test_near_duplicate_in_a_later_request_is_stored_as_duplicate():
    """Case 8: same bill, new invoice_id -> DUPLICATE pointing at the original."""
    assert _post([invoice()]).json()["accepted"] == 1
    assert _post([invoice(invoice_id="INV-000000999-N")]).json()["duplicate"] == 1

    original = Invoice.objects.get(invoice_id="INV-000000123")
    dup = Invoice.objects.get(invoice_id="INV-000000999-N")
    assert original.status == Status.PENDING
    assert dup.status == Status.DUPLICATE
    assert dup.duplicate_of_id == original.pk
    assert _counts_match_rows()


def test_near_duplicate_inside_one_batch_is_stored_as_duplicate():
    body = _post([invoice(), invoice(invoice_id="INV-000000999-N")]).json()
    assert (body["accepted"], body["duplicate"]) == (1, 1)
    dup = Invoice.objects.get(invoice_id="INV-000000999-N")
    assert dup.duplicate_of.invoice_id == "INV-000000123"


def test_different_amount_is_a_different_bill():
    body = _post([invoice(), invoice(invoice_id="INV-2", amount=4210.56)]).json()
    assert body["accepted"] == 2


@pytest.mark.parametrize("changes, reason", [
    ({"amount": None}, "missing_amount"),
    ({"vendor_id": ""}, "blank_vendor"),
    ({"amount": -5}, "non_positive_amount"),
    ({"currency": "XYZ"}, "bad_currency"),
    ({"po_number": ""}, "missing_po"),
])
def test_malformed_invoice_is_stored_invalid_and_never_dispatched(changes, reason):
    """Case 6."""
    assert _post([invoice(**changes)]).json()["invalid"] == 1
    row = Invoice.objects.get(invoice_id="INV-000000123")
    assert (row.status, row.reason, row.fingerprint) == (Status.INVALID, reason, None)


def test_record_without_invoice_id_is_rejected_not_stored():
    body = _post([invoice(invoice_id="")]).json()
    assert body["rejected"] == [{"index": 0, "error": "missing invoice_id"}]
    assert Invoice.objects.count() == 0
