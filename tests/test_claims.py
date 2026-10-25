"""Tests 3-4: leases make the outcome exactly-once even when workers crash or race."""
import threading

import pytest
from django.db import connection

from invoices.models import Invoice, ReconciliationResult, Status
from invoices.services import claims
from invoices.services.claims import ResultData
from tests.conftest import dispatch_now, ingest

pytestmark = pytest.mark.django_db(transaction=True)

RESULT = ResultData(matched=False, decided_by="erp")


def _expire_lease(pk):
    with connection.cursor() as cur:
        cur.execute("UPDATE invoice SET lease_until = now() - interval '1 second' WHERE id = %s",
                    [pk])


def test_stalled_worker_cannot_overwrite_after_its_lease_was_taken_over():
    """Case 12: worker 1 claims, freezes past its lease; worker 2 finishes; worker 1 wakes."""
    pk = ingest()
    assert dispatch_now() == [pk]
    first = claims.claim(pk)
    assert first is not None

    _expire_lease(pk)
    assert dispatch_now() == [pk]  # dispatcher re-queues the expired PROCESSING row
    assert Invoice.objects.get(pk=pk).crash_count == 1

    second = claims.claim(pk)
    assert second is not None and second.lease_token != first.lease_token
    assert claims.finish(second, Status.UNMATCHED, "vendor_mismatch", RESULT) is True

    # Worker 1 wakes up with its old token: every write is refused.
    assert claims.finish(first, Status.MATCHED, None, RESULT) is False
    assert claims.reschedule(first, 1, count_attempt=True, count_rate_limited=False,
                             error="x") is False
    assert claims.dead_letter(first, "max_attempts", "x") is False

    row = Invoice.objects.get(pk=pk)
    assert (row.status, row.reason) == (Status.UNMATCHED, "vendor_mismatch")
    assert ReconciliationResult.objects.filter(invoice_id=pk).count() == 1


def test_two_workers_claiming_the_same_row_only_one_wins():
    pk = ingest()
    dispatch_now()
    barrier = threading.Barrier(2)
    got = []

    def worker():
        try:
            barrier.wait()
            got.append(claims.claim(pk))
        finally:
            connection.close()

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(c is not None for c in got) == 1


def test_claim_of_a_finished_row_is_a_no_op():
    """A duplicate Celery delivery after the outcome was recorded does nothing."""
    pk = ingest()
    dispatch_now()
    c = claims.claim(pk)
    claims.finish(c, Status.UNMATCHED, "po_not_found", RESULT)
    assert claims.claim(pk) is None


def test_row_whose_lease_keeps_expiring_is_dead_lettered_as_poison(settings):
    pk = ingest()
    for _ in range(settings.MAX_CRASHES - 1):
        dispatch_now()
        claims.claim(pk)
        _expire_lease(pk)
    dispatch_now()
    claims.claim(pk)
    _expire_lease(pk)

    assert claims.dead_letter_poison() == 1
    row = Invoice.objects.get(pk=pk)
    assert (row.status, row.reason) == (Status.DEAD_LETTERED, "poison")


def test_requeue_dead_letters_resets_counters_and_keeps_status_counts_right():
    pk = ingest()
    dispatch_now()
    c = claims.claim(pk)
    claims.dead_letter(c, "max_attempts", "http_503", count_attempt=True)

    assert claims.requeue_dead_letters(reason="max_attempts", dry_run=True) == 1
    assert claims.requeue_dead_letters(reason="max_attempts") == 1
    row = Invoice.objects.get(pk=pk)
    assert (row.status, row.attempts, row.reason) == (Status.PENDING, 0, None)
    with pytest.raises(ValueError):
        claims.requeue_dead_letters()  # no filter: refuse to requeue everything
