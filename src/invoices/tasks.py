"""Celery entry points. Thin on purpose: the logic lives in services/.

Retries are never Celery retries (no countdown / eta / self.retry): a retry is a
RETRY_SCHEDULED row that the dispatcher re-sends when it is due.
"""
import structlog
from celery import shared_task

from invoices.services import dispatcher
from invoices.services import reconcile as reconcile_service

log = structlog.get_logger()


@shared_task(bind=True, acks_late=True, reject_on_worker_lost=True, ignore_result=True)
def reconcile(self, invoice_pk: int) -> None:
    # trace_id travels in the message headers, so even "claim lost" lines are traceable.
    trace_id = self.request.get("trace_id") or (self.request.headers or {}).get("trace_id")
    with structlog.contextvars.bound_contextvars(trace_id=trace_id):
        reconcile_service.process(invoice_pk)


def _enqueue(pk: int, trace_id: str) -> None:
    reconcile.apply_async(args=[pk], queue=dispatcher.RECONCILE_QUEUE,
                          headers={"trace_id": trace_id})
    log.info("invoice.dispatched", invoice_pk=pk, trace_id=trace_id)


@shared_task(ignore_result=True)
def dispatch() -> int:
    return dispatcher.dispatch_due(_enqueue)
