"""Moves due rows from Postgres into the Redis queue (PLANNING.md 3.7).

Postgres holds the backlog; Redis holds at most QUEUE_HIGH_WATER ids. If this process dies
after marking rows QUEUED but before enqueueing them, their queue lease expires and the
next run picks them up again: nothing is lost.
"""
from collections.abc import Callable

import structlog
from django.conf import settings

from invoices.services import claims
from invoices.services.redis_conn import get_redis

log = structlog.get_logger()

RECONCILE_QUEUE = "reconcile"  # Celery's Redis transport keeps this queue in a list key


def dispatch_due(enqueue: Callable[[int, str], None]) -> int:
    lock = get_redis().lock("dispatch:lock", timeout=10, blocking=False)
    if not lock.acquire():
        return 0  # another dispatcher is running
    try:
        poisoned = claims.dead_letter_poison()
        if poisoned:
            log.warning("invoice.dead_lettered", reason="poison", count=poisoned)

        room = settings.QUEUE_HIGH_WATER - get_redis().llen(RECONCILE_QUEUE)
        if room <= 0:
            return 0
        rows = claims.pick_due(min(settings.DISPATCH_BATCH, room))
        for pk, trace_id in rows:  # after commit: a lost enqueue is recovered by the lease
            enqueue(pk, trace_id)
        if rows:
            log.info("invoice.dispatched", count=len(rows))
        return len(rows)
    finally:
        try:
            lock.release()
        except Exception:  # lock expired already; the next run takes a fresh one
            pass
