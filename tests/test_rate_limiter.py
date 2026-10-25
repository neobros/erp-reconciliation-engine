"""Tests 6 and 9: the global limiter across threads, the cooldown, and breaker-lite.
Real Redis: the Lua script and Redis' clock are the thing under test."""
import threading
import time
from collections import Counter

import pytest

from invoices.services import rate_limiter
from invoices.services.classify import Kind, Outcome


def test_no_redis_clock_second_gets_more_than_erp_rps_slots(redis_db, settings):
    """30 threads hammer the limiter for 3 s. Each success is attributed to a Redis-clock
    second by reading TIME just before and just after acquiring. Successes whose before and
    after fall in the same second are certain; that second must never exceed ERP_RPS."""
    deadline = time.monotonic() + 3
    successes = []  # (second_before, second_after)
    busy = []  # 30 threads share 8 slots/s, so some legitimately give up (RateLimitBusy)
    lock = threading.Lock()

    def hammer():
        while time.monotonic() < deadline:
            before = redis_db.time()[0]
            try:
                rate_limiter.acquire_erp_slot(max_wait=2)
            except rate_limiter.RateLimitBusy:
                busy.append(1)
                continue
            after = redis_db.time()[0]
            with lock:
                successes.append((before, after))

    threads = [threading.Thread(target=hammer) for _ in range(30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    certain = Counter(b for b, a in successes if b == a)
    assert certain, "expected some successes"
    assert max(certain.values()) <= settings.ERP_RPS
    seconds_touched = {s for pair in successes for s in pair}
    assert len(successes) <= settings.ERP_RPS * len(seconds_touched)
    assert len(successes) >= settings.ERP_RPS * 2  # and it does not starve


def test_cooldown_blocks_every_caller(redis_db):
    rate_limiter.start_cooldown(1.0)
    assert 0 < rate_limiter.try_acquire() <= 1000
    with pytest.raises(rate_limiter.RateLimitBusy):
        rate_limiter.acquire_erp_slot(max_wait=0.2)


def test_slot_is_free_again_after_cooldown_expires(redis_db):
    rate_limiter.start_cooldown(0.1)
    time.sleep(0.15)
    rate_limiter.acquire_erp_slot(max_wait=1)  # does not raise


def test_20_transient_failures_in_a_row_pause_everyone(redis_db):
    """Test 9: breaker-lite."""
    transient = Outcome(Kind.TRANSIENT, "http_503")
    for _ in range(19):
        rate_limiter.record_health(transient)
    assert redis_db.pttl(rate_limiter.COOLDOWN_KEY) < 0  # not yet
    rate_limiter.record_health(transient)
    assert redis_db.pttl(rate_limiter.COOLDOWN_KEY) > 25_000
    assert redis_db.get(rate_limiter.FAIL_STREAK_KEY) == "0"


def test_a_success_resets_the_failure_streak(redis_db):
    for _ in range(19):
        rate_limiter.record_health(Outcome(Kind.TRANSIENT, "timeout"))
    rate_limiter.record_health(Outcome(Kind.UNMATCHED, "vendor_mismatch"))
    rate_limiter.record_health(Outcome(Kind.TRANSIENT, "timeout"))
    assert redis_db.pttl(rate_limiter.COOLDOWN_KEY) < 0
