"""Global ERP rate limit, shared cooldown and breaker-lite (PLANNING.md 3.3, 3.6.1).

The ERP counts requests per calendar second, so we do the same: one Redis counter per
Redis-clock second, at most ERP_RPS per second across every worker. A token bucket of 10
could send 10 calls at x.00 and 9 more at x.99, which is 19 in one ERP window.
"""
import random
import time
from functools import lru_cache

import structlog
from django.conf import settings

from invoices.observability import ERP_BREAKER_OPEN, LIMITER_WAIT
from invoices.services.classify import Kind, Outcome
from invoices.services.redis_conn import get_redis

log = structlog.get_logger()

RL_KEY = "erp:rl"
COOLDOWN_KEY = "erp:cooldown"
FAIL_STREAK_KEY = "erp:fail_streak"
BREAKER_STREAK = 20
BREAKER_PAUSE_SECONDS = 30

# Returns 0 = go now; otherwise milliseconds to wait.
_LUA = """
local cool = redis.call('PTTL', KEYS[2])
if cool > 0 then return cool end
local t = redis.call('TIME')
local key = KEYS[1] .. ':' .. t[1]
local n = redis.call('INCR', key)
if n == 1 then redis.call('EXPIRE', key, 2) end
if n <= tonumber(ARGV[1]) then return 0 end
return math.floor((1000000 - tonumber(t[2])) / 1000) + 1
"""


class RateLimitBusy(Exception):
    """No ERP slot within LIMITER_MAX_WAIT seconds."""


@lru_cache(maxsize=1)
def _script():
    return get_redis().register_script(_LUA)


def try_acquire() -> int:
    """One attempt: 0 if we got a slot, else milliseconds until it is worth trying again."""
    return int(_script()(keys=[RL_KEY, COOLDOWN_KEY], args=[settings.ERP_RPS]))


def acquire_erp_slot(max_wait: float | None = None) -> None:
    max_wait = settings.LIMITER_MAX_WAIT if max_wait is None else max_wait
    waited = 0.0
    while True:
        wait_ms = try_acquire()
        if wait_ms == 0:
            return
        if waited >= max_wait:
            raise RateLimitBusy()
        # Jitter spreads the threads that wake up at the same second boundary.
        pause = min(wait_ms / 1000 + random.uniform(0, 0.05), max(max_wait - waited, 0.01))
        time.sleep(pause)
        waited += pause
        LIMITER_WAIT.observe(pause)


def start_cooldown(seconds: float) -> None:
    """Pause every worker: the limiter returns the cooldown's remaining time."""
    get_redis().set(COOLDOWN_KEY, 1, px=max(int(seconds * 1000), 1))
    log.info("erp.cooldown_started", seconds=seconds)


def record_health(outcome: Outcome) -> None:
    """Breaker-lite: 20 transient failures in a row across all workers pause everyone 30 s."""
    r = get_redis()
    if outcome.kind is Kind.TRANSIENT:
        if r.incr(FAIL_STREAK_KEY) >= BREAKER_STREAK:
            r.set(FAIL_STREAK_KEY, 0)
            start_cooldown(BREAKER_PAUSE_SECONDS)
            ERP_BREAKER_OPEN.inc()
            log.warning("erp.breaker_open", pause_seconds=BREAKER_PAUSE_SECONDS)
    elif outcome.kind in (Kind.MATCHED, Kind.UNMATCHED):
        r.set(FAIL_STREAK_KEY, 0)
