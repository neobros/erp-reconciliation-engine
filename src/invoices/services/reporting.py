"""Operational answers that stay fast at 12M rows (PLANNING.md 5.2, DESIGN.md section 6).

Every query here reads a small counter table or a partial index of *unfinished* rows, so its
cost depends on the backlog, never on the size of the invoice table.
"""
import json
import time

from django.conf import settings
from django.db import connection
from django.utils import timezone

from invoices.models import UNFINISHED_STATUSES, Status
from invoices.services.dispatcher import RECONCILE_QUEUE
from invoices.services.redis_conn import get_redis

SUMMARY_CACHE_SECONDS = 5
COMPLETIONS_KEY = "done:"  # + epoch minute; one counter per minute
DRAIN_WINDOW_MINUTES = 5

_UNFINISHED_SQL = "(" + ",".join(f"'{s}'" for s in UNFINISHED_STATUSES) + ")"


def note_completion() -> None:
    """Called after each final outcome; feeds completions/s for the drain estimate."""
    key = f"{COMPLETIONS_KEY}{int(time.time() // 60)}"
    pipe = get_redis().pipeline()
    pipe.incr(key)
    pipe.expire(key, (DRAIN_WINDOW_MINUTES + 2) * 60)
    pipe.execute()


def completions_per_sec() -> float:
    """Average over the last 5 whole minutes (the current, partial minute is left out)."""
    now_min = int(time.time() // 60)
    keys = [f"{COMPLETIONS_KEY}{m}" for m in range(now_min - DRAIN_WINDOW_MINUTES, now_min)]
    return sum(int(v or 0) for v in get_redis().mget(keys)) / (DRAIN_WINDOW_MINUTES * 60)


def status_counts() -> dict[str, int]:
    counts = {s: 0 for s in Status.values}
    with connection.cursor() as cur:
        cur.execute("SELECT status, count FROM status_count")
        counts.update(dict(cur.fetchall()))
    return counts


def queue_depth() -> int:
    return get_redis().llen(RECONCILE_QUEUE)


def oldest_unfinished_age_seconds() -> float:
    with connection.cursor() as cur:  # partial index invoice_lag_idx
        cur.execute(f"SELECT extract(epoch FROM now() - min(created_at)) FROM invoice "
                    f"WHERE status IN {_UNFINISHED_SQL}")
        age = cur.fetchone()[0]
    return float(age) if age is not None else 0.0


def lag(counts: dict[str, int]) -> dict:
    unfinished = sum(counts[s] for s in UNFINISHED_STATUSES)
    rate = completions_per_sec()
    if unfinished == 0:
        drain = 0
    elif rate > 0:
        drain = round(unfinished / rate)
    else:
        drain = -1  # nothing completed recently: cannot estimate
    return {"oldest_unfinished_age_seconds": round(oldest_unfinished_age_seconds(), 1),
            "completions_per_sec_5m": round(rate, 2), "estimated_drain_seconds": drain}


def _window(hours: int) -> dict:
    with connection.cursor() as cur:
        cur.execute(
            "SELECT outcome, sum(count) FROM outcome_hourly "
            "WHERE hour >= date_trunc('hour', now()) - make_interval(hours => %s) "
            "AND outcome IN ('MATCHED', 'UNMATCHED') GROUP BY outcome",
            [hours - 1],
        )
        rows = dict(cur.fetchall())
    matched, unmatched = int(rows.get("MATCHED", 0)), int(rows.get("UNMATCHED", 0))
    total = matched + unmatched
    return {"matched": matched, "unmatched": unmatched,
            "match_rate": round(matched / total, 4) if total else None}


def _stuck() -> dict:
    with connection.cursor() as cur:  # partial index invoice_stuck_idx
        where = (f"WHERE status IN {_UNFINISHED_SQL} "
                 f"AND updated_at < now() - make_interval(secs => %s)")
        cur.execute(f"SELECT count(*) FROM invoice {where}", [settings.STUCK_AFTER_SECONDS])
        count = cur.fetchone()[0]
        cur.execute(
            f"SELECT invoice_id, status, extract(epoch FROM now() - updated_at), attempts, "
            f"last_error FROM invoice {where} ORDER BY updated_at LIMIT 20",
            [settings.STUCK_AFTER_SECONDS],
        )
        oldest = [{"invoice_id": i, "status": s, "age_seconds": round(float(a)),
                   "attempts": n, "last_error": e} for i, s, a, n, e in cur.fetchall()]
    return {"count": count, "oldest": oldest}


def _dead_letter_reasons() -> dict:
    with connection.cursor() as cur:  # partial index invoice_dlq_idx
        cur.execute("SELECT reason, count(*) FROM invoice WHERE status = 'DEAD_LETTERED' "
                    "GROUP BY reason")
        return dict(cur.fetchall())


def summary(window_hours: int) -> dict:
    """Cached for 5 s, so a dashboard refreshing every second cannot load the database."""
    r = get_redis()
    cache_key = f"report:summary:{window_hours}"
    cached = r.get(cache_key)
    if cached:
        return json.loads(cached)

    counts = status_counts()
    data = {
        "generated_at": timezone.now().isoformat(),
        "counts": counts,
        "window_hours": window_hours,
        "window": _window(window_hours),
        "stuck": _stuck(),
        "queue_depth": queue_depth(),
        "lag": lag(counts),
        "dead_letter_reasons": _dead_letter_reasons(),
    }
    r.set(cache_key, json.dumps(data), ex=SUMMARY_CACHE_SECONDS)
    return data
