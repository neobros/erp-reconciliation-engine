"""Counter tables, updated inside the caller's transaction (PLANNING.md 5.1).

Deadlock rule: every transaction updates counter rows in the same order (status_count
rows sorted by status, then outcome_hourly rows sorted by outcome), each table in a single
statement. Two transactions can then never wait on each other's counter rows in a cycle.
"""
from collections import Counter
from collections.abc import Iterable

from django.db import connection

from invoices.models import FINAL_STATUSES, StatusCount


def on_transition(old: str | None, new: str) -> None:
    record_transitions([(old, new)])


def record_transitions(changes: Iterable[tuple[str | None, str]]) -> None:
    """Apply many (old_status, new_status) changes; `old=None` means a new row."""
    deltas: Counter[str] = Counter()
    finals: Counter[str] = Counter()
    for old, new in changes:
        if old == new:
            continue
        if old is not None:
            deltas[old] -= 1
        deltas[new] += 1
        if new in FINAL_STATUSES:
            finals[new] += 1

    rows = sorted((s, d) for s, d in deltas.items() if d != 0)
    with connection.cursor() as cur:
        if rows:
            values = ", ".join(["(%s, %s)"] * len(rows))
            cur.execute(
                f"INSERT INTO status_count (status, count) VALUES {values} "
                "ON CONFLICT (status) DO UPDATE SET count = status_count.count + EXCLUDED.count",
                [x for row in rows for x in row],
            )
        final_rows = sorted(finals.items())
        if final_rows:
            values = ", ".join(["(date_trunc('hour', now()), %s, %s)"] * len(final_rows))
            cur.execute(
                f"INSERT INTO outcome_hourly (hour, outcome, count) VALUES {values} "
                "ON CONFLICT (hour, outcome) "
                "DO UPDATE SET count = outcome_hourly.count + EXCLUDED.count",
                [x for row in final_rows for x in row],
            )


def get_count(*statuses: str) -> int:
    return sum(
        StatusCount.objects.filter(status__in=statuses).values_list("count", flat=True)
    )
