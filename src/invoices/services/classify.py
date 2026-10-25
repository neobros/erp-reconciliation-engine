"""Pure retry policy: what an ERP response or exception means (PLANNING.md 3.2, 3.5).
No I/O here, so every row of the table is a one-line test."""
import random
from dataclasses import dataclass
from enum import StrEnum

import httpx

TRANSIENT_STATUSES = {500, 502, 503, 504}


class Kind(StrEnum):
    MATCHED = "matched"
    UNMATCHED = "unmatched"
    RATE_LIMITED = "rate_limited"
    TRANSIENT = "transient"
    PERMANENT = "permanent"


@dataclass(frozen=True)
class Outcome:
    kind: Kind
    reason: str | None = None  # e.g. "vendor_mismatch", "http_503", "timeout"
    retry_after: float | None = None  # seconds, for RATE_LIMITED
    body: dict | None = None


def _retry_after(response: httpx.Response) -> float:
    try:
        value = float(response.headers.get("Retry-After", "1"))
    except ValueError:  # e.g. an HTTP-date; we only honour seconds
        return 1.0
    return value if value > 0 else 1.0


def classify(response: httpx.Response | None, exc: Exception | None) -> Outcome:
    if exc is not None:
        if isinstance(exc, httpx.TimeoutException):
            return Outcome(Kind.TRANSIENT, "timeout")
        if isinstance(exc, httpx.TransportError):  # connect/read/protocol errors
            return Outcome(Kind.TRANSIENT, "connection_error")
        return Outcome(Kind.TRANSIENT, "internal_error")  # a bug on our side; retried, then DLQ

    code = response.status_code
    if code == 429:
        return Outcome(Kind.RATE_LIMITED, "http_429", retry_after=_retry_after(response))
    if code in TRANSIENT_STATUSES:
        return Outcome(Kind.TRANSIENT, f"http_{code}")
    if code != 200:
        return Outcome(Kind.PERMANENT, f"http_{code}")

    try:
        body = response.json()
    except ValueError:
        return Outcome(Kind.TRANSIENT, "bad_body")
    if not isinstance(body, dict) or not isinstance(body.get("matched"), bool):
        return Outcome(Kind.TRANSIENT, "bad_body")
    if body["matched"]:
        return Outcome(Kind.MATCHED, body=body)  # currency is checked in matching.py
    return Outcome(Kind.UNMATCHED, body.get("reason") or "erp_unmatched", body=body)


def backoff_seconds(attempt: int, base: float = 2, cap: float = 300) -> float:
    """Full jitter: attempt 1 waits 0-4 s, attempt 2 waits 0-8 s, ... capped at 300 s."""
    return random.uniform(0, min(cap, base * 2**attempt))
