"""What we have learned about each PO from ERP answers (PLANNING.md Step 4).

Redis hash `po:{po_number}`. The ERP has no "get PO" endpoint, but its answers reveal
facts. The cache is only ever used to say NO (matching.decide_from_cache); a stale entry
can cause a wrong rejection, never a wrong payment.
"""
import uuid

from django.conf import settings

from invoices.services.classify import Kind, Outcome
from invoices.services.redis_conn import get_redis

LOCK_MS = 5000

# Compare-and-delete: only the holder's token releases the lock.
_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""


def _key(po: str) -> str:
    return f"po:{po}"


def get(po: str) -> dict | None:
    facts = get_redis().hgetall(_key(po))
    return facts or None


def learn(po: str, invoice_vendor: str, outcome: Outcome) -> None:
    """Merge facts from a MATCHED / UNMATCHED answer; other outcomes teach nothing."""
    body = outcome.body or {}
    if outcome.kind is Kind.MATCHED:
        facts = {"vendor_id": body.get("vendor_id") or invoice_vendor,
                 "po_amount": body.get("po_amount"), "currency": body.get("currency"),
                 "status": body.get("status")}
    elif outcome.kind is Kind.UNMATCHED and outcome.reason == "vendor_mismatch":
        facts = {"vendor_id": body.get("expected_vendor")}
    elif outcome.kind is Kind.UNMATCHED and outcome.reason == "amount_out_of_tolerance":
        # The ERP checks the vendor before the amount, so our vendor was the right one.
        facts = {"vendor_id": invoice_vendor, "po_amount": body.get("po_amount")}
    elif outcome.kind is Kind.UNMATCHED and outcome.reason == "po_not_found":
        pipe = get_redis().pipeline()
        pipe.delete(_key(po))
        pipe.hset(_key(po), "missing", "1")
        pipe.expire(_key(po), settings.PO_NEGATIVE_TTL)
        pipe.execute()
        return
    else:
        return

    facts = {k: str(v) for k, v in facts.items() if v is not None}
    if not facts:
        return
    pipe = get_redis().pipeline()
    pipe.hdel(_key(po), "missing")  # the PO exists after all
    pipe.hset(_key(po), mapping=facts)
    pipe.expire(_key(po), settings.PO_CACHE_TTL)
    pipe.execute()


def try_lock(po: str) -> str | None:
    """Single-flight: only one worker asks the ERP about an unknown PO at a time."""
    token = uuid.uuid4().hex
    if get_redis().set(f"po:lock:{po}", token, nx=True, px=LOCK_MS):
        return token
    return None


def release_lock(po: str, token: str) -> None:
    get_redis().eval(_RELEASE_LUA, 1, f"po:lock:{po}", token)
