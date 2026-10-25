"""Pure matching decisions (PLANNING.md Step 4).

Rule: the cache can say no; only the ERP can say yes. decide_from_cache never returns
MATCHED, so a stale cache entry can wrongly reject an invoice but never approve one.
"""
from decimal import Decimal, InvalidOperation

from invoices.models import Status

TOLERANCE = Decimal("0.01")  # 1% of the PO amount, the ERP's own rule


def decide_from_cache(inv, facts: dict | None) -> tuple[str, str] | None:
    """("UNMATCHED", reason) when cached PO facts already prove a mismatch, else None."""
    if not facts:
        return None
    if facts.get("missing"):
        return (Status.UNMATCHED, "po_not_found")
    if facts.get("vendor_id") and facts["vendor_id"] != inv.vendor_id:
        return (Status.UNMATCHED, "vendor_mismatch")
    if facts.get("po_amount"):
        try:
            po_amount = Decimal(facts["po_amount"])
        except InvalidOperation:
            return None  # unreadable fact: let the ERP decide
        if abs(inv.amount - po_amount) > po_amount * TOLERANCE:
            return (Status.UNMATCHED, "amount_out_of_tolerance")
    return None


def check_currency(inv, body: dict) -> tuple[str, str | None]:
    """The ERP approves without comparing currencies, so we do it."""
    if body.get("currency") and body["currency"] != inv.currency:
        return (Status.UNMATCHED, "currency_mismatch")
    return (Status.MATCHED, None)
