"""Test 7: the matching rule's local parts. Pure, no I/O.

PO-001234: V-1003, 4200.00 USD. 1% tolerance = 42.00 -> 4158.00 ... 4242.00 are consistent.
"""
import itertools
from decimal import Decimal
from types import SimpleNamespace

import pytest

from invoices.models import Status
from invoices.services.matching import check_currency, decide_from_cache

FACTS = {"vendor_id": "V-1003", "po_amount": "4200.0", "currency": "USD", "status": "OPEN"}


def inv(vendor="V-1003", amount="4210.55", currency="USD"):
    return SimpleNamespace(vendor_id=vendor, amount=Decimal(amount), currency=currency)


@pytest.mark.parametrize("amount, expected", [
    ("4242.00", None),  # exactly +1%: consistent, so the ERP decides
    ("4158.00", None),  # exactly -1%
    ("4242.01", (Status.UNMATCHED, "amount_out_of_tolerance")),
    ("4157.99", (Status.UNMATCHED, "amount_out_of_tolerance")),
    ("6300.00", (Status.UNMATCHED, "amount_out_of_tolerance")),  # Case 2
])
def test_tolerance_edges(amount, expected):
    assert decide_from_cache(inv(amount=amount), FACTS) == expected


def test_known_wrong_vendor_is_rejected_without_the_erp():
    """Case 3, invoice B."""
    assert decide_from_cache(inv(vendor="V-1007"), {"vendor_id": "V-1003"}) == (
        Status.UNMATCHED, "vendor_mismatch")


def test_known_missing_po_is_rejected():
    assert decide_from_cache(inv(), {"missing": "1"}) == (Status.UNMATCHED, "po_not_found")


def test_no_facts_means_ask_the_erp():
    assert decide_from_cache(inv(), None) is None
    assert decide_from_cache(inv(), {}) is None


def test_cache_never_approves():
    """The invariant: across every combination, the cache says UNMATCHED or nothing."""
    vendors = ["V-1003", "V-1007"]
    amounts = ["1.00", "4158.00", "4200.00", "4242.00", "4242.01", "99999.99"]
    fact_sets = [None, {}, {"missing": "1"}, {"vendor_id": "V-1003"},
                 {"vendor_id": "V-1003", "po_amount": "4200.0"}, FACTS,
                 {"po_amount": "not-a-number"}]
    for vendor, amount, facts in itertools.product(vendors, amounts, fact_sets):
        decision = decide_from_cache(inv(vendor=vendor, amount=amount), facts)
        assert decision is None or decision[0] == Status.UNMATCHED


def test_currency_mismatch_overrides_an_erp_match():
    """Case 5: the ERP says matched, but the PO is USD and the invoice EUR."""
    assert check_currency(inv(currency="EUR"), {"matched": True, "currency": "USD"}) == (
        Status.UNMATCHED, "currency_mismatch")


def test_same_currency_is_matched():
    assert check_currency(inv(), {"matched": True, "currency": "USD"}) == (
        Status.MATCHED, None)
