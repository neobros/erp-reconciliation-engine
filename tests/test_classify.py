"""Test 5: every row of the classification table (PLANNING.md 3.2). Pure, no I/O."""
import httpx
import pytest

from invoices.services.classify import Kind, backoff_seconds, classify

REQ = httpx.Request("GET", "http://erp/erp/purchase-orders/match")


def resp(status, json=None, text=None, headers=None):
    if json is not None:
        return httpx.Response(status, json=json, headers=headers, request=REQ)
    return httpx.Response(status, text=text or "", headers=headers, request=REQ)


@pytest.mark.parametrize("response, exc, kind, reason", [
    (None, httpx.ReadTimeout("t", request=REQ), Kind.TRANSIENT, "timeout"),
    (None, httpx.ConnectTimeout("t", request=REQ), Kind.TRANSIENT, "timeout"),
    (None, httpx.ConnectError("c", request=REQ), Kind.TRANSIENT, "connection_error"),
    (None, httpx.RemoteProtocolError("p", request=REQ), Kind.TRANSIENT, "connection_error"),
    (None, httpx.ReadError("r", request=REQ), Kind.TRANSIENT, "connection_error"),
    (None, ValueError("our bug"), Kind.TRANSIENT, "internal_error"),
    (resp(429, json={"error": "rate_limited"}), None, Kind.RATE_LIMITED, "http_429"),
    (resp(500), None, Kind.TRANSIENT, "http_500"),
    (resp(502), None, Kind.TRANSIENT, "http_502"),
    (resp(503, json={"error": "upstream_unavailable"}), None, Kind.TRANSIENT, "http_503"),
    (resp(504, json={"error": "gateway_timeout"}), None, Kind.TRANSIENT, "http_504"),
    (resp(400), None, Kind.PERMANENT, "http_400"),
    (resp(404), None, Kind.PERMANENT, "http_404"),
    (resp(422), None, Kind.PERMANENT, "http_422"),
    (resp(200, text="<html>oops</html>"), None, Kind.TRANSIENT, "bad_body"),
    (resp(200, json={"po_number": "PO-1"}), None, Kind.TRANSIENT, "bad_body"),
    (resp(200, json=[1, 2]), None, Kind.TRANSIENT, "bad_body"),
    (resp(200, json={"matched": True, "currency": "USD"}), None, Kind.MATCHED, None),
    (resp(200, json={"matched": False, "reason": "vendor_mismatch"}), None, Kind.UNMATCHED,
     "vendor_mismatch"),
    (resp(200, json={"matched": False}), None, Kind.UNMATCHED, "erp_unmatched"),
])
def test_classify(response, exc, kind, reason):
    outcome = classify(response, exc)
    assert (outcome.kind, outcome.reason) == (kind, reason)


@pytest.mark.parametrize("header, expected", [("1", 1.0), ("2.5", 2.5), (None, 1.0),
                                               ("soon", 1.0), ("0", 1.0)])
def test_retry_after_is_read_from_429(header, expected):
    headers = {"Retry-After": header} if header else None
    assert classify(resp(429, headers=headers), None).retry_after == expected


def test_matched_keeps_the_body_for_the_currency_check():
    body = {"matched": True, "currency": "USD", "po_amount": 4200.0}
    assert classify(resp(200, json=body), None).body == body


@pytest.mark.parametrize("attempt, cap", [(1, 4), (2, 8), (5, 64), (20, 300)])
def test_backoff_is_full_jitter_and_capped(attempt, cap):
    waits = [backoff_seconds(attempt) for _ in range(200)]
    assert all(0 <= w <= cap for w in waits)
    assert max(waits) > cap / 2  # spread over the range, not a constant
