"""Thin httpx wrapper for the ERP. It makes no decisions and catches no exceptions;
classify.py decides what a response or an exception means (PLANNING.md 3.1)."""
import time
from decimal import Decimal
from functools import lru_cache

import httpx
from django.conf import settings

from invoices.observability import ERP_REQUEST_SECONDS


@lru_cache(maxsize=1)
def _client() -> httpx.Client:
    # One client per process: httpx.Client is thread-safe and pools connections.
    read = settings.ERP_READ_TIMEOUT
    return httpx.Client(
        base_url=settings.ERP_BASE_URL,
        timeout=httpx.Timeout(connect=settings.ERP_CONNECT_TIMEOUT, read=read,
                              write=read, pool=read),
    )


class ErpClient:
    def match(self, po_number: str, vendor_id: str, amount: Decimal, currency: str,
              trace_id: str) -> httpx.Response:
        started = time.monotonic()
        try:
            return _client().get(
                "/erp/purchase-orders/match",
                params={"po_number": po_number, "vendor_id": vendor_id,
                        "amount": f"{amount:.2f}", "currency": currency},
                headers={"X-Request-ID": trace_id},
            )
        finally:
            ERP_REQUEST_SECONDS.observe(time.monotonic() - started)
