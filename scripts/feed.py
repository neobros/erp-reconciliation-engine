#!/usr/bin/env python3
"""POST an NDJSON file to the ingest API in batches. Stdlib only.

    python scripts/feed.py data/invoices.ndjson [--url http://localhost:8000] [--batch 500]

On 429 it waits Retry-After and re-sends the same batch (the API is idempotent, so a
re-send after a timeout is also safe). Prints totals at the end.
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter

KEYS = ("accepted", "duplicate", "invalid", "already_seen")


def post_batch(url: str, batch: list, timeout: float) -> dict:
    body = json.dumps(batch).encode()
    while True:
        req = urllib.request.Request(
            f"{url}/invoices", data=body, method="POST",
            headers={"Content-Type": "application/json", "X-Request-ID": str(uuid.uuid4())},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                wait = float(exc.headers.get("Retry-After", "5"))
                print(f"  429 backlog full, waiting {wait:.0f}s", file=sys.stderr)
                time.sleep(wait)
                continue
            raise SystemExit(f"HTTP {exc.code}: {exc.read().decode()[:500]}") from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            print(f"  network error ({exc}), retrying in 2s", file=sys.stderr)
            time.sleep(2)


def batches(path: str, size: int):
    batch = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                batch.append(json.loads(line))
                if len(batch) == size:
                    yield batch
                    batch = []
    if batch:
        yield batch


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("file")
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--timeout", type=float, default=60)
    args = ap.parse_args()

    totals: Counter = Counter()
    rejected = 0
    started = time.monotonic()
    for n, batch in enumerate(batches(args.file, args.batch), start=1):
        result = post_batch(args.url.rstrip("/"), batch, args.timeout)
        totals.update({k: result.get(k, 0) for k in KEYS})
        rejected += len(result.get("rejected", []))
        print(f"batch {n}: " + " ".join(f"{k}={result.get(k, 0)}" for k in KEYS))

    elapsed = time.monotonic() - started
    sent = sum(totals.values()) + rejected
    print(f"\nTOTAL sent={sent} " + " ".join(f"{k}={totals[k]}" for k in KEYS)
          + f" rejected={rejected} in {elapsed:.1f}s ({sent / max(elapsed, 0.001):.0f}/s)")


if __name__ == "__main__":
    main()
