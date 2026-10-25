#!/usr/bin/env python3
"""Bonus: replay a 4,000 invoices/minute burst against the ingest API. Stdlib only.

    python scripts/burst.py data/invoices.ndjson --rate 4000 --minutes 1

Sends batches of 100 on a fixed schedule, re-labels invoice_id / invoice_number with a run
prefix so every run is new work, and reports ingest latency. Watch the effect with:
    curl -s localhost:8000/reports/summary      (lag.oldest_unfinished_age_seconds, counts)
"""
import argparse
import itertools
import json
import statistics
import threading
import time
import urllib.error
import urllib.request

BATCH = 100


def load(path: str, prefix: str):
    with open(path, encoding="utf-8") as fh:
        rows = [json.loads(line) for line in fh if line.strip()]
    for n, row in enumerate(itertools.cycle(rows)):
        row = dict(row)
        row["invoice_id"] = f"{prefix}{n}-{row.get('invoice_id', '')}"[:64]
        row["invoice_number"] = f"{prefix}{n}-{row.get('invoice_number', '')}"[:64]
        yield row


def post(url: str, batch: list, latencies: list, statuses: list) -> None:
    req = urllib.request.Request(f"{url}/invoices", data=json.dumps(batch).encode(),
                                 method="POST", headers={"Content-Type": "application/json"})
    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            statuses.append(resp.status)
    except urllib.error.HTTPError as exc:
        statuses.append(exc.code)
    latencies.append(time.monotonic() - started)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("file")
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--rate", type=int, default=4000, help="invoices per minute")
    ap.add_argument("--minutes", type=float, default=1)
    args = ap.parse_args()

    total = int(args.rate * args.minutes)
    interval = 60 / (args.rate / BATCH)  # seconds between batches
    records = load(args.file, prefix=f"BURST{int(time.time())}-")
    latencies, statuses, threads = [], [], []
    start = time.monotonic()
    for i in range(0, total, BATCH):
        batch = [next(records) for _ in range(min(BATCH, total - i))]
        t = threading.Thread(target=post, args=(args.url, batch, latencies, statuses))
        t.start()
        threads.append(t)
        time.sleep(max(0.0, start + (i // BATCH + 1) * interval - time.monotonic()))
    for t in threads:
        t.join()

    elapsed = time.monotonic() - start
    lat = sorted(latencies)
    print(f"sent {total} invoices in {elapsed:.1f}s ({total / elapsed * 60:.0f}/min)")
    print(f"HTTP statuses: { {s: statuses.count(s) for s in set(statuses)} }")
    print(f"batch latency p50={statistics.median(lat) * 1000:.0f}ms "
          f"p95={lat[int(len(lat) * 0.95) - 1] * 1000:.0f}ms max={lat[-1] * 1000:.0f}ms")


if __name__ == "__main__":
    main()
