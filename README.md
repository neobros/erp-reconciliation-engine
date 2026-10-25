# ERP Reconciliation Engine

Invoice ingestion and reconciliation pipeline: invoices arrive (often duplicated, sometimes
malformed), are checked against a slow, rate-limited, flaky ERP, and each one reaches
**exactly one outcome**, without ever being processed into two results.

- **Why it is built this way:** [DESIGN.md](DESIGN.md) (architecture, scaling, failure modes,
  caching, reporting, on-call, what was not built).
- **Tradeoffs** (the 3 biggest decisions): [DESIGN.md section 8](DESIGN.md#8-tradeoffs).
- **What was built, step by step:** [PLANNING.md](PLANNING.md).

Stack: Python 3.12, Django 5.2 + DRF, PostgreSQL 16, Redis 7, Celery 5 (threads pool +
beat), httpx, structlog, prometheus_client, pytest.

---

## Run it (one command)

Requires Docker. Ports 8000, 5432, 6379, 9100 and 9101 must be free.

```bash
mkdir -p data                                              # git-ignored, so not in the clone
python harness/generate_invoices.py --count 3000 --seed 42 --out data/invoices.ndjson
ERP_PO_COUNT=1000 docker compose up -d --build --wait     # ~2 min on a clean machine
```

`ERP_PO_COUNT` must equal `max(100, count // 3)` (harness rule). 1000 is also the default.
On Windows PowerShell: `$env:ERP_PO_COUNT="1000"; docker compose up -d --build --wait`.

## Demo (about 5 minutes)

```bash
python scripts/feed.py data/invoices.ndjson     # 2730 accepted, 68 duplicate, 112 invalid, 90 already_seen
python scripts/feed.py data/invoices.ndjson     # again: all 3000 already_seen (idempotent)

curl -s localhost:8000/reports/summary          # counts, match rate, stuck, lag, dead letters
curl -s localhost:8000/metrics                  # ingest counters + backlog/lag gauges
curl -s localhost:9101/metrics                  # worker: ERP codes, latency, cache, limiter, retries

docker compose logs -f worker                   # one JSON line per step, each with trace_id
docker compose exec api pytest -q               # 69 tests, real Postgres + Redis, ~15 s
docker compose exec api ruff check .

python scripts/burst.py data/invoices.ndjson --rate 4000 --minutes 1   # bonus: burst load
docker compose exec api python src/manage.py requeue_dead_letters --reason max_attempts --since 2h
```

### Optional: live dashboard (Prometheus + Grafana)

```bash
docker compose --profile monitoring up -d
```

Open <http://localhost:3000> (no login): the **"Invoice pipeline: what is it doing right
now?"** dashboard opens as the home page. Prometheus is at <http://localhost:9090>. The
dashboard file is [monitoring/grafana/dashboards/pipeline.json](monitoring/grafana/dashboards/pipeline.json);
its 11 panels are the "first dashboard" of DESIGN.md section 7 (ingest vs completion rate,
oldest-unfinished age, drain time, backlog by status, ERP calls by code against the 8/s line,
latency, limiter wait, breaker, cache decided rate, retries, dead letters).

### Graceful shutdown

`docker compose stop worker` sends SIGTERM: Celery stops taking messages, finishes the tasks
it is running (warm shutdown) and puts unstarted prefetched messages back in Redis. The
worker has a 30 s `stop_grace_period`, longer than the longest task (20 s limiter wait +
2.5 s ERP timeout). Measured mid-run: stop took 1.4 s, 3 in-flight invoices finished, 0 left
`PROCESSING`, and after a restart all 2,730 finished with one result each. If a worker is
killed instead, its rows come back through the 60 s lease.

Endpoints: `POST /invoices` (JSON array, 1–500), `GET /reports/summary?window_hours=1`,
`GET /metrics`, `GET /healthz`, `GET /readyz`.

**Statuses vs the brief's names:**

| Brief | Our status(es) |
|---|---|
| pending | `PENDING`, `QUEUED`, `PROCESSING`, `RETRY_SCHEDULED` (the stage it is waiting in) |
| matched | `MATCHED` (ready for payment) |
| unmatched | `UNMATCHED`, with `reason` (vendor / amount / PO / currency) |
| failed | `INVALID` (failed validation at ingest, never sent to the ERP) |
| dead-lettered | `DEAD_LETTERED`, with `reason` (`max_attempts`, `poison`, `permanent_error`) |
| (duplicate) | `DUPLICATE`: same bill under a new ID, kept for audit, never processed |

**Global rate limit across processes:** `docker compose up -d --scale worker=2` runs two
worker containers; together they still never exceed 8 ERP calls in a second.

### What a run looks like (measured, 3,000 invoices, ERP chaos on)

| | |
|---|---|
| Final statuses | 2,728 UNMATCHED, 2 MATCHED, 112 INVALID, 68 DUPLICATE, 0 left unfinished |
| ERP calls | 1,251 for 2,730 reconciled invoices: the PO cache decided 1,535 (56%) |
| ERP 429s | **0**; never more than 8 calls started in one second |
| 503s / timeouts | all retried with backoff; none reached the dead-letter queue |
| Results | exactly one `reconciliation_result` row per invoice |
| Burst (4,000/min for 1 min) | all `202`, ingest p95 374 ms; backlog waits in Postgres, Redis queue stays ≤ ~1,000 |

**Why almost nothing matches:** the provided generator and mock ERP use different random
streams, so only ~0.08% of generated invoices match a PO (DESIGN.md, assumption A5). The
harness was not modified. The matching rule is tested with fixtures instead
(`tests/test_matching.py`, `tests/test_reconcile_flow.py`).

---

## How it works (short)

```
POST /invoices ─► Postgres (row = durable queue + state machine) ─► dispatcher (beat, 2 s)
                                                                      │ ids only, ≤ 1,000
worker (10 threads) ◄─ Redis list ◄──────────────────────────────────┘
  claim (lease) → PO cache (can say no, never yes) → global limiter (8/s per Redis-clock
  second) → ERP → classify → finish / reschedule / dead-letter, guarded by lease_token
```

- **Exactly-once outcome:** `UNIQUE(invoice_id)` + our own content fingerprint (partial
  unique index) at ingest; atomic claim; every final write `WHERE lease_token = <mine>`;
  `UNIQUE` on the result row.
- **Retries are rows** (`RETRY_SCHEDULED` + `next_attempt_at`), never Celery countdowns.
- **429 never burns an attempt**; it sets a shared cooldown so every worker backs off.
  20 transient failures in a row pause everyone for 30 s (breaker-lite).
- **Reporting** reads counter tables updated in the same transaction as each status change,
  plus partial indexes over unfinished rows, so it stays fast at 12M rows.

## Repository map

| Path | What |
|---|---|
| `src/invoices/services/ingest.py` | validate, fingerprint, idempotent insert |
| `src/invoices/services/claims.py` | every lease-guarded status change (raw SQL) |
| `src/invoices/services/reconcile.py` | the worker flow for one invoice |
| `src/invoices/services/classify.py` | pure: ERP response/exception → outcome; backoff |
| `src/invoices/services/rate_limiter.py` | Lua limiter, cooldown, breaker-lite |
| `src/invoices/services/po_cache.py`, `matching.py` | PO facts cache; pure match decisions |
| `src/invoices/services/dispatcher.py` | Postgres → Redis, with high-water mark |
| `src/invoices/services/counters.py`, `reporting.py` | counter tables; `/reports/summary` |
| `src/invoices/observability.py` | metrics, JSON logs |
| `tests/` | the tests that matter (below) |
| `scripts/feed.py`, `scripts/burst.py` | ingest feeder; burst load (stdlib only) |
| `monitoring/` | Prometheus scrape config, Grafana provisioning + dashboard JSON |

## Tests (what they prove)

| File | Proves |
|---|---|
| `test_idempotency.py` | 20 concurrent POSTs of one invoice → 1 row; near-duplicates (also inside one batch) → `DUPLICATE`; malformed → `INVALID` |
| `test_claims.py` | a stalled worker whose lease was taken over cannot write; 2 racing claims → 1 winner; poison rows dead-lettered; requeue |
| `test_classify.py` | every row of the transient / permanent / rate-limited table |
| `test_rate_limiter.py` | 30 threads never get > 8 slots in a Redis-clock second; cooldown blocks all; breaker-lite |
| `test_matching.py` | 1% tolerance edges; currency check; the cache never approves (all combinations) |
| `test_reconcile_flow.py` | respx ERP: 503×6 → dead letter; 429×3 → match with `attempts == 0`; timeout retry; cache saves an ERP call; PO single-flight |

---

## Built vs deferred

**Built:** everything in PLANNING.md Steps 1–7, plus three of the four bonus items:
Prometheus `/metrics` with a Grafana dashboard, graceful shutdown/draining, and the burst
load script.

**Deliberately not built** (reasons and production designs in DESIGN.md section 9):

- Auth and multi-tenancy.
- PO budget tracking: several valid invoices can together exceed one PO. Biggest
  real-world gap.
- Payment batching / outbox: `MATCHED` is the finish line.
- Per-source fairness and priority queues (the fourth bonus item; design only).
- A full circuit breaker (half-open state): breaker-lite covers the outage case.
- Fuzzy duplicate detection, counter-drift repair job.
- Alerting rules: the five page-worthy alerts are specified in DESIGN.md section 7 but not
  written as Prometheus alert rules.

## Known issues

- **Retries wait behind the queue.** A retry is *due* after its backoff, but it re-enters the
  dispatcher's FIFO behind up to ~1,000 queued ids, so during a backlog it can take ~2 min.
  Backoff is a minimum, not an exact time.
- **One worker on native Windows reaches ~4.5 ERP calls/s** (Celery's event loop falls back
  to a slower polling loop off Linux). In Docker one worker reaches 8/s. Run it in Docker.
- **`estimated_drain_seconds` reads low for the first 5 minutes** of a run: it averages
  completions over a fixed 5-minute window.
- **`outcome_hourly` keeps a dead letter's count after it is requeued**, so a requeued
  invoice counts twice in the hourly totals (once as dead-lettered, once as its new outcome).
  `status_count` stays exact.
- The image is built three times by compose (api, worker, beat share the same Dockerfile);
  later builds are fully cached.

## Time spent

**Total: 5 hours** (inside the 6-hour limit).

| Area | Time |
|---|---|
| Reading the harness, design, planning | 0:30 |
| Ingest + idempotency (Step 2) | 0:40 |
| Worker, limiter, retries, dead letters (Step 3, incl. throughput tuning) | 1:15 |
| Cache, reporting, observability (Steps 4–5) | 0:55 |
| Tests (Step 6) | 0:40 |
| DESIGN.md / README (Step 7) | 0:30 |
| Bonus: graceful shutdown, Grafana dashboard | 0:30 |
| **Total** | **5:00** |
