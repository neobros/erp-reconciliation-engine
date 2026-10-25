# PLANNING.md — Implementation Spec

This file tells you exactly what to build: files, tables, SQL, function signatures and the
check that proves each step works. Build the steps in order. **At 5:15 into the 6 hours,
stop coding and write the docs, whatever state the code is in.**

The *why* behind each decision is in `DESIGN.md`. This file is the *what* and the *how*.

---

## 0. Read first: the harness has surprises

We replayed the 100k sample (seed 42) against the ERP's matching logic:

| Outcome | Count |
|---|---|
| vendor_mismatch | 78,524 |
| amount_out_of_tolerance | 8,045 |
| po_not_found | 4,749 |
| matched | 75 |
| exact duplicate (same `invoice_id`) | 2,885 |
| near-duplicate (same content, new `invoice_id`) | 1,920 |
| malformed | ~3,800 |

1. **Almost nothing matches** (0.08%, not the ~81% the README suggests). The generator and
   the ERP use different random streams. **Do not modify `harness/`.** Test the matching
   rule with our own fixtures, not the dataset.
2. **The ERP ignores currency.** We check currency ourselves.
3. **Caching by exact request gets 0 hits.** All 91k lookups are unique, but they touch only
   35,614 POs. We cache per PO (step 4).
4. **The ERP's limit resets each calendar second** (`int(time.time())`). A normal token
   bucket still triggers 429s. Step 3 explains the fix.
5. **An ERP "timeout" is a 6 s hang that ends in a 504.** Client read timeout = 2.5 s.
6. **A 100k run takes ~3 hours.** Demo with `--count 3000` and `ERP_PO_COUNT=1000`.
   Rule: `ERP_PO_COUNT = max(100, count // 3)`. Always change both together.

---

## 0.1 The domain in two minutes

**Reconciliation = checking an incoming bill against a trusted source before paying it.**

1. A company approves spending in advance: **purchase order (PO)** `PO-001234`, "up to
   $4,200.00 with vendor `V-1003` (Cornerstone Electric)". It is stored in the **ERP**, the
   company's main system and source of truth.
2. The vendor does the work and sends an **invoice**: "you owe me $4,210.55 for PO-001234".
3. We **reconcile** it: ask the ERP whether this invoice matches a real PO.
4. Matched → **ready for payment** (`MATCHED`). Not matched → flagged (`UNMATCHED`).

| Term | Meaning | In our code |
|---|---|---|
| Invoice | A bill from a supplier | one JSON record, one `Invoice` row |
| Vendor | The supplier | `vendor_id` (`V-1003`) |
| Purchase order (PO) | Spending approved in advance | `po_number` (`PO-001234`) |
| ERP / system of record | The trusted database of POs | the mock ERP API |
| Tolerance | How far the amounts may differ | 1% of the PO amount |
| Ready for payment | Passed every check | status `MATCHED` + a `ReconciliationResult(matched=True)` |

**Match rule** (all must pass):

```
1. PO exists?                     no  → UNMATCHED (po_not_found)
2. invoice vendor == PO vendor?   no  → UNMATCHED (vendor_mismatch)
3. |amount − PO amount| ≤ 1%?     no  → UNMATCHED (amount_out_of_tolerance)
4. currency == PO currency?       no  → UNMATCHED (currency_mismatch)   ← our check; ERP skips it
   all yes                            → MATCHED
```

**Why so much care about duplicates:** processing an invoice twice can mean **paying a
supplier twice**. That is a serious incident, and it is why idempotency matters most here.

---

## 1. Stack

| Piece | Choice |
|---|---|
| Python | 3.12 |
| Web | Django 5.x + djangorestframework, served by gunicorn |
| DB | PostgreSQL 16, driver `psycopg[binary]` 3.x |
| Worker | Celery 5.x, Redis broker, `--pool=threads --concurrency=10` |
| Scheduler | Celery beat (runs `dispatch` every 2 s) |
| Redis | Redis 7 (`redis` Python client) |
| HTTP client | `httpx` |
| Logs | `structlog` (JSON output) |
| Metrics | `prometheus_client` |
| Tests | `pytest`, `pytest-django`, `respx` |
| Lint | `ruff` |

**Why threads?** Each ERP call waits 0.2–0.9 s. To reach ~8 calls/s, ~10 calls must be in
flight at once. A process pool of 2–4 cannot do that.

---

## 2. Repository layout

```
.
├── docker-compose.yml
├── Dockerfile                    # one image for api, worker, beat
├── docker/mock-erp.Dockerfile    # wraps harness/ without changing it
├── requirements.txt
├── README.md  DESIGN.md  PLANNING.md
├── harness/                      # given; DO NOT EDIT
├── scripts/
│   ├── feed.py                   # POSTs NDJSON to the API in batches of 500
│   └── burst.py                  # bonus: 4,000/min load test
├── src/
│   ├── manage.py
│   ├── config/
│   │   ├── settings.py
│   │   ├── celery.py
│   │   └── urls.py
│   └── invoices/
│       ├── models.py
│       ├── migrations/
│       ├── api/
│       │   ├── serializers.py
│       │   └── views.py          # ingest, reports, healthz, readyz, metrics
│       ├── services/
│       │   ├── ingest.py         # validate, fingerprint, insert
│       │   ├── claims.py         # claim, finish, reschedule, dead_letter (SQL)
│       │   ├── erp_client.py     # httpx wrapper, no decisions
│       │   ├── classify.py       # pure: response/exception -> Outcome
│       │   ├── rate_limiter.py   # Lua script + acquire_erp_slot
│       │   ├── po_cache.py       # learn + lookup PO facts
│       │   ├── matching.py       # pure: currency + cache decisions
│       │   ├── counters.py       # status_counts / outcome_hourly updates
│       │   └── reporting.py
│       ├── tasks.py              # thin: dispatch(), reconcile(id)
│       ├── observability.py      # structlog config, metric objects
│       └── management/commands/requeue_dead_letters.py
└── tests/
```

**Rule:** `views.py` and `tasks.py` stay thin. Any function that makes a decision lives in
`services/` and has a test.

---

## 3. Configuration (env vars, read in `settings.py`)

| Name | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgres://app:app@postgres:5432/app` | |
| `REDIS_URL` | `redis://redis:6379/0` | broker + limiter + cache |
| `ERP_BASE_URL` | `http://mock-erp:9100` | |
| `ERP_RPS` | `8` | our global limit (ERP allows 10) |
| `ERP_CONNECT_TIMEOUT` | `1.0` | seconds |
| `ERP_READ_TIMEOUT` | `2.5` | seconds |
| `MAX_ATTEMPTS` | `6` | temporary failures before dead-letter |
| `MAX_CRASHES` | `3` | expired leases before dead-letter (poison) |
| `MAX_RATE_LIMITED` | `20` | 429s for one invoice before one counts as an attempt |
| `LEASE_SECONDS` | `60` | worker lease on a PROCESSING row |
| `QUEUE_LEASE_SECONDS` | `300` | how long a QUEUED row waits before re-dispatch |
| `LIMITER_MAX_WAIT` | `20` | seconds a worker waits for a slot before giving up |
| `DISPATCH_BATCH` | `200` | rows per dispatcher run |
| `QUEUE_HIGH_WATER` | `1000` | stop dispatching above this Redis queue length |
| `PENDING_HARD_LIMIT` | `200000` | ingest returns 429 above this backlog |
| `PO_CACHE_TTL` | `900` | seconds, positive PO facts |
| `PO_NEGATIVE_TTL` | `120` | seconds, `po_not_found` |
| `STUCK_AFTER_SECONDS` | `600` | non-final row older than this = stuck |
| `ALLOWED_CURRENCIES` | `USD,EUR,GBP` | |

---

## 4. Data model (`invoices/models.py`)

### 4.1 `Invoice`

| Field | Type | Notes |
|---|---|---|
| `id` | BigAutoField PK | |
| `invoice_id` | CharField(64), **unique** | Guard 1 |
| `fingerprint` | CharField(64), null for INVALID | Guard 2 |
| `duplicate_of` | FK self, null | set when status = DUPLICATE |
| `invoice_number`, `vendor_id`, `po_number`, `currency`, `source`, `cost_center` | CharField, blank allowed | |
| `amount` | DecimalField(14,2), null | **never float** |
| `raw` | JSONField | original payload |
| `status` | CharField(20) + CheckConstraint on the values below | |
| `reason` | CharField(64), null | e.g. `vendor_mismatch`, `missing_amount` |
| `attempts` | int, default 0 | temporary ERP failures only |
| `crash_count` | int, default 0 | expired PROCESSING leases |
| `rate_limited_count` | int, default 0 | 429s seen |
| `next_attempt_at` | DateTimeField, null | |
| `lease_token` | UUIDField, null | Guard 3 |
| `lease_until` | DateTimeField, null | |
| `last_error` | TextField, blank | |
| `trace_id` | CharField(64) | |
| `created_at`, `updated_at` | DateTimeField | `updated_at` set on every status change |
| `finished_at`, `dead_lettered_at` | DateTimeField, null | |

**Statuses:** `PENDING, QUEUED, PROCESSING, RETRY_SCHEDULED, MATCHED, UNMATCHED, INVALID,
DUPLICATE, DEAD_LETTERED`. Final = `MATCHED, UNMATCHED, INVALID, DUPLICATE, DEAD_LETTERED`.

**Indexes / constraints** (write the partial ones with `models.Index(condition=Q(...))` and
`UniqueConstraint(condition=Q(...))`):

```sql
UNIQUE (invoice_id);
UNIQUE (fingerprint) WHERE duplicate_of_id IS NULL AND status <> 'INVALID';   -- Guard 2
INDEX (status, next_attempt_at) WHERE status IN ('PENDING','RETRY_SCHEDULED'); -- dispatcher
INDEX (lease_until) WHERE status IN ('QUEUED','PROCESSING');                   -- expired leases
INDEX (updated_at) WHERE status IN ('PENDING','QUEUED','PROCESSING','RETRY_SCHEDULED'); -- stuck
INDEX (created_at) WHERE status IN ('PENDING','QUEUED','PROCESSING','RETRY_SCHEDULED'); -- lag age
INDEX (status, dead_lettered_at) WHERE status = 'DEAD_LETTERED';
```

### 4.2 `ReconciliationResult`

`invoice` OneToOne (**unique**), `matched` bool, `reason` str null, `po_amount` Decimal null,
`po_currency` str null, `decided_by` (`erp` | `cache`), `erp_response` JSON null,
`created_at`. Rows with `matched=True` are the "ready for payment" list.

### 4.3 `InvoiceAttempt`

`invoice` FK, `attempt_no` int, `started_at`, `duration_ms` int, `http_status` int null,
`error` str null (exception class name), `error_class` (`transient` | `permanent` |
`rate_limited` | `ok`), `trace_id`.

### 4.4 Counter tables

- `StatusCount`: `status` PK, `count` bigint.
- `OutcomeHourly`: `hour` (DateTime truncated to hour) + `outcome` (unique together), `count`.

---

## 5. State machine

```mermaid
stateDiagram-v2
    [*] --> INVALID: fails validation
    [*] --> DUPLICATE: fingerprint already seen
    [*] --> PENDING: accepted
    PENDING --> QUEUED: dispatcher
    RETRY_SCHEDULED --> QUEUED: dispatcher, when due
    QUEUED --> QUEUED: lease expired, re-dispatch
    QUEUED --> PROCESSING: worker claim
    PROCESSING --> QUEUED: lease expired (crash_count+1)
    PROCESSING --> MATCHED
    PROCESSING --> UNMATCHED
    PROCESSING --> RETRY_SCHEDULED: transient / 429 / limiter busy
    PROCESSING --> DEAD_LETTERED: permanent, or attempts >= MAX
    QUEUED --> DEAD_LETTERED: crash_count >= MAX_CRASHES
    DEAD_LETTERED --> PENDING: requeue command
```

**Every status change** goes through one function, `claims.transition(...)`, which also
updates the counter tables in the same transaction (`services/counters.py`, Step 5).

---

## 5.1 Practical cases (walk-throughs)

Each case follows one invoice through the system. **Use them as test fixtures**: the last
line of each case says which test covers it. All cases use this PO, returned by a mocked ERP
(respx) in tests:

```
PO-001234 → vendor V-1003, po_amount 4200.00, currency USD, status OPEN
1% tolerance = 42.00 → accepted invoice amounts: 4158.00 … 4242.00
```

Base invoice (each case changes only what it mentions):

```json
{"invoice_id": "INV-000000123", "invoice_number": "1003-48217", "vendor_id": "V-1003",
 "vendor_name": "Cornerstone Electric", "po_number": "PO-001234", "amount": 4210.55,
 "currency": "USD", "cost_center": "CC-014", "source": "property-erp",
 "issued_at": "2025-09-14T08:22:00Z", "submitted_at": "2025-09-20T08:22:00Z"}
```

### Case 1 — Happy path: the invoice matches

1. `POST /invoices` → valid → fingerprint computed → row inserted `PENDING` → `202 {"accepted": 1}`.
2. Dispatcher (≤ 2 s later) → `QUEUED`, ID pushed to Redis.
3. Worker claims → `PROCESSING`. Cache has nothing for PO-001234 → takes the PO lock.
4. Limiter gives a slot → ERP answers `200 {"matched": true, "po_amount": 4200.00, "currency": "USD", ...}`.
5. `classify` → MATCHED. `check_currency`: USD == USD → `MATCHED`.
6. `finish` (lease-guarded) → status `MATCHED`, one `ReconciliationResult(matched=True, decided_by="erp")`.
   PO facts saved in `po:PO-001234`.

**Final:** `MATCHED` (ready for payment). `|4210.55 − 4200.00| = 10.55 ≤ 42.00`. → tests 7, 8

### Case 2 — Amount too far off

Invoice `amount: 6300.00`. ERP → `{"matched": false, "reason": "amount_out_of_tolerance", "po_amount": 4200.00}`.
**Final:** `UNMATCHED (amount_out_of_tolerance)`. Cache learns `po_amount=4200.00` and
`vendor_id=V-1003`. Edge cases to test: `4242.00` → MATCHED (exactly 1%); `4242.01` → UNMATCHED. → test 7

### Case 3 — Wrong vendor, then the cache saves an ERP call

1. Invoice A: `vendor_id: "V-1007"`, `invoice_id: INV-A`. ERP → `{"matched": false, "reason": "vendor_mismatch", "expected_vendor": "V-1003"}`.
   **Final:** `UNMATCHED (vendor_mismatch)`. Cache stores `po:PO-001234 → vendor_id=V-1003`.
2. Five minutes later, invoice B: `vendor_id: "V-1007"`, same PO, different amount and number.
   `decide_from_cache` sees V-1007 ≠ V-1003 → **`UNMATCHED (vendor_mismatch)`, no ERP call**,
   `decided_by="cache"`.
3. Invoice C: `vendor_id: "V-1003"`, amount 4199.00. Cache facts are consistent → cache
   **cannot approve** → normal ERP call → `MATCHED`.

→ test 7 (`decide_from_cache` never returns MATCHED)

### Case 4 — PO does not exist

Invoice `po_number: "PO-9123456"`. ERP → `{"matched": false, "reason": "po_not_found"}`.
**Final:** `UNMATCHED (po_not_found)`. Cache stores "missing" for **2 minutes only**, because
a new PO might be created soon.

### Case 5 — Currency differs (the ERP would wrongly approve)

Invoice `currency: "EUR"`. ERP → `{"matched": true, "currency": "USD", ...}` (the ERP ignores
currency). `check_currency`: EUR ≠ USD → **`UNMATCHED (currency_mismatch)`**.
Without our check we would pay €4,210.55 against a $4,200.00 approval. → test 7

### Case 6 — Malformed invoice

Invoice without `amount` (generator marks it `"_defect": "no_amount"`).
`validate` → reason `missing_amount` → inserted directly as **`INVALID`**, `fingerprint=NULL`.
It is never dispatched and never reaches the ERP. Response: `202 {"invalid": 1}`.
Same for blank vendor (`blank_vendor`), negative amount (`non_positive_amount`), currency
`XYZ` (`bad_currency`), blank PO (`missing_po`).

### Case 7 — Same invoice sent twice, at the same moment

Upstream sends `INV-000000123` twice, concurrently (at-least-once delivery).
1. Both requests run `INSERT … ON CONFLICT (invoice_id) DO NOTHING RETURNING id`.
2. Postgres lets one win. The other gets no row back → counted as `already_seen`.
3. Only one row exists, so only one dispatch, one ERP call, one result.

**Final:** one row, one outcome. → test 1 (20 threads)

### Case 8 — Same bill, new ID (near-duplicate, the "pay twice" risk)

Invoice `INV-000000999-N`, every other field identical to Case 1's invoice.
1. `invoice_id` is new, so Guard 1 lets it in.
2. Fingerprint `sha256("V-1003|PO-001234|4210.55|USD|1003-48217")` already exists →
   the partial unique index rejects it.
3. Savepoint rolled back → re-inserted as **`DUPLICATE`**, `duplicate_of = INV-000000123`.

**Final:** `DUPLICATE`, never sent to the ERP, never paid. Kept for audit. → test 2

### Case 9 — ERP has a hiccup (503), then works

1. Attempt: ERP → `503`. `classify` → TRANSIENT `http_503`. `attempts` 0 → 1.
   `next_attempt_at = now + random(0..4 s)` → `RETRY_SCHEDULED`. Celery task ends normally.
2. Dispatcher picks it up when due → `QUEUED` → claimed again.
3. ERP → `200 matched` → `MATCHED`.

`InvoiceAttempt` has 2 rows (503, then 200). → test 8

### Case 10 — Rate limited (429)

ERP → `429`, `Retry-After: 1`.
1. `start_cooldown(1)` sets `erp:cooldown` → **every** worker waits ~1 s at the limiter.
2. Invoice → `RETRY_SCHEDULED` in 1 s. `rate_limited_count` +1, **`attempts` unchanged**:
   the ERP never looked at our invoice, so it is not our invoice's failure.
3. Next try → `MATCHED`, with `attempts == 0`. → test 8

### Case 11 — ERP keeps failing → dead letter → requeue

ERP returns 503 on every call.
1. Attempts 1–5: backoff waits up to 4, 8, 16, 32, 64 s.
2. Attempt 6 fails → `attempts + 1 >= MAX_ATTEMPTS` → **`DEAD_LETTERED`**,
   reason `max_attempts`, `last_error = http_503`, all six tries kept in `InvoiceAttempt`.
3. After 20 failures in a row across all workers, breaker-lite pauses everyone for 30 s,
   so an outage does not burn through every invoice's attempts.
4. ERP is fixed → operator runs
   `python manage.py requeue_dead_letters --reason max_attempts --since 2h` → back to
   `PENDING`, `attempts=0`, history kept → processed normally.

→ test 8

### Case 12 — Worker crashes mid-invoice

1. Worker 1 claims → `PROCESSING`, `lease_until = now + 60 s`, `lease_token = T1`. Then the container is killed.
2. 60 s later the dispatcher finds `PROCESSING` with an expired lease → `QUEUED`, `crash_count = 1`.
3. Worker 2 claims (`lease_token = T2`) → ERP → `finish` succeeds → `MATCHED`.
4. *If worker 1 was only frozen* and wakes up, its `finish … WHERE lease_token = T1` updates
   **0 rows** → logs `invoice.lease_lost` → its result is thrown away.

**Final:** exactly one outcome. If the same invoice crashes a worker 3 times →
`DEAD_LETTERED (poison)`. → test 3

### Case 13 — Hot PO: 50 invoices for one unknown PO arrive together

1. The first worker takes `po:lock:PO-005555` and calls the ERP.
2. The other 49 cannot take the lock and the PO is unknown → `RETRY_SCHEDULED` in 2 s
   (no attempt counted).
3. When they come back, the cache knows the PO → mismatches are decided locally; consistent
   ones still call the ERP (the cache never approves).

### Case 14 — Burst: 60,000 invoices in 15 minutes

1. Ingest: 67 inserts/s, every request answered `202` in milliseconds.
2. The Redis queue stays at ≤ 1,000 IDs; the rest wait in Postgres as `PENDING`.
3. Workers process ~7.5/s; at the end of the burst ~53,000 are waiting.
4. `/reports/summary` shows `lag.oldest_unfinished_age_seconds` rising and
   `estimated_drain_seconds` ≈ 8,000 (~2.2 h).
5. Nothing is lost; the backlog drains on its own. Above 200,000 waiting, ingest would return `429`.

→ `scripts/burst.py` (bonus)

### Quick reference: case → status

| Case | Final status | Reason | ERP called? |
|---|---|---|---|
| 1 Happy path | MATCHED | – | yes |
| 2 Amount off | UNMATCHED | amount_out_of_tolerance | yes |
| 3 Wrong vendor (2nd time) | UNMATCHED | vendor_mismatch | **no (cache)** |
| 4 Unknown PO | UNMATCHED | po_not_found | yes |
| 5 Currency differs | UNMATCHED | currency_mismatch | yes |
| 6 Malformed | INVALID | missing_amount etc. | no |
| 7 Exact duplicate | (one row only) | already_seen | once |
| 8 Near-duplicate | DUPLICATE | – | no |
| 9 503 then OK | MATCHED | – | twice |
| 10 429 then OK | MATCHED | – | twice, attempts = 0 |
| 11 Always 503 | DEAD_LETTERED | max_attempts | 6 times |
| 12 Worker crash | MATCHED (once) | – | once or twice |
| 13 Hot PO | depends | – | once per PO, then cache |

---

## 6. Step-by-step build

### Step 1 — Skeleton (0:00–0:30)

**docker-compose.yml** services:

| Service | Image / command | Ports |
|---|---|---|
| `postgres` | `postgres:16`, healthcheck `pg_isready` | 5432 |
| `redis` | `redis:7`, healthcheck `redis-cli ping` | 6379 |
| `mock-erp` | build `docker/mock-erp.Dockerfile`; env `ERP_SEED=42 ERP_RATE_LIMIT=10 ERP_PO_COUNT=${ERP_PO_COUNT:-1000} ERP_CHAOS=1` | 9100 |
| `api` | `sh -c "python manage.py migrate && gunicorn config.wsgi -b 0.0.0.0:8000 -w 4"` | 8000 |
| `worker` | `celery -A config worker -Q reconcile --pool=threads --concurrency=10` | 9101 (metrics) |
| `beat` | `celery -A config beat` | |

`docker/mock-erp.Dockerfile`: `python:3.12-slim`, `pip install fastapi uvicorn`,
`COPY harness/ /app`, `CMD uvicorn mock_erp:app --host 0.0.0.0 --port 9100`. Build context `.`.

Endpoints: `GET /healthz` → `200 {"status":"ok"}` (no dependencies).
`GET /readyz` → runs `SELECT 1` and Redis `PING`; returns 200 or 503 with which one failed.

**Done when:** `docker compose up` starts all six; `curl localhost:8000/readyz` → 200.

---

### Step 2 — Ingest (0:30–1:15)

**`POST /invoices`**: body = JSON array of 1–500 invoices. Header `X-Request-ID` optional
(generate a uuid4 if missing); this is the `trace_id`.

**Backpressure first:** if `StatusCount[PENDING] + StatusCount[RETRY_SCHEDULED]` >
`PENDING_HARD_LIMIT`, return `429` with `Retry-After: 30` and do nothing.

**Validation** (`services/ingest.py::validate(raw) -> tuple[clean | None, reason | None]`):

| Rule | Reason code |
|---|---|
| `invoice_id` missing/blank | reject the record in the response (`rejected`), no row (no key to store it under) |
| `vendor_id` blank | `blank_vendor` |
| `po_number` blank | `missing_po` |
| `amount` missing or not a number | `missing_amount` |
| `amount <= 0` | `non_positive_amount` |
| `currency` not in `ALLOWED_CURRENCIES` | `bad_currency` |
| `invoice_number` blank | `missing_invoice_number` |

Parse `amount` as `Decimal(str(value)).quantize(Decimal("0.01"))`. Strip whitespace from strings.
Ignore `_defect` and the client's `content_hash` (keep them in `raw` only).

**Fingerprint** (`fingerprint(clean) -> str`):

```python
basis = "|".join([vendor_id, po_number, f"{amount:.2f}", currency, invoice_number])
return hashlib.sha256(basis.encode()).hexdigest()
```

**Insert, one record at a time, each inside a savepoint** (`transaction.atomic()` nested):

```
for each record in the batch:
    if invalid:
        INSERT status='INVALID', reason=..., fingerprint=NULL
            ON CONFLICT (invoice_id) DO NOTHING RETURNING id
    else:
        try (savepoint):
            INSERT status='PENDING', fingerprint=fp
                ON CONFLICT (invoice_id) DO NOTHING RETURNING id
        except UniqueViolation on the fingerprint constraint:
            rollback savepoint
            original = SELECT id FROM invoice WHERE fingerprint=fp AND duplicate_of_id IS NULL
                       AND status <> 'INVALID'
            INSERT status='DUPLICATE', duplicate_of=original, fingerprint=fp
                ON CONFLICT (invoice_id) DO NOTHING RETURNING id
    no row returned  -> count as "already_seen"
```

> **Important:** use `ON CONFLICT (invoice_id) DO NOTHING` **with the column named**. A bare
> `ON CONFLICT DO NOTHING` would also silently swallow the fingerprint conflict, and we would
> lose the DUPLICATE record. Use raw SQL via `connection.cursor()`; Django's
> `bulk_create(ignore_conflicts=True)` cannot tell you which rows were new.

Counter updates for new rows happen in the same transaction (section 5.1). The counter
tables are therefore built in Step 2, not Step 5.

> **Transaction scope:** one transaction **per invoice** (the savepoint sits inside it), not
> one per batch. A batch-wide transaction holds row and counter locks for the whole batch,
> so two overlapping batches can deadlock. If a batch dies halfway, the upstream re-sends it
> and the rows already written come back as `already_seen`.
>
> **Counter lock order:** `counters.py` updates `status_count` rows sorted by status, then
> `outcome_hourly` rows, each table in one statement. Every transaction takes counter locks
> in the same order, so they cannot deadlock on each other.

**Response:** `202 {"accepted": n, "duplicate": n, "invalid": n, "already_seen": n,
"rejected": [{"index": i, "error": "..."}], "trace_id": "..."}`.

The API **never** calls the ERP and never enqueues Celery tasks. The dispatcher does that.

**`scripts/feed.py <file.ndjson> [--url] [--batch 500]`**: reads lines, POSTs batches, on 429
sleeps `Retry-After` and retries the same batch, prints totals at the end.

**Done when:** feeding the 3,000-row file twice leaves the same row count, and the second run
reports everything as `already_seen`.

---

### Step 3 — Worker core (1:15–2:45)

#### 3.1 `services/erp_client.py`

```python
class ErpClient:
    def match(self, po_number: str, vendor_id: str, amount: Decimal, currency: str,
              trace_id: str) -> httpx.Response: ...
```

- One module-level `httpx.Client(base_url=ERP_BASE_URL, timeout=httpx.Timeout(connect=1.0,
  read=2.5, write=2.5, pool=2.5))`. It is thread-safe; reuse it.
- `GET /erp/purchase-orders/match` with params; `amount` sent as `f"{amount:.2f}"`.
- Send header `X-Request-ID: trace_id`.
- Does **not** catch exceptions and makes **no** decisions. It records `erp_request_seconds`.

#### 3.2 `services/classify.py` (pure function, no I/O)

```python
class Kind(StrEnum):
    MATCHED = "matched"; UNMATCHED = "unmatched"
    RATE_LIMITED = "rate_limited"; TRANSIENT = "transient"; PERMANENT = "permanent"

@dataclass(frozen=True)
class Outcome:
    kind: Kind
    reason: str | None = None         # e.g. "vendor_mismatch", "http_503", "timeout"
    retry_after: float | None = None  # seconds, for RATE_LIMITED
    body: dict | None = None

def classify(response: httpx.Response | None, exc: Exception | None) -> Outcome: ...
```

| Input | Outcome |
|---|---|
| `httpx.TimeoutException` | TRANSIENT, `timeout` |
| `httpx.ConnectError`, `httpx.RemoteProtocolError`, `httpx.ReadError` | TRANSIENT, `connection_error` |
| status 429 | RATE_LIMITED, `retry_after` = header `Retry-After` as float, default 1.0 |
| status 500, 502, 503, 504 | TRANSIENT, `http_<code>` |
| other 4xx | PERMANENT, `http_<code>` |
| 200, body not JSON or no `matched` key | TRANSIENT, `bad_body` |
| 200, `matched: true` | MATCHED (currency is checked later, in `matching.py`) |
| 200, `matched: false` | UNMATCHED, `reason` = body `reason` (default `erp_unmatched`) |

#### 3.3 `services/rate_limiter.py`

Lua script, registered once with `redis.register_script`:

```lua
-- KEYS[1] = "erp:rl"   KEYS[2] = "erp:cooldown"   ARGV[1] = limit
-- Returns 0 = go now; otherwise milliseconds to wait.
local cool = redis.call('PTTL', KEYS[2])
if cool > 0 then return cool end
local t = redis.call('TIME')                     -- Redis clock, shared by all workers
local key = KEYS[1] .. ':' .. t[1]               -- one counter per clock second
local n = redis.call('INCR', key)
if n == 1 then redis.call('EXPIRE', key, 2) end
if n <= tonumber(ARGV[1]) then return 0 end
return math.floor((1000000 - tonumber(t[2])) / 1000) + 1
```

```python
class RateLimitBusy(Exception): ...

def acquire_erp_slot(max_wait: float = LIMITER_MAX_WAIT) -> None:
    waited = 0.0
    while True:
        wait_ms = _script(keys=["erp:rl", "erp:cooldown"], args=[ERP_RPS])
        if wait_ms == 0:
            return
        if waited >= max_wait:
            raise RateLimitBusy()
        pause = wait_ms / 1000 + random.uniform(0, 0.05)
        time.sleep(pause)
        waited += pause
        LIMITER_WAIT.observe(pause)

def start_cooldown(seconds: float) -> None:
    redis.set("erp:cooldown", 1, px=int(seconds * 1000))
```

Why per clock second and not a token bucket: the ERP counts per clock second. A bucket of 10
can send 10 at x.00 and 9 more at x.99, which is 19 in one ERP window.

#### 3.4 `services/claims.py` (all raw SQL, each in its own transaction)

```python
def claim(invoice_pk: int) -> ClaimedInvoice | None
```
```sql
UPDATE invoice
SET status='PROCESSING', lease_token=%(tok)s,
    lease_until=now() + make_interval(secs => %(lease)s), updated_at=now()
WHERE id=%(id)s AND status='QUEUED'
RETURNING id, invoice_id, vendor_id, po_number, amount, currency, trace_id,
          attempts, rate_limited_count;
```
No row → return `None` (another worker has it, or it is finished).

```python
def finish(c: ClaimedInvoice, status: str, reason: str | None, result: ResultData) -> bool
```
```sql
UPDATE invoice SET status=%(status)s, reason=%(reason)s, lease_token=NULL,
       lease_until=NULL, finished_at=now(), updated_at=now()
WHERE id=%(id)s AND lease_token=%(tok)s;
```
Then (same transaction, only if 1 row updated) `INSERT INTO reconciliation_result ...`
and counters. 0 rows → log `invoice.lease_lost` and return `False`.

```python
def reschedule(c, delay_s: float, *, count_attempt: bool, count_rate_limited: bool,
               error: str) -> bool
```
Sets `status='RETRY_SCHEDULED'`, `next_attempt_at=now()+delay`, increments `attempts` /
`rate_limited_count` as asked, sets `last_error`, clears lease. Guarded by `lease_token`.

```python
def dead_letter(c, reason: str, error: str) -> bool
```
Sets `status='DEAD_LETTERED'`, `reason`, `last_error`, `dead_lettered_at=now()`, clears lease.
Guarded by `lease_token`.

```python
def record_attempt(invoice_pk, attempt_no, started_at, duration_ms, http_status, error,
                   error_class, trace_id) -> None
```

#### 3.5 Backoff

```python
def backoff_seconds(attempt: int, base: float = 2, cap: float = 300) -> float:
    return random.uniform(0, min(cap, base * 2 ** attempt))   # "full jitter"
```

#### 3.6 `tasks.reconcile(invoice_pk)` — the worker flow

```
c = claims.claim(invoice_pk)
if c is None: return                                  # someone else has it / done
bind structlog: trace_id, invoice_id

decision = matching.decide_from_cache(c, po_cache.get(c.po_number))
if decision: claims.finish(c, decision..., decided_by="cache"); return

lock = po_cache.try_lock(c.po_number)                 # SET po:lock:{po} NX PX 5000
if not lock and po_cache.is_unknown(c.po_number):
    claims.reschedule(c, 2, count_attempt=False, count_rate_limited=False, error="po_lock")
    return

try:
    rate_limiter.acquire_erp_slot()
except RateLimitBusy:
    claims.reschedule(c, 2 + random.uniform(0, 3), count_attempt=False,
                      count_rate_limited=False, error="limiter_busy"); return

response, exc = call ErpClient.match(...) catching Exception into exc
outcome = classify(response, exc)
claims.record_attempt(...)
po_cache.learn(c.po_number, c.vendor_id, outcome)     # only on MATCHED / UNMATCHED

match outcome.kind:
  MATCHED:      status, reason = matching.check_currency(c, outcome.body)  # MATCHED or UNMATCHED
                claims.finish(c, status, reason, decided_by="erp")
  UNMATCHED:    claims.finish(c, "UNMATCHED", outcome.reason, decided_by="erp")
  RATE_LIMITED: rate_limiter.start_cooldown(outcome.retry_after)
                if c.rate_limited_count + 1 >= MAX_RATE_LIMITED: treat as TRANSIENT
                else: claims.reschedule(c, outcome.retry_after, count_attempt=False,
                                        count_rate_limited=True, error="http_429")
  TRANSIENT:    if c.attempts + 1 >= MAX_ATTEMPTS: claims.dead_letter(c, "max_attempts", reason)
                else: claims.reschedule(c, backoff_seconds(c.attempts + 1),
                                        count_attempt=True, count_rate_limited=False,
                                        error=outcome.reason)
  PERMANENT:    claims.dead_letter(c, "permanent_error", outcome.reason)

after classify: rate_limiter.record_health(outcome)    # breaker-lite, see 3.6.1
except any unexpected Exception (a bug in our code):
    log invoice.internal_error with traceback
    treat as TRANSIENT with reason "internal_error"     # retried, then dead-lettered
finally: release the PO lock if we hold it
```

#### 3.6.1 Breaker-lite (keeps an ERP outage from burning every invoice's attempts)

```python
def record_health(outcome: Outcome) -> None:
    if outcome.kind is Kind.TRANSIENT:
        streak = redis.incr("erp:fail_streak")
        if streak >= 20:                       # 20 failures in a row across all workers
            start_cooldown(30)                 # everyone pauses 30 s, then a few probe calls
            redis.set("erp:fail_streak", 0)
            log erp.breaker_open; BREAKER_OPEN_TOTAL.inc()
    elif outcome.kind in (Kind.MATCHED, Kind.UNMATCHED):
        redis.set("erp:fail_streak", 0)
```

It reuses the cooldown key, so the limiter pauses every worker for free. During a 1-hour
outage only about 20 invoices per 30 s use up an attempt, instead of 8 every second.

Celery task options: `@shared_task(acks_late=True, reject_on_worker_lost=True,
ignore_result=True)`. **Never** use `self.retry(countdown=...)` or `eta`; retries are rows.

#### 3.7 `tasks.dispatch()` — runs every 2 s from beat

Before running, take a Redis lock `SET dispatch:lock 1 NX PX 10000` so only one dispatcher
runs at a time. Then:

1. **Poison check** (dead-letter rows that keep crashing workers):
   ```sql
   UPDATE invoice SET status='DEAD_LETTERED', reason='poison', dead_lettered_at=now(),
          lease_token=NULL, updated_at=now()
   WHERE status='PROCESSING' AND lease_until < now() AND crash_count + 1 >= %(max_crashes)s
   RETURNING id;
   ```
2. **Room check:** `room = QUEUE_HIGH_WATER - redis.llen("reconcile")`. If `room <= 0`, stop.
3. **Pick and mark** (one transaction):
   ```sql
   WITH due AS (
     SELECT id, status FROM invoice
     WHERE status = 'PENDING'
        OR (status = 'RETRY_SCHEDULED' AND next_attempt_at <= now())
        OR (status IN ('QUEUED','PROCESSING') AND lease_until < now())
     ORDER BY updated_at
     LIMIT %(n)s
     FOR UPDATE SKIP LOCKED
   )
   UPDATE invoice i
   SET status='QUEUED', lease_token=NULL,
       lease_until=now() + make_interval(secs => %(queue_lease)s),
       crash_count = i.crash_count + CASE WHEN due.status='PROCESSING' THEN 1 ELSE 0 END,
       updated_at=now()
   FROM due WHERE i.id = due.id
   RETURNING i.id;
   ```
   with `n = min(DISPATCH_BATCH, room)`.
4. **After commit**, `reconcile.apply_async(args=[id], queue="reconcile")` for each id.

If the process dies between step 3 and 4, those rows stay `QUEUED`; their `lease_until`
expires and they are picked up again. Nothing is lost.

#### 3.8 Celery settings (`config/celery.py`)

```python
task_acks_late = True
task_reject_on_worker_lost = True
worker_prefetch_multiplier = 2
broker_transport_options = {"visibility_timeout": 300}
task_routes = {"invoices.tasks.reconcile": {"queue": "reconcile"},
               "invoices.tasks.dispatch": {"queue": "dispatch"}}
beat_schedule = {"dispatch": {"task": "invoices.tasks.dispatch", "schedule": 2.0,
                              "options": {"expires": 10}}}
```

> **Measured, not guessed.** With `worker_prefetch_multiplier = 1` one worker (10 threads)
> reached only ~5.9 ERP calls/s in Docker: a thread gets its next message only after acking
> the last one, so ~3 threads sat idle. With `2` it reached 7.97/s (our limiter's cap), with
> 0 429s. Unstarted prefetched messages of a dead worker are recovered by the queue lease.
>
> **`dispatch` has its own queue** and the worker runs `-Q reconcile,dispatch`. On Celery's
> default queue nobody would consume it; on `reconcile` it would wait behind ~1,000 tasks.

**Done when:** a 3,000-invoice run with `ERP_CHAOS=1` ends with every row in a final status,
and `erp_responses_total{code="429"}` stays near zero.

---

### Step 4 — Cache and currency (2:45–3:30)

**Rule: the cache can say no; only the ERP can say yes.**

`services/po_cache.py`: Redis hash `po:{po_number}`.

| ERP answer | Store | TTL |
|---|---|---|
| `matched: true` | `vendor_id`, `po_amount`, `currency`, `status` | `PO_CACHE_TTL` |
| `vendor_mismatch` | `vendor_id = body.expected_vendor` | `PO_CACHE_TTL` |
| `amount_out_of_tolerance` | `vendor_id = invoice vendor`, `po_amount = body.po_amount` | `PO_CACHE_TTL` |
| `po_not_found` | `missing = 1` | `PO_NEGATIVE_TTL` |

Merge new fields into the hash (`HSET`) and reset the TTL.

`services/matching.py` (pure):

```python
def decide_from_cache(inv, facts: dict | None) -> tuple[str, str] | None:
    # returns ("UNMATCHED", reason) or None.  NEVER returns MATCHED.
    if not facts: return None
    if facts.get("missing"): return ("UNMATCHED", "po_not_found")
    if "vendor_id" in facts and facts["vendor_id"] != inv.vendor_id:
        return ("UNMATCHED", "vendor_mismatch")
    if "po_amount" in facts and abs(inv.amount - Decimal(facts["po_amount"])) > Decimal(facts["po_amount"]) * Decimal("0.01"):
        return ("UNMATCHED", "amount_out_of_tolerance")
    return None

def check_currency(inv, body: dict) -> tuple[str, str | None]:
    if body.get("currency") and body["currency"] != inv.currency:
        return ("UNMATCHED", "currency_mismatch")
    return ("MATCHED", None)
```

Metrics: `cache_lookups_total{result="hit_decided"|"hit_undecided"|"miss"}`.

**Done when:** logs show `decided_by=cache` and the ERP call count is lower than the number
of invoices processed.

---

### Step 5 — Reporting and observability (3:30–4:15)

#### 5.1 Counters (`services/counters.py`)

Called inside the same transaction as every status change:

```python
def on_transition(old: str | None, new: str) -> None:
    # status_counts: -1 old (if any), +1 new
    # INSERT ... ON CONFLICT (status) DO UPDATE SET count = status_count.count + EXCLUDED.count
    # if new is final: outcome_hourly(date_trunc('hour', now()), new) += 1
```

#### 5.2 `GET /reports/summary?window_hours=1`

Cached in Redis for 5 s under `report:summary:{window_hours}`.

```json
{
  "generated_at": "...",
  "counts": {"PENDING": 0, "QUEUED": 0, "PROCESSING": 0, "RETRY_SCHEDULED": 0,
             "MATCHED": 0, "UNMATCHED": 0, "INVALID": 0, "DUPLICATE": 0, "DEAD_LETTERED": 0},
  "window_hours": 1,
  "window": {"matched": 0, "unmatched": 0, "match_rate": 0.0},
  "stuck": {"count": 0, "oldest": [{"invoice_id": "...", "status": "...",
            "age_seconds": 0, "attempts": 0, "last_error": "..."}]},
  "queue_depth": 0,
  "lag": {"oldest_unfinished_age_seconds": 0, "completions_per_sec_5m": 0.0,
          "estimated_drain_seconds": 0},
  "dead_letter_reasons": {"max_attempts": 0, "poison": 0, "permanent_error": 0}
}
```

- `counts` → `SELECT * FROM status_count`.
- `window` → sum of `outcome_hourly` for the last N hours; `match_rate = matched /
  (matched + unmatched)`, or `null` if 0.
- `stuck` → non-final rows with `updated_at < now() - STUCK_AFTER_SECONDS` (uses the partial
  index), count + 20 oldest.
- `dead_letter_reasons` → `GROUP BY reason` on the DEAD_LETTERED partial index.

#### 5.3 Logs (`observability.py`)

structlog → JSON to stdout. Bound on every line: `trace_id`, `invoice_id` (when known).
Pass `trace_id` to Celery via task `headers`, and bind it at task start.

Events: `invoice.received`, `invoice.duplicate`, `invoice.invalid`, `invoice.dispatched`,
`invoice.claimed`, `erp.call` (status, duration_ms), `invoice.cache_decided`,
`invoice.retry_scheduled` (delay, reason), `invoice.outcome`, `invoice.dead_lettered`,
`invoice.lease_lost`, `erp.cooldown_started`.

#### 5.4 Metrics

| Metric | Type | Labels |
|---|---|---|
| `invoices_ingested_total` | counter | `result` (accepted/duplicate/invalid/already_seen) |
| `invoices_outcome_total` | counter | `status`, `reason` |
| `erp_request_seconds` | histogram | – |
| `erp_responses_total` | counter | `code` (200/429/503/504/timeout/conn_error) |
| `erp_breaker_open_total` | counter | – |
| `retries_scheduled_total` | counter | `reason` |
| `dead_letters_total` | counter | `reason` |
| `cache_lookups_total` | counter | `result` |
| `limiter_wait_seconds` | histogram | – |
| `queue_depth` | gauge | computed at scrape time (`LLEN reconcile`) |
| `backlog` | gauge | `status`, computed at scrape time from `status_count` |
| `oldest_unfinished_age_seconds` | gauge | scrape time: `now() - min(created_at)` over non-final rows (partial index) |
| `estimated_drain_seconds` | gauge | scrape time: unfinished count ÷ completions/s over the last 5 min (from `outcome_hourly` + a Redis counter of recent completions); `-1` if nothing completed |

- API: `GET /metrics` uses `prometheus_client` multiprocess mode
  (`PROMETHEUS_MULTIPROC_DIR`, because gunicorn runs 4 processes).
- Worker: threads pool = one process, so call `prometheus_client.start_http_server(9101)`
  on `worker_process_init`.

#### 5.5 `manage.py requeue_dead_letters`

Options: `--invoice INV-...` (repeatable), `--reason <reason>`, `--since 1h`, `--dry-run`.
Sets `status='PENDING'`, `attempts=0`, `crash_count=0`, `rate_limited_count=0`,
`reason=NULL`; keeps `InvoiceAttempt` history; updates counters. Prints how many rows.

**Done when:** during a run, `/reports/summary` and `/metrics` answer "what is the pipeline
doing right now?" without reading logs.

> **As built:**
> - The worker starts its metrics server on Celery's `worker_init` signal. `worker_process_init`
>   only fires in prefork child processes, so with the threads pool it would never run.
> - The backlog/lag gauges are exposed by the API's `/metrics` only (one source of truth);
>   the worker's :9101 exposes ERP, cache, limiter, retry and outcome metrics.
> - `PROMETHEUS_MULTIPROC_DIR` is set for the gunicorn process only, so
>   `docker compose exec api pytest` does not write into the API's metrics.
> - `CELERY_WORKER_REDIRECT_STDOUTS = False`, so structlog's JSON lines are not wrapped in
>   Celery's own log format.
> - `requeue_dead_letters` refuses to run with no filter (no accidental "requeue everything").
> - The container works from `/app` (with `PYTHONPATH=/app/src`), so `pytest` finds
>   `pytest.ini` and `tests/`. Management commands: `python src/manage.py ...`.

---

### Step 6 — Tests (4:15–5:15)

Run against the real Postgres and Redis from compose (`pytest` inside the `api` container or
with `DATABASE_URL` / `REDIS_URL` pointing at the published ports). Use
`@pytest.mark.django_db(transaction=True)` for the concurrency tests so threads see commits.

| # | File | Test | Pass condition |
|---|---|---|---|
| 1 | `test_idempotency.py` | 20 threads POST the same invoice at once | 1 `Invoice` row; status counts add up |
| 2 | `test_idempotency.py` | same content, new `invoice_id` (also inside one batch) | 2nd row is `DUPLICATE`, `duplicate_of` = 1st |
| 3 | `test_claims.py` | claim → expire lease → dispatcher re-queues → 2nd worker claims + finishes → 1st worker calls `finish` | 1st `finish` returns `False`; exactly 1 `ReconciliationResult` |
| 4 | `test_claims.py` | 2 threads call `claim` on the same QUEUED row | exactly one gets a result |
| 5 | `test_classify.py` | parametrized over every row of the step 3.2 table | correct `Kind` + reason |
| 6 | `test_rate_limiter.py` | 30 threads call `acquire_erp_slot` for 3 s, record `TIME` of each success | no clock second has > `ERP_RPS` successes; cooldown key blocks all |
| 7 | `test_matching.py` | tolerance edges (exactly 1%, 1.01%), currency mismatch, `decide_from_cache` never returns MATCHED | as described |
| 8 | `test_reconcile_flow.py` (respx) | 503 × 6 → DEAD_LETTERED, reason saved; 429 × 3 then 200 → MATCHED with `attempts == 0`; timeout → retried | as described |
| 9 | `test_reconcile_flow.py` | 20 TRANSIENT outcomes in a row | `erp:cooldown` key is set |

---

### Step 7 — Docs (5:15–6:00)

- `README.md`: one-command run, demo script below, built vs deferred, known issues, time per area.
- `DESIGN.md`: already written; update numbers if the implementation changed.

Demo script for the README:

```bash
python harness/generate_invoices.py --count 3000 --seed 42 --out data/invoices.ndjson
ERP_PO_COUNT=1000 docker compose up -d --build
python scripts/feed.py data/invoices.ndjson
python scripts/feed.py data/invoices.ndjson        # again: everything "already_seen"
curl -s localhost:8000/reports/summary | python -m json.tool
curl -s localhost:8000/metrics | grep erp_responses_total
docker compose exec api pytest -q
```

---

## 7. Do not build (write these in README "Deferred")

- Login, auth, multiple clients/tenants.
- PO budget tracking (many invoices matching one PO and over-billing it). Design only.
- Real payment or a payment outbox. `MATCHED` is the finish line.
- Priority queues / per-source fairness. Design only.
- A full circuit breaker (half-open state, per-error-type thresholds). Breaker-lite (3.6.1)
  covers the outage case. Design only.
- Grafana dashboard JSON (bonus only if everything else is done). **Built after Step 7**,
  with graceful shutdown: `monitoring/` (Prometheus + Grafana behind the `monitoring`
  compose profile) and `stop_grace_period: 30s` on the worker.

## 8. Definition of done (checklist)

- [x] `docker compose up` brings up all six services in under 10 minutes on a clean machine
- [x] Feeding the dataset twice does not change row or result counts
- [x] No invoice has two `ReconciliationResult` rows (`UNIQUE` holds)
- [x] With chaos on, every invoice reaches a final status; dead letters have reasons
- [x] 429s stay near zero; limiter wait shows in metrics
- [x] `/reports/summary`, `/metrics`, `/healthz`, `/readyz` work
- [x] All tests in step 6 pass
- [x] README lists what was deferred and why
