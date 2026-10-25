# CLAUDE.md

Senior-engineer take-home: an invoice ingestion and reconciliation pipeline.
Requirements: `ASSIGNMENT.md`. What to build: `PLANNING.md`. Why: `DESIGN.md`.

## How to work

- Read `PLANNING.md` and `DESIGN.md` before writing code.
- Implement **one Step from `PLANNING.md` at a time**. Run its "Done when" check, show how
  you verified it, then stop and let the developer review and commit.
- If something in the plan looks wrong or impossible, **stop and say so**. Don't quietly
  build something different. If a change is agreed, update `PLANNING.md` / `DESIGN.md` too.
- Keep code small and explainable. The developer must defend every line in a 30-minute call.
  No features beyond the plan (see "Do not build" in `PLANNING.md`).

## Hard rules (these protect correctness; never break them)

- **Never modify anything in `harness/`.** The mock ERP's rate limit and failures are the test.
- **Postgres is the source of truth.** Redis carries invoice IDs only, plus the limiter, cooldown and cache.
- **Never schedule retries with Celery `countdown`, `eta` or `self.retry()`.** A retry is a
  row update: `RETRY_SCHEDULED` + `next_attempt_at`. The dispatcher re-sends it.
- **Every final write is guarded by `WHERE lease_token = <mine>`.** Every status change goes
  through `claims.py` and updates the counter tables in the same transaction.
- **Ingest uses `ON CONFLICT (invoice_id) DO NOTHING`, with the column named.** A bare
  `ON CONFLICT DO NOTHING` would silently swallow fingerprint duplicates.
- **The cache can say no; only the ERP can say yes.** `decide_from_cache` never returns MATCHED.
- **Always check currency locally** on an ERP match. The ERP ignores it.
- **Money is `Decimal` / `numeric(14,2)`**, never float.
- **ERP calls only after `acquire_erp_slot()`.** Never add per-worker sleeps or Celery `rate_limit`.
- A 429 or `RateLimitBusy` **never** increments `attempts`.
- Decisions live in pure functions in `invoices/services/`. `views.py` and `tasks.py` stay thin.

## Commands

```bash
python harness/generate_invoices.py --count 3000 --seed 42 --out data/invoices.ndjson
ERP_PO_COUNT=1000 docker compose up -d --build     # ERP_PO_COUNT = max(100, count // 3)
python scripts/feed.py data/invoices.ndjson
curl -s localhost:8000/reports/summary
docker compose exec api pytest -q
docker compose exec api ruff check .
docker compose exec api python src/manage.py requeue_dead_letters --reason max_attempts --since 2h
```

## Known harness facts (don't "fix" these)

- Only ~0.08% of the generated invoices match the ERP (generator and ERP use different random
  streams). This is expected, and documented in `DESIGN.md` A5. Test the matching rule with
  fixtures (PLANNING.md 5.1), not the dataset.
- The ERP limit resets each calendar second, so our limiter counts per Redis `TIME` second at 8/s.
- An ERP "timeout" is a 6 s hang; the client read timeout is 2.5 s.

## Tests

- Concurrency and idempotency tests use real Postgres and Redis, with
  `@pytest.mark.django_db(transaction=True)`. Don't mock the database: the constraint is what's being tested.
- ERP behaviour in tests is mocked with `respx`, using the fixtures in PLANNING.md 5.1.
- Never weaken or delete a failing test to make it pass. Fix the code or report the problem.
