# Post-Deployment Test Plan — Assignment 2 (Weather Intelligence)

Run against the deployed app URL after `DEPLOYMENT_PLAN.md` is complete. Two parts: a regression pass on the inherited ticket tracker, then a smoke test of the new weather pipeline.

## Part 1 — Ticket tracker regression

The ticket code in this deployment is byte-for-byte identical to Assignment 1's graded logic (verified by diff), but it's running in a new process against — potentially — the same live tables, so it's worth a quick pass rather than assuming.

| # | Step | Expected result |
|---|------|------------------|
| 1 | Load `/` | Existing tickets list renders (same data as Assignment 1, since it's the same Lakebase tables) |
| 2 | Create a new ticket | Ticket appears in the list immediately |
| 3 | Open the new ticket, edit title/status/priority, save | Change persists and shows on reload |
| 4 | Add a message to the ticket | Message appears in the thread |
| 5 | Refresh the page | All of the above still shows — confirms writes are going to Lakebase, not held in memory |

If any of these fail, stop — it means something about this deployment's environment (not the code, which is verified identical) is misconfigured, most likely the secret/connection wiring from `DEPLOYMENT_PLAN.md`.

## Part 2 — Weather pipeline smoke test

### 2a. Empty-state check (before syncing anything)

| # | Step | Expected result |
|---|------|------------------|
| 1 | `POST /weather/search` with `{"query": "flooding"}` | 200 response, `results: []`, message saying no data has been embedded yet — not an error |

### 2b. Sync

| # | Step | Expected result |
|---|------|------------------|
| 2 | `POST /weather/sync` with `{"locations": ["Chicago, IL", "Austin, TX"]}` | 200 response with `synced` count > 0 (assuming either city has active alerts or a forecast, which is nearly always true) |
| 3 | Query `weather_documents` in Lakebase directly | Rows exist with populated `narrative_text`, `source_type`, `issued_at`, `payload`, `synced_at` |

### 2c. Embed

| # | Step | Expected result |
|---|------|------------------|
| 4 | Run `python ingest_weather_embeddings.py` (locally, pointed at the same `LAKEBASE_URL`) | Log output shows N documents found, chunk embeddings written |
| 5 | Query `weather_embeddings` in Lakebase directly | Rows exist, `embedding` populated, `model_name = 'sentence-transformers/all-MiniLM-L6-v2'` |

### 2d. Search

| # | Step | Expected result |
|---|------|------------------|
| 6 | `POST /weather/search` with a query relevant to what you synced (e.g. `"severe weather risk"` or `"forecast for this weekend"`) | 200 response, ranked `results` array with `location`, `headline`, `chunk_text`, `similarity`, highest similarity first |
| 7 | `POST /weather/search` with `{"query": "  "}` (blank) | 400, clear validation error |
| 8 | `POST /weather/search` with `{"query": "storms", "top_k": 500}` | 200, results clamped to 20 max, not an error |

### 2e. Idempotency (re-run safety)

| # | Step | Expected result |
|---|------|------------------|
| 9 | Re-run step 2 (`/weather/sync`) with the same locations | 200, no duplicate-key error, `synced` count reflects refreshed rows |
| 10 | Re-run step 4 (`ingest_weather_embeddings.py`) | Completes without error; no duplicate rows in `weather_embeddings` |

## Sign-off

- [ ] Part 1 (ticket regression) passed — no unexpected divergence from Assignment 1 behavior
- [ ] Part 2a–2d (weather pipeline) passed — sync, embed, search all working end to end
- [ ] Part 2e (idempotency) passed — safe to re-run without manual cleanup
- [ ] Screenshots captured for submission evidence (list view, a search result, and a re-run showing no crash)
