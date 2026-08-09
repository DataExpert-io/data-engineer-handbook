# Weather Intelligence — Unstructured Data → Lakebase Vector Search → REST API

Assignment 2. Extends the Assignment 1 Novanega ticket tracker app (same Flask app, same `lakebase.py` connection helper) with a weather harvesting → embedding → semantic search pipeline.

## Data source

**National Weather Service API (api.weather.gov)** — free, no API key, no auth plumbing. Two endpoints are harvested per location:

- `GET /alerts/active?area={state}` — active watches/warnings, each with free-text `description` + `instruction` fields.
- `GET /gridpoints/{office}/{x},{y}/forecast` — multi-day forecast, narrative `detailedForecast` string per period.

NWS doesn't geocode city names, so `weather_client.py` resolves a small built-in list of major US cities (Chicago IL, Austin TX, New York NY, etc. — see `_CITY_COORDS`) to lat/lon before calling `/points/{lat},{lon}`. Any `"lat,lon"` pair also works directly. This is the one known shortcut in the harvester — see Limitations below.

## Schema

`weather_schema.sql` (also folded into `schema.sql`):

- **`weather_documents`** — one row per harvested alert or forecast period. `id` (stable TEXT PK — the NWS alert id, or a hash of location+grid+period for forecasts), `location`, `source_type` (`alert`/`forecast`), `headline`, `narrative_text`, `issued_at`, `payload` (raw JSONB for provenance), `synced_at`.
- **`weather_embeddings`** — one row per chunk. `document_id` (FK), `chunk_index`, `chunk_text`, `embedding vector(384)`, `model_name`, `created_at`, unique on `(document_id, chunk_index)` so re-embedding upserts instead of duplicating.
- HNSW index (`vector_cosine_ops`) on `weather_embeddings.embedding` for fast cosine search, same as the reference ticker-news pipeline.

**Chunking:** sliding window, `CHUNK_SIZE=800` / `CHUNK_OVERLAP=100` chars — matches the spec's suggested defaults. Most NWS alert/forecast text is short enough to fit in one chunk; chunking mainly kicks in for long combined alert description+instruction blocks.

**Embedding model:** `sentence-transformers/all-MiniLM-L6-v2`, 384-dim — same model as the reference news pipeline, so both stay queryable with the same `<=>` cosine-distance convention.

## Pipeline: sync → embed → search

```bash
# 1. Harvest + upsert raw documents
curl -X POST http://localhost:8000/weather/sync \
  -H "Content-Type: application/json" \
  -d '{"locations": ["Chicago, IL", "Austin, TX"], "limit": 50}'

# 2. Embed unembedded documents (batch script, psycopg2 -- not Spark)
python ingest_weather_embeddings.py

# 3. Semantic search
curl -X POST http://localhost:8000/weather/search \
  -H "Content-Type: application/json" \
  -d '{"query": "flash flood risk this weekend", "top_k": 5}'
```

- `/weather/sync` upserts on `weather_documents.id` (`ON CONFLICT DO UPDATE`) so re-running it refreshes rather than duplicates.
- `ingest_weather_embeddings.py` only picks up documents with zero rows in `weather_embeddings` (`NOT EXISTS` check), and upserts on `(document_id, chunk_index)` so re-running it is also safe.
- `/weather/search` loads the embedding model once (lazily, on first request) and reuses it for every query. Handles an empty `weather_embeddings` table (returns `results: []` with a message instead of erroring), blank queries (400), and clamps `top_k` to 1–20.

## Deploy as a Databricks App

Same process as Assignment 1, pointed at this folder:

1. Upload this folder to GitHub (same repo, e.g. `data-engineer-handbook/databricks-ai-bootcamp/Assignment2-nn-weather-search`), making sure `.env` is excluded.
2. Run `weather_schema.sql` (or the full `schema.sql`, which includes it) against Lakebase first, so the tables exist before deploy.
3. In Databricks, create a new App pointing its Git folder at this path in the repo.
4. Add the same **Secret** resource as Assignment 1 — scope/key matching what `LAKEBASE_URL` was stored under, resource key `LAKEBASE_URL`, permission Can read. `app.yaml` here is unchanged from Assignment 1's working config (`valueFrom: "LAKEBASE_URL"`).
5. Once running, hit `POST /weather/sync` and `POST /weather/search` against the deployed URL the same way as the local curl examples above, then run `ingest_weather_embeddings.py` locally against the same `LAKEBASE_URL` (it's a batch script, not part of the deployed app) to populate embeddings.

One thing to watch that didn't come up in Assignment 1: `/weather/sync` calls out to `api.weather.gov` over the network. Confirm the deployed app's egress isn't blocked before assuming a `synced: 0` response means bad locations rather than a blocked outbound call.

## Known limitations / what I'd improve with more time

- **Geocoding is a static lookup, not a real geocoder.** Only the ~15 cities in `weather_client._CITY_COORDS` resolve by name; anything else needs a raw `"lat,lon"` pair. A real geocoding API (e.g. Census Bureau geocoder, also free/no-key) would remove this limitation.
- **No dedup beyond the DB layer for forecast periods.** Forecast document IDs are a hash of location+grid+period-number+start-time, so re-syncing the same day produces the same IDs (good), but a forecast reissued mid-day with the same period number will overwrite rather than version.
- **No scheduled re-sync yet** — `/weather/sync` is manually triggered. A Databricks Job or cron calling it every N minutes would keep alerts current (stretch goal in the spec).
- **Single source type combined, not yet filterable** — `/weather/search` searches across both alerts and forecasts; adding a `source_type` filter param would let callers scope to just active warnings vs. just forecast narrative.
- **No RAG summary endpoint** — the stretch-goal `GET /weather/search?query=...` with an LLM-generated summary of top results isn't built; `/weather/search` returns raw ranked matches only.
