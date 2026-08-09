# Deployment Plan — Assignment 2 (Weather Intelligence)

This deploys `Assignment2-nn-weather-search` as a **new, separate Databricks App** — it does not touch or redeploy the already-graded Assignment 1 app. Same connection pattern (`lakebase.py`, `app.yaml`), new Git folder, new app, new URL.

## Pre-deploy

1. **Confirm `.env` is not in the folder** (or is excluded via `.gitignore` — already set). Never let a real `LAKEBASE_URL` reach GitHub.
2. **Run schema first**, against the same Lakebase instance Assignment 1 uses: execute `schema.sql` (or `weather_schema.sql` alone if `tickets`/`ticket_messages` already exist from Assignment 1's run) via the Lakebase SQL editor or psql. This creates `weather_documents`, `weather_embeddings`, and attempts `CREATE EXTENSION vector` / the HNSW index. If those two fail under your role due to ownership (same class of issue as the `CREATE INDEX` problem on Assignment 1), that's non-fatal — `app.py` retries them as a safety net at startup and swallows the failure the same way it did for the ticket indexes.
3. **Confirm the `LAKEBASE_URL` secret still exists** in the same Databricks secret scope/key you set up for Assignment 1 (`put_secret()` under that scope). You're reusing the secret, not recreating it.

## Deploy steps

1. **Upload the folder to GitHub.** Same repo as Assignment 1, sibling path (e.g. `databricks-ai-bootcamp/Assignment2-nn-weather-search`). Double-check no `.env` file made it into the upload before confirming.
2. **Create a new Databricks App** (do not edit the Assignment 1 app). Point its Git folder configuration at this new repo path.
3. **Add the Secret resource to this new app.** This is the step most likely to bite you — resources are per-app, not shared automatically even though the secret itself already exists from Assignment 1. In the new app's Resources section: add a Secret resource, same scope/key as before, resource key = `LAKEBASE_URL` (must match `app.yaml`'s `valueFrom` exactly), permission = Can read.
4. **Deploy and wait for compute to start.**
5. **Hit `/healthz`** on the new app's URL. Expect `{"status": "ok"}`. If it 404s or hangs, compute is likely still starting — same pattern as Assignment 1.
6. **Load `/`** in a browser. Confirm the ticket tracker UI renders (not blank, not raw `{{ }}` template syntax). If you see unrendered Jinja tags, hard refresh (Cmd+Shift+R) before assuming it's broken — this bit us on Assignment 1 and was just a stale cache.

## Known gotchas to watch for (recurrence of Assignment 1 issues)

- **Missing secrets / crash on boot** — means the Secret resource wasn't added to *this* app, or the resource key doesn't match `app.yaml`'s `valueFrom`.
- **`fe_sendauth: no password supplied`** — the stored secret value is stale, truncated, or has a stray `LAKEBASE_URL=` prefix pasted into it. Verify the secret value directly rather than guessing.
- **App shows "Crashed" but deployment history is all green** — check the runtime logs, not the deployment logs; the app can build fine and still fail at process startup (e.g., an unhandled exception in `ensure_weather_schema()`).
- **No visible Stop/Start button** — use the Databricks CLI: `databricks apps stop <app-name>` / `databricks apps start <app-name>`.

## Rollback

If this deployment is broken, it's isolated — stop it via the CLI or delete the app. The Assignment 1 app is a completely separate deployment and is unaffected regardless of what happens here.
