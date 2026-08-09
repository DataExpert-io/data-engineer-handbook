# Novanega Feedback & Feature Request Tracker

Databricks App backed by Lakebase (Postgres). Built to PRD v3 (see project docs). Implements:

1. View all tickets
2. View a ticket's detail and message history
3. Create a new ticket with an initial message
4. Add a message to an existing ticket
5. Edit ticket title, category, priority, and status

Status filtering is intentionally deferred (PRD v2/v3 decision) but the `status` field itself is core and shown/editable throughout.

## Files

- `app.py` - Flask app and all routes (see below)
- `lakebase.py` - Lakebase connection helper (single `LAKEBASE_URL`, resolved from a Databricks secret in production or `.env` locally)
- `schema.sql` - table DDL (run this first)
- `seed.sql` - the three required seed tickets, two messages each
- `templates/index.html` - ticket list + create-ticket form
- `templates/ticket.html` - ticket detail, edit form, message history, add-message form
- `app.yaml` - Databricks App deployment config
- `requirements.txt`, `.env.example`, `.gitignore`

## Routes

| Method | Path | Story |
|---|---|---|
| GET | `/` | 1 - ticket list page |
| GET | `/ticket/<id>` | 2, 5 - ticket detail page |
| GET | `/api/tickets` | 1 - list tickets, newest first |
| POST | `/api/tickets` | 3 - create ticket + initial message |
| GET | `/api/tickets/<id>` | 2 - ticket detail |
| PATCH | `/api/tickets/<id>` | 5 - edit title/category/priority/status |
| GET | `/api/tickets/<id>/messages` | 2 - messages, oldest first |
| POST | `/api/tickets/<id>/messages` | 4 - add a message |
| GET | `/healthz` | health check |

All validation from PRD v3 (explicit blank checks on title/message_text/author, enum checks on category/priority/status/submitted_by, protected-field rejection on edit) lives in `app.py`'s `validate_*` functions, and is backed up by `CHECK` constraints in `schema.sql` as a second line of defense.

## Setup

### 1. Create a Lakebase instance and a native-password role

In your Databricks workspace: **Catalog** > **Lakebase** > **Create Lakebase instance**. Once it's running, open **Roles & Databases**, enable native (password) authentication if needed, and create a role with a static password. Copy the connection URL it gives you — it looks like:

```
postgresql://<role>:<password>@<host>.database.cloud.databricks.com:5432/databricks_postgres?sslmode=require
```

### 2. Store the connection URL as a Databricks secret

From a notebook or terminal attached to a running cluster:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
w.secrets.create_scope(scope="database")
w.secrets.put_secret(scope="database", key="lakebase-url", string_value="<your connection URL>")
```

`app.yaml` already points at `database/lakebase-url` — no further config needed for deployment.

### 3. Create the schema and seed data

Open the Lakebase SQL editor (or connect with `psql` using the connection URL) and run, in order:

```
schema.sql
seed.sql
```

This gives you the tables + sample records screenshot for submission.

### 4. Local development (optional, before deploying)

```
cp .env.example .env
# paste your connection URL into LAKEBASE_URL in .env

pip install -r requirements.txt

python app.py
```

Visit `http://localhost:8000`.

### 5. Deploy as a Databricks App

1. **Workspace** > **Create** > **Git folder**, paste this repo's Git URL, create.
2. **Compute** > **Apps** > **Create app** > **Custom**, name it (e.g. `novanega-tracker`).
3. Point the app's source at the Git folder from step 1 (the folder containing `app.py` and `app.yaml`). Databricks reads `app.yaml` automatically.
4. Click **Deploy**. Once running, open the app URL and confirm `/healthz` returns `{"status": "ok"}`.
5. After any code change: pull the latest into the Git folder, click **Deploy** again.

### 6. Verify (assignment step 4)

- Existing (seeded) tickets load on `/`
- Create a new ticket — appears in the list
- Add a message to an existing ticket — appears in its detail view
- Update a ticket's status — reflected in the list and detail view
- Refresh the browser — everything above is still there (it's reading from Lakebase, not app memory)

## Submission checklist

- [ ] Deployed app URL
- [ ] Zipped source code (this folder)
- [ ] Screenshot of the deployed app
- [ ] Screenshot of the Lakebase tables with sample records
- [ ] 3-5 sentence reflection (see `REFLECTION_TEMPLATE.md`) — hardest part, Lakebase vs. a traditional analytics table, next feature you'd add

Do not commit or submit real database credentials, API keys, or secrets. `LAKEBASE_URL` should only ever live in `.env` (gitignored) locally, or in the Databricks secret scope in production.
