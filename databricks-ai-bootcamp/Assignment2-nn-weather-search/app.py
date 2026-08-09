"""
Novanega Feedback & Feature Request Tracker + Weather Intelligence
Databricks App backed by Lakebase (Postgres + pgvector).

Assignment 1 (Stories 1-5, ticket tracker) is unchanged below and kept
running so this app remains a true extension, not a rewrite. Assignment 2
adds a weather harvesting -> vectorization -> semantic search pipeline:

  POST /weather/sync    Harvest NWS alerts/forecasts for given locations,
                         upsert into weather_documents.
  POST /weather/search  Embed a query and return the top_k most
                         semantically similar weather_embeddings rows.

Embedding/ingestion itself happens out-of-band in
ingest_weather_embeddings.py (run after /weather/sync), matching the
day-2 reference app's split between sync (Flask) and embed (batch script).

Run locally:
    python app.py
Deploy as a Databricks App using app.yaml.
"""

import logging
import os

from flask import Flask, jsonify, render_template, request

import lakebase
import weather_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("novanega-tracker")

app = Flask(__name__)

CREATED_BY = os.environ.get("APP_IDENTITY", "novanega-app")

ALLOWED_CATEGORIES = {"Feature Request", "Bug", "Feedback", "Question"}
ALLOWED_STATUSES = {"open", "in_progress", "resolved"}
ALLOWED_PRIORITIES = {"Low", "Medium", "High"}
ALLOWED_SUBMITTED_BY = {"provider", "pilot_user", "internal"}

# Fields that Business Rule 16 (PRD v3) says are not editable after creation.
PROTECTED_FIELDS = {"submitted_by", "created_by", "created_at", "ticket_id"}

EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

# Loaded lazily on first use of /weather/search, not at import time, so that
# `python app.py --help`-style invocations and unit tests that never touch
# the search endpoint don't pay the (~90MB) model download/load cost.
_embedding_model = None


def get_embedding_model():
    global _embedding_model
    if _embedding_model is None:
        from sentence_transformers import SentenceTransformer

        logger.info("Loading embedding model %s ...", EMBEDDING_MODEL_NAME)
        _embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    return _embedding_model


def _vector_literal(embedding) -> str:
    """Format a list of floats as a pgvector input literal, e.g. '[0.1,0.2]'."""
    return "[" + ",".join(f"{v:.8f}" for v in embedding) + "]"


# ---------------------------------------------------------------------------
# Schema safety net. schema.sql / weather_schema.sql are the source of
# truth (run them first so the submission screenshot shows the tables), but
# this makes the app resilient if it's ever pointed at a fresh Lakebase
# instance.
# ---------------------------------------------------------------------------
def ensure_schema():
    lakebase.run_write(
        """
        CREATE TABLE IF NOT EXISTS tickets (
            ticket_id     BIGSERIAL PRIMARY KEY,
            title         TEXT NOT NULL,
            category      TEXT NOT NULL,
            status        TEXT NOT NULL DEFAULT 'open',
            priority      TEXT NOT NULL,
            created_by    TEXT NOT NULL DEFAULT 'novanega-app',
            submitted_by  TEXT NOT NULL,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT tickets_title_not_blank CHECK (length(trim(title)) > 0),
            CONSTRAINT tickets_category_allowed CHECK (category IN ('Feature Request','Bug','Feedback','Question')),
            CONSTRAINT tickets_status_allowed CHECK (status IN ('open','in_progress','resolved')),
            CONSTRAINT tickets_priority_allowed CHECK (priority IN ('Low','Medium','High')),
            CONSTRAINT tickets_submitted_by_allowed CHECK (submitted_by IN ('provider','pilot_user','internal'))
        )
        """
    )
    lakebase.run_write(
        """
        CREATE TABLE IF NOT EXISTS ticket_messages (
            message_id    BIGSERIAL PRIMARY KEY,
            ticket_id     BIGINT NOT NULL REFERENCES tickets(ticket_id) ON DELETE CASCADE,
            message_text  TEXT NOT NULL,
            author        TEXT NOT NULL,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT ticket_messages_text_not_blank CHECK (length(trim(message_text)) > 0),
            CONSTRAINT ticket_messages_author_not_blank CHECK (length(trim(author)) > 0)
        )
        """
    )
    # Index creation is best-effort and non-fatal. In Postgres, CREATE INDEX
    # (even with IF NOT EXISTS) requires being the table's owner -- the
    # app's runtime role often isn't the owner if schema.sql was run under
    # a different (e.g. admin) role, which is the normal/expected setup.
    # The indexes already exist from schema.sql in that case, so a failure
    # here is not a real problem; don't let it block the app from starting.
    try:
        lakebase.run_write("CREATE INDEX IF NOT EXISTS idx_tickets_created_at ON tickets (created_at DESC)")
        lakebase.run_write("CREATE INDEX IF NOT EXISTS idx_ticket_messages_ticket_id ON ticket_messages (ticket_id)")
    except Exception as exc:
        logger.warning(
            "Skipping ticket index creation (likely already created by schema.sql under a different role): %s",
            exc,
        )


def ensure_weather_schema():
    """Same safety-net pattern as ensure_schema(), for the weather tables."""
    try:
        lakebase.run_write("CREATE EXTENSION IF NOT EXISTS vector")
    except Exception as exc:
        logger.warning("Skipping CREATE EXTENSION vector (likely already enabled, or role lacks privilege): %s", exc)

    lakebase.run_write(
        """
        CREATE TABLE IF NOT EXISTS weather_documents (
            id              TEXT PRIMARY KEY,
            location        TEXT NOT NULL,
            source_type     TEXT NOT NULL,
            headline        TEXT,
            narrative_text  TEXT NOT NULL,
            issued_at       TIMESTAMPTZ,
            payload         JSONB,
            synced_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT weather_documents_source_type_allowed CHECK (source_type IN ('alert', 'forecast')),
            CONSTRAINT weather_documents_narrative_not_blank CHECK (length(trim(narrative_text)) > 0)
        )
        """
    )
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS weather_embeddings (
            id            BIGSERIAL PRIMARY KEY,
            document_id   TEXT NOT NULL REFERENCES weather_documents(id) ON DELETE CASCADE,
            chunk_index   INT NOT NULL,
            chunk_text    TEXT NOT NULL,
            embedding     vector({EMBEDDING_DIM}) NOT NULL,
            model_name    TEXT NOT NULL,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT weather_embeddings_doc_chunk_unique UNIQUE (document_id, chunk_index)
        )
        """
    )
    try:
        lakebase.run_write("CREATE INDEX IF NOT EXISTS idx_weather_documents_source_type ON weather_documents (source_type)")
        lakebase.run_write("CREATE INDEX IF NOT EXISTS idx_weather_embeddings_document_id ON weather_embeddings (document_id)")
        lakebase.run_write(
            "CREATE INDEX IF NOT EXISTS idx_weather_embeddings_embedding_hnsw "
            "ON weather_embeddings USING hnsw (embedding vector_cosine_ops)"
        )
    except Exception as exc:
        logger.warning(
            "Skipping weather index creation (likely already created by weather_schema.sql under a different role, "
            "or pgvector version doesn't support hnsw): %s",
            exc,
        )


# ---------------------------------------------------------------------------
# Validation helpers. PRD v3 principle: "explicit always" -- every mandatory
# field gets its own named check, nothing relies on a broader catch-all.
# ---------------------------------------------------------------------------
def _blank(value) -> bool:
    return not isinstance(value, str) or len(value.strip()) == 0


def validate_new_ticket(payload: dict) -> list:
    """Story 3 AC 2, 5, 6. Returns a list of field-level error strings."""
    errors = []
    if _blank(payload.get("title")):
        errors.append("title must not be blank")
    if _blank(payload.get("message_text")):
        errors.append("initial message text must not be blank")
    if _blank(payload.get("author")):
        errors.append("initial message author must not be blank")
    if payload.get("category") not in ALLOWED_CATEGORIES:
        errors.append(f"category must be one of {sorted(ALLOWED_CATEGORIES)}")
    if payload.get("priority") not in ALLOWED_PRIORITIES:
        errors.append(f"priority must be one of {sorted(ALLOWED_PRIORITIES)}")
    if payload.get("submitted_by") not in ALLOWED_SUBMITTED_BY:
        errors.append(f"submitted_by must be one of {sorted(ALLOWED_SUBMITTED_BY)}")
    return errors


def validate_new_message(payload: dict) -> list:
    """Story 4 AC 2, 4. Returns a list of field-level error strings."""
    errors = []
    if _blank(payload.get("message_text")):
        errors.append("message_text must not be blank")
    if _blank(payload.get("author")):
        errors.append("author must not be blank")
    return errors


def validate_ticket_update(payload: dict) -> list:
    """Story 5 AC 2, 3, 4. Returns a list of field-level error strings."""
    errors = []

    protected_keys_present = PROTECTED_FIELDS.intersection(payload.keys())
    if protected_keys_present:
        errors.append(
            f"the following fields are not editable after ticket creation: {sorted(protected_keys_present)}"
        )

    if "title" in payload and _blank(payload.get("title")):
        errors.append("title must not be blank")
    if "category" in payload and payload.get("category") not in ALLOWED_CATEGORIES:
        errors.append(f"category must be one of {sorted(ALLOWED_CATEGORIES)}")
    if "priority" in payload and payload.get("priority") not in ALLOWED_PRIORITIES:
        errors.append(f"priority must be one of {sorted(ALLOWED_PRIORITIES)}")
    if "status" in payload and payload.get("status") not in ALLOWED_STATUSES:
        errors.append(f"status must be one of {sorted(ALLOWED_STATUSES)}")

    return errors


# ---------------------------------------------------------------------------
# Error handling: always return JSON, never an HTML error page, so the
# frontend's fetch().json() call never chokes.
# ---------------------------------------------------------------------------
@app.errorhandler(Exception)
def handle_exception(err):
    logger.exception("Unhandled exception while processing request")
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    """Story 1: ticket list + create-ticket form."""
    return render_template(
        "index.html",
        categories=sorted(ALLOWED_CATEGORIES),
        statuses=sorted(ALLOWED_STATUSES),
        priorities=["Low", "Medium", "High"],
        submitted_by_options=sorted(ALLOWED_SUBMITTED_BY),
    )


@app.route("/ticket/<int:ticket_id>")
def ticket_detail_page(ticket_id):
    """Story 2 + Story 5: ticket detail, message history, edit form."""
    return render_template(
        "ticket.html",
        ticket_id=ticket_id,
        categories=sorted(ALLOWED_CATEGORIES),
        statuses=sorted(ALLOWED_STATUSES),
        priorities=["Low", "Medium", "High"],
    )


# ---------------------------------------------------------------------------
# API: tickets
# ---------------------------------------------------------------------------
@app.route("/api/tickets", methods=["GET"])
def list_tickets():
    """Story 1 AC 1-3: all persisted tickets, newest created first."""
    rows = lakebase.run_query(
        """
        SELECT ticket_id, title, category, status, priority, created_by,
               submitted_by, created_at
        FROM tickets
        ORDER BY created_at DESC
        """
    )
    return jsonify(rows)


@app.route("/api/tickets", methods=["POST"])
def create_ticket():
    """Story 3: create ticket + initial message in one flow."""
    payload = request.get_json(force=True, silent=True) or {}
    errors = validate_new_ticket(payload)
    if errors:
        return jsonify({"error": "Ticket could not be created", "details": errors}), 400

    ticket = lakebase.run_write_returning(
        """
        INSERT INTO tickets (title, category, status, priority, created_by, submitted_by)
        VALUES (%s, %s, 'open', %s, %s, %s)
        RETURNING ticket_id, title, category, status, priority, created_by, submitted_by, created_at
        """,
        (
            payload["title"].strip(),
            payload["category"],
            payload["priority"],
            CREATED_BY,
            payload["submitted_by"],
        ),
    )

    lakebase.run_write(
        """
        INSERT INTO ticket_messages (ticket_id, message_text, author)
        VALUES (%s, %s, %s)
        """,
        (ticket["ticket_id"], payload["message_text"].strip(), payload["author"].strip()),
    )

    return jsonify(ticket), 201


@app.route("/api/tickets/<int:ticket_id>", methods=["GET"])
def get_ticket(ticket_id):
    """Story 2: ticket detail."""
    rows = lakebase.run_query(
        """
        SELECT ticket_id, title, category, status, priority, created_by,
               submitted_by, created_at
        FROM tickets
        WHERE ticket_id = %s
        """,
        (ticket_id,),
    )
    if not rows:
        return jsonify({"error": f"Ticket {ticket_id} not found"}), 404
    return jsonify(rows[0])


@app.route("/api/tickets/<int:ticket_id>", methods=["PATCH"])
def update_ticket(ticket_id):
    """Story 5: edit title, category, priority, status only."""
    payload = request.get_json(force=True, silent=True) or {}
    errors = validate_ticket_update(payload)
    if errors:
        return jsonify({"error": "Ticket could not be updated", "details": errors}), 400

    editable_fields = {"title", "category", "priority", "status"}
    updates = {k: v for k, v in payload.items() if k in editable_fields}
    if not updates:
        return jsonify({"error": "No editable fields (title, category, priority, status) were provided"}), 400

    if "title" in updates:
        updates["title"] = updates["title"].strip()

    set_clause = ", ".join(f"{field} = %s" for field in updates)
    params = list(updates.values()) + [ticket_id]

    ticket = lakebase.run_write_returning(
        f"""
        UPDATE tickets
        SET {set_clause}
        WHERE ticket_id = %s
        RETURNING ticket_id, title, category, status, priority, created_by, submitted_by, created_at
        """,
        params,
    )
    if not ticket:
        return jsonify({"error": f"Ticket {ticket_id} not found"}), 404
    return jsonify(ticket)


# ---------------------------------------------------------------------------
# API: ticket messages
# ---------------------------------------------------------------------------
@app.route("/api/tickets/<int:ticket_id>/messages", methods=["GET"])
def list_messages(ticket_id):
    """Story 2 AC 2-3: messages for a ticket, oldest to newest."""
    ticket_rows = lakebase.run_query("SELECT ticket_id FROM tickets WHERE ticket_id = %s", (ticket_id,))
    if not ticket_rows:
        return jsonify({"error": f"Ticket {ticket_id} not found"}), 404

    rows = lakebase.run_query(
        """
        SELECT message_id, ticket_id, message_text, author, created_at
        FROM ticket_messages
        WHERE ticket_id = %s
        ORDER BY created_at ASC
        """,
        (ticket_id,),
    )
    return jsonify(rows)


@app.route("/api/tickets/<int:ticket_id>/messages", methods=["POST"])
def add_message(ticket_id):
    """Story 4: add a follow-up message to an existing ticket."""
    ticket_rows = lakebase.run_query("SELECT ticket_id FROM tickets WHERE ticket_id = %s", (ticket_id,))
    if not ticket_rows:
        return jsonify({"error": f"Ticket {ticket_id} not found"}), 404

    payload = request.get_json(force=True, silent=True) or {}
    errors = validate_new_message(payload)
    if errors:
        return jsonify({"error": "Message could not be added", "details": errors}), 400

    message = lakebase.run_write_returning(
        """
        INSERT INTO ticket_messages (ticket_id, message_text, author)
        VALUES (%s, %s, %s)
        RETURNING message_id, ticket_id, message_text, author, created_at
        """,
        (ticket_id, payload["message_text"].strip(), payload["author"].strip()),
    )
    return jsonify(message), 201


# ---------------------------------------------------------------------------
# API: weather (Assignment 2)
# ---------------------------------------------------------------------------
@app.route("/weather/sync", methods=["POST"])
def weather_sync():
    """Harvest NWS alerts + forecast narratives for given locations and
    upsert into weather_documents. Embedding happens separately via
    ingest_weather_embeddings.py."""
    payload = request.get_json(force=True, silent=True) or {}
    locations = payload.get("locations")
    limit = payload.get("limit", 50)

    if not isinstance(locations, list) or not locations or not all(isinstance(loc, str) and loc.strip() for loc in locations):
        return jsonify({"error": "locations must be a non-empty list of location strings"}), 400
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        return jsonify({"error": "limit must be an integer"}), 400
    limit = max(1, min(limit, 200))

    docs = weather_client.sync_locations(locations, limit=limit)

    synced = 0
    for doc in docs:
        lakebase.run_write(
            """
            INSERT INTO weather_documents
                (id, location, source_type, headline, narrative_text, issued_at, payload, synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (id) DO UPDATE SET
                location = EXCLUDED.location,
                headline = EXCLUDED.headline,
                narrative_text = EXCLUDED.narrative_text,
                issued_at = EXCLUDED.issued_at,
                payload = EXCLUDED.payload,
                synced_at = now()
            """,
            (
                doc["id"],
                doc["location"],
                doc["source_type"],
                doc["headline"],
                doc["narrative_text"],
                doc["issued_at"],
                __import__("json").dumps(doc["payload"], default=str),
            ),
        )
        synced += 1

    return jsonify({"synced": synced, "locations": locations}), 200


@app.route("/weather/search", methods=["POST"])
def weather_search():
    """Cosine-similarity semantic search over weather_embeddings."""
    payload = request.get_json(force=True, silent=True) or {}
    query = payload.get("query")
    top_k = payload.get("top_k", 5)

    if _blank(query):
        return jsonify({"error": "query must not be blank"}), 400
    try:
        top_k = int(top_k)
    except (TypeError, ValueError):
        return jsonify({"error": "top_k must be an integer"}), 400
    top_k = max(1, min(top_k, 20))

    count_rows = lakebase.run_query("SELECT count(*) AS n FROM weather_embeddings")
    if not count_rows or count_rows[0]["n"] == 0:
        return jsonify({
            "query": query,
            "results": [],
            "message": "No weather data has been embedded yet. Run POST /weather/sync, "
                       "then python ingest_weather_embeddings.py, then retry.",
        }), 200

    model = get_embedding_model()
    embedding = model.encode(query.strip()).tolist()
    vector_literal = _vector_literal(embedding)

    rows = lakebase.run_query(
        """
        SELECT d.id, d.location, d.headline, d.narrative_text, e.chunk_text,
               1 - (e.embedding <=> %s::vector) AS similarity
        FROM weather_embeddings e
        JOIN weather_documents d ON d.id = e.document_id
        ORDER BY e.embedding <=> %s::vector
        LIMIT %s
        """,
        (vector_literal, vector_literal, top_k),
    )
    return jsonify({"query": query, "results": rows}), 200


if __name__ == "__main__":
    ensure_schema()
    ensure_weather_schema()
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", 8000))
    app.run(debug=True, host=host, port=port)
