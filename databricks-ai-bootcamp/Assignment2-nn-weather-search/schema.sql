-- Novanega Feedback & Feature Request Tracker
-- Lakebase (Postgres) schema
-- Matches PRD v3 data model + business rules.
--
-- Run this once via the Lakebase SQL editor (or psql against your
-- LAKEBASE_URL) before deploying the app. app.py also runs this same
-- CREATE TABLE IF NOT EXISTS logic at startup as a safety net, but running
-- it here first is what gives you the "Lakebase tables" screenshot for
-- submission.

CREATE TABLE IF NOT EXISTS tickets (
    ticket_id     BIGSERIAL PRIMARY KEY,
    title         TEXT NOT NULL,
    category      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'open',
    priority      TEXT NOT NULL,
    created_by    TEXT NOT NULL DEFAULT 'novanega-app',
    submitted_by  TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Business rule 17: blank text fields are rejected explicitly.
    CONSTRAINT tickets_title_not_blank CHECK (length(trim(title)) > 0),

    -- Business rules 4-6: fixed enums, explicit allowed-value sets.
    CONSTRAINT tickets_category_allowed CHECK (
        category IN ('Feature Request', 'Bug', 'Feedback', 'Question')
    ),
    CONSTRAINT tickets_status_allowed CHECK (
        status IN ('open', 'in_progress', 'resolved')
    ),
    CONSTRAINT tickets_priority_allowed CHECK (
        priority IN ('Low', 'Medium', 'High')
    ),
    CONSTRAINT tickets_submitted_by_allowed CHECK (
        submitted_by IN ('provider', 'pilot_user', 'internal')
    )
);

CREATE TABLE IF NOT EXISTS ticket_messages (
    message_id    BIGSERIAL PRIMARY KEY,
    ticket_id     BIGINT NOT NULL REFERENCES tickets(ticket_id) ON DELETE CASCADE,
    message_text  TEXT NOT NULL,
    author        TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Business rule 17: blank message_text and blank author are both
    -- rejected explicitly (Story 3 AC5, Story 4 AC4).
    CONSTRAINT ticket_messages_text_not_blank CHECK (length(trim(message_text)) > 0),
    CONSTRAINT ticket_messages_author_not_blank CHECK (length(trim(author)) > 0)
);

CREATE INDEX IF NOT EXISTS idx_tickets_created_at ON tickets (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ticket_messages_ticket_id ON ticket_messages (ticket_id);
CREATE INDEX IF NOT EXISTS idx_ticket_messages_created_at ON ticket_messages (ticket_id, created_at ASC);

-- Assignment 2: Weather Intelligence pipeline. See weather_schema.sql for
-- the full commented version; included again here so a single schema.sql
-- run sets up everything needed for both assignments.
CREATE EXTENSION IF NOT EXISTS vector;

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
);

CREATE TABLE IF NOT EXISTS weather_embeddings (
    id            BIGSERIAL PRIMARY KEY,
    document_id   TEXT NOT NULL REFERENCES weather_documents(id) ON DELETE CASCADE,
    chunk_index   INT NOT NULL,
    chunk_text    TEXT NOT NULL,
    embedding     vector(384) NOT NULL,
    model_name    TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT weather_embeddings_doc_chunk_unique UNIQUE (document_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_weather_documents_source_type ON weather_documents (source_type);
CREATE INDEX IF NOT EXISTS idx_weather_embeddings_document_id ON weather_embeddings (document_id);
CREATE INDEX IF NOT EXISTS idx_weather_embeddings_embedding_hnsw
    ON weather_embeddings USING hnsw (embedding vector_cosine_ops);
