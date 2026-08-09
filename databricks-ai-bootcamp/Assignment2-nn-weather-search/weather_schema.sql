-- Weather Intelligence pipeline schema (Assignment 2).
-- Adds two tables on top of Assignment 1's Lakebase instance:
--   weather_documents  -- raw harvested alert/forecast text
--   weather_embeddings -- chunked + embedded vectors for semantic search
--
-- Run this once via the Lakebase SQL editor (or psql against LAKEBASE_URL)
-- before syncing/embedding. app.py's ensure_weather_schema() also runs this
-- same CREATE TABLE IF NOT EXISTS logic at startup as a safety net.

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

-- HNSW index for cosine-distance ANN search (pgvector >= 0.5.0).
CREATE INDEX IF NOT EXISTS idx_weather_embeddings_embedding_hnsw
    ON weather_embeddings USING hnsw (embedding vector_cosine_ops);
