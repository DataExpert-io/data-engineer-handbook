"""
Batch embedding job for the weather pipeline (Assignment 2, Part 2).

Plain psycopg2 script -- deliberately NOT Spark (spark.write.jdbc is not
reliable against this Lakebase instance; see assignment spec). Mirrors
notebooks/ingest_ticker_news_embeddings.py's shape, but writes vectors in
one step via a %s::vector cast instead of a two-step array-then-UPDATE-cast.

What it does:
  1. Reads weather_documents rows that don't have embeddings yet
     (LEFT JOIN weather_embeddings, WHERE NULL).
  2. Chunks narrative_text with a sliding window (CHUNK_SIZE/CHUNK_OVERLAP).
  3. Embeds each chunk with sentence-transformers/all-MiniLM-L6-v2 (384-dim),
     the same model app.py's /weather/search loads for queries.
  4. Batch-writes rows into weather_embeddings via execute_values, casting
     each embedding to vector(384) with %s::vector.
     ON CONFLICT (document_id, chunk_index) DO UPDATE keeps re-runs safe.

Run after POST /weather/sync has populated weather_documents:
    python ingest_weather_embeddings.py
"""

import logging

from psycopg2.extras import execute_values

import lakebase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingest_weather_embeddings")

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

# Most NWS alert/forecast text is short; chunking mainly matters for
# combined alert description+instruction blocks that run long.
CHUNK_SIZE = 800
CHUNK_OVERLAP = 100

BATCH_SIZE = 64  # documents' chunks embedded per model.encode() call


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list:
    """Sliding-window character chunker. Returns a list of chunk strings
    (always at least one, even if text is shorter than chunk_size)."""
    text = text.strip()
    if not text:
        return []
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    step = max(chunk_size - overlap, 1)
    while start < len(text):
        chunk = text[start:start + chunk_size].strip()
        if chunk:
            chunks.append(chunk)
        if start + chunk_size >= len(text):
            break
        start += step
    return chunks


def fetch_unembedded_documents() -> list:
    """weather_documents rows with zero rows in weather_embeddings."""
    return lakebase.run_query(
        """
        SELECT d.id, d.narrative_text
        FROM weather_documents d
        WHERE NOT EXISTS (
            SELECT 1 FROM weather_embeddings e WHERE e.document_id = d.id
        )
        ORDER BY d.synced_at ASC
        """
    )


def embed_and_write(model, documents: list) -> int:
    """Chunk + embed each document, batch-insert into weather_embeddings.
    Returns total chunk rows written."""
    # Build a flat list of (document_id, chunk_index, chunk_text) first so we
    # can embed in one batched model.encode() call per outer batch instead of
    # per-document (much faster on CPU).
    flat_chunks = []
    for doc in documents:
        chunks = chunk_text(doc["narrative_text"])
        for idx, chunk in enumerate(chunks):
            flat_chunks.append((doc["id"], idx, chunk))

    if not flat_chunks:
        return 0

    total_written = 0
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            for batch_start in range(0, len(flat_chunks), BATCH_SIZE):
                batch = flat_chunks[batch_start:batch_start + BATCH_SIZE]
                texts = [c[2] for c in batch]
                embeddings = model.encode(texts, show_progress_bar=False)

                values = [
                    (
                        doc_id,
                        chunk_index,
                        chunk_txt,
                        "[" + ",".join(f"{v:.8f}" for v in vector) + "]",
                        MODEL_NAME,
                    )
                    for (doc_id, chunk_index, chunk_txt), vector in zip(batch, embeddings)
                ]

                execute_values(
                    cur,
                    """
                    INSERT INTO weather_embeddings
                        (document_id, chunk_index, chunk_text, embedding, model_name)
                    VALUES %s
                    ON CONFLICT (document_id, chunk_index) DO UPDATE SET
                        chunk_text = EXCLUDED.chunk_text,
                        embedding = EXCLUDED.embedding,
                        model_name = EXCLUDED.model_name,
                        created_at = now()
                    """,
                    values,
                    template="(%s, %s, %s, %s::vector, %s)",
                )
                conn.commit()
                total_written += len(values)
                logger.info("Wrote %d/%d chunk embeddings", total_written, len(flat_chunks))

    return total_written


def main():
    from sentence_transformers import SentenceTransformer

    documents = fetch_unembedded_documents()
    logger.info("Found %d unembedded weather_documents rows", len(documents))
    if not documents:
        logger.info("Nothing to embed. Run POST /weather/sync first if you expected rows here.")
        return

    logger.info("Loading embedding model %s ...", MODEL_NAME)
    model = SentenceTransformer(MODEL_NAME)

    written = embed_and_write(model, documents)
    logger.info("Done. Wrote %d chunk embeddings across %d documents.", written, len(documents))


if __name__ == "__main__":
    main()
