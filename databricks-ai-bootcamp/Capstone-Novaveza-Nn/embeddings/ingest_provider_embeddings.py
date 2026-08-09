"""
Provider embeddings ingest — Phase 2 of the capstone pipeline.

Reads every row from the `providers` table (populated by the Phase 1 Spark
pipeline), builds one short searchable text description per provider,
embeds it with sentence-transformers, and writes it into the
`provider_embeddings` pgvector table.

Same shape as Assignment 2's ingest_weather_embeddings.py — one embedding
per row is enough here since provider descriptions are short (no sliding-
window chunking needed, unlike the weather narrative text).

Run locally:
    pip install -r requirements.txt
    python3 ingest_provider_embeddings.py

Requires LAKEBASE_URL in a local .env file (same connection string used in
the Databricks notebooks and Assignment 2's app).
"""

import logging
import os

import psycopg2
from psycopg2.extras import execute_values
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingest_provider_embeddings")

EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384


def get_lakebase_url() -> str:
    url = os.getenv("LAKEBASE_URL")
    if not url:
        # Fall back to reading a local .env file directly, same pattern as
        # Assignment 2, in case python-dotenv isn't installed/loaded.
        try:
            with open(".env") as f:
                for line in f:
                    if line.strip().startswith("LAKEBASE_URL="):
                        url = line.strip().split("=", 1)[1].strip().strip('"').strip("'")
                        break
        except FileNotFoundError:
            pass
    if not url:
        raise RuntimeError(
            "LAKEBASE_URL not found in environment or .env file. Copy the "
            "same connection string you used for Assignment 2 into a .env "
            "file in this folder: LAKEBASE_URL=postgresql://..."
        )
    return url


def build_provider_text(row: dict) -> str:
    """One short, searchable sentence per provider — combines what someone
    would actually search for: specialty, name/credential, and location."""
    parts = []
    if row["taxonomy_desc"]:
        parts.append(row["taxonomy_desc"])
    if row["display_name"]:
        name = row["display_name"]
        if row["credential"]:
            name = f"{name}, {row['credential']}"
        parts.append(name)
    location_bits = [b for b in [row["city"], row["state"], row["zip5"]] if b]
    if location_bits:
        parts.append("located in " + ", ".join(location_bits))
    return " — ".join(parts) if parts else "Unknown provider"


def fetch_providers(conn) -> list:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT npi, display_name, credential, taxonomy_code, taxonomy_desc,
                   city, state, zip5
            FROM providers
        """)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def embed_and_write(conn, model, providers: list):
    texts = [build_provider_text(p) for p in providers]
    logger.info("Embedding %d provider descriptions...", len(texts))
    embeddings = model.encode(texts, show_progress_bar=True, batch_size=64)

    rows = [
        (
            providers[i]["npi"],
            texts[i],
            embeddings[i].tolist(),
            EMBEDDING_MODEL_NAME,
        )
        for i in range(len(providers))
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO provider_embeddings (npi, chunk_text, embedding, model_name)
            VALUES %s
            ON CONFLICT (npi) DO UPDATE SET
                chunk_text = EXCLUDED.chunk_text,
                embedding = EXCLUDED.embedding,
                model_name = EXCLUDED.model_name,
                created_at = now()
            """,
            rows,
            template="(%s, %s, %s::vector, %s)",
        )
    conn.commit()
    logger.info("Wrote %d/%d provider embeddings", len(rows), len(providers))


def main():
    lakebase_url = get_lakebase_url()
    logger.info("Connecting to Lakebase...")
    conn = psycopg2.connect(lakebase_url)

    try:
        providers = fetch_providers(conn)
        logger.info("Fetched %d providers", len(providers))
        if not providers:
            logger.warning("No providers found — run the Phase 1 pipeline first.")
            return

        logger.info("Loading embedding model: %s", EMBEDDING_MODEL_NAME)
        model = SentenceTransformer(EMBEDDING_MODEL_NAME)

        embed_and_write(conn, model, providers)
        logger.info("Done.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
