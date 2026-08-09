"""
Provider data adapter — all Lakebase/psycopg2 calls and query logic live
here, kept out of the MCP tool functions themselves (same separation
required by the assignment brief and already used in Assignment 3's
weather_broker.py).

Two operations:
  - search_providers: semantic search over provider_embeddings (retrieve)
  - confirm_provider: write a confirmation/flag row for a provider (write)
"""

import logging
import os
from typing import Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger("provider_broker")

EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

_embedding_model = None


class ProviderLookupError(Exception):
    """Raised for any user-facing provider lookup/write failure (bad NPI,
    DB error, etc.). Tool functions catch this and return a clean
    {"error": "..."} dict instead of letting a stack trace reach the agent."""


def _get_lakebase_url() -> str:
    url = os.getenv("LAKEBASE_URL")
    if not url:
        raise ProviderLookupError("LAKEBASE_URL is not configured on this server.")
    return url


def _get_connection():
    try:
        return psycopg2.connect(_get_lakebase_url())
    except psycopg2.OperationalError as exc:
        raise ProviderLookupError(f"Could not connect to the provider database: {exc}") from exc


def _get_embedding_model():
    global _embedding_model
    if _embedding_model is None:
        from sentence_transformers import SentenceTransformer
        _embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    return _embedding_model


def _vector_literal(embedding) -> str:
    return "[" + ",".join(str(float(x)) for x in embedding) + "]"


def search_providers(query: str, limit: int = 10) -> list:
    """Semantic search over provider descriptions (specialty + name + location).
    Returns a list of dicts: npi, display_name, credential, taxonomy_desc,
    city, state, zip5, similarity."""
    if not query or not query.strip():
        raise ProviderLookupError("Search query cannot be blank.")

    limit = max(1, min(limit, 20))
    model = _get_embedding_model()
    vector_literal = _vector_literal(model.encode(query.strip()).tolist())

    conn = _get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT p.npi, p.display_name, p.credential, p.taxonomy_desc,
                       p.city, p.state, p.zip5,
                       1 - (pe.embedding <=> %s::vector) AS similarity
                FROM provider_embeddings pe
                JOIN providers p ON p.npi = pe.npi
                ORDER BY pe.embedding <=> %s::vector
                LIMIT %s
                """,
                (vector_literal, vector_literal, limit),
            )
            rows = cur.fetchall()
    except psycopg2.Error as exc:
        raise ProviderLookupError(f"Search failed: {exc}") from exc
    finally:
        conn.close()

    return [dict(row) for row in rows]


def confirm_provider(npi: str, status: str, note: Optional[str] = None) -> dict:
    """Write a confirmation or flag for a provider's info. status must be
    'confirmed_current' or 'flagged_outdated'. Returns the provider's display
    name and the confirmation that was recorded, or raises ProviderLookupError
    if the NPI doesn't exist or status is invalid."""
    if status not in ("confirmed_current", "flagged_outdated"):
        raise ProviderLookupError(
            f"Invalid status '{status}'. Must be 'confirmed_current' or 'flagged_outdated'."
        )

    conn = _get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT display_name FROM providers WHERE npi = %s", (npi,))
            provider = cur.fetchone()
            if provider is None:
                raise ProviderLookupError(f"No provider found with NPI '{npi}'.")

            cur.execute(
                """
                INSERT INTO provider_confirmations (npi, status, note, confirmed_by)
                VALUES (%s, %s, %s, 'agent')
                RETURNING id, confirmed_at
                """,
                (npi, status, note),
            )
            confirmation = cur.fetchone()
        conn.commit()
    except psycopg2.Error as exc:
        raise ProviderLookupError(f"Could not record confirmation: {exc}") from exc
    finally:
        conn.close()

    return {
        "npi": npi,
        "display_name": provider["display_name"],
        "status": status,
        "note": note,
        "confirmation_id": confirmation["id"],
        "confirmed_at": str(confirmation["confirmed_at"]),
    }
