"""
Lakebase (Databricks-managed Postgres) connection helper.

Connects using a single LAKEBASE_URL (a standard Postgres connection URL,
e.g. postgresql://role:password@host:5432/databricks_postgres?sslmode=require)
pointing at a native Postgres role with a static, non-expiring password.

In production on Databricks Apps, LAKEBASE_URL is resolved from the
Databricks secret scope named by LAKEBASE_SECRET_SCOPE / LAKEBASE_SECRET_KEY
(see app.yaml). For local development, set LAKEBASE_URL directly in .env.
"""

import base64
import os
from contextlib import contextmanager

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

# Loads .env into os.environ for local development. On Databricks Apps,
# there is no .env file, so this is a no-op there -- LAKEBASE_URL simply
# won't be set, and _lakebase_url() falls through to the secret scope.
load_dotenv()

_SCOPE = os.environ.get("LAKEBASE_SECRET_SCOPE", "database")
_KEY = os.environ.get("LAKEBASE_SECRET_KEY", "lakebase-url")


def _lakebase_url() -> str:
    """Resolve the Lakebase connection URL.

    Local dev: read LAKEBASE_URL directly from the environment (.env).
    Deployed on Databricks Apps: fetch and decode it from the Databricks
    secret scope instead, so the real credential never lives in source
    control or app.yaml.
    """
    env_url = os.environ.get("LAKEBASE_URL")
    if env_url:
        return env_url

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    secret = w.secrets.get_secret(scope=_SCOPE, key=_KEY)
    return base64.b64decode(secret.value).decode("utf-8")


@contextmanager
def get_connection():
    """Yield a raw psycopg2 connection with a RealDictCursor factory."""
    conn = psycopg2.connect(_lakebase_url(), cursor_factory=RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()


def run_query(sql: str, params=None) -> list:
    """Run a read query against Lakebase and return rows as list[dict]."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def run_write(sql: str, params=None) -> int:
    """Run an INSERT/UPDATE/DELETE against Lakebase, return affected row count."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount


def run_write_returning(sql: str, params=None) -> dict:
    """Run an INSERT/UPDATE with a RETURNING clause, return the single resulting row."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            conn.commit()
            return row
