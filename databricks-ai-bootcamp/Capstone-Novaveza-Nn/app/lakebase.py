"""
Lakebase (Postgres) connection helper — same interface used across the
ticket tracker, the weather app, and this capstone app.
"""

import os

import psycopg2
import psycopg2.extras


def _get_url() -> str:
    url = os.getenv("LAKEBASE_URL")
    if not url:
        raise RuntimeError("LAKEBASE_URL is not set")
    return url


def get_connection():
    return psycopg2.connect(_get_url())


def run_query(sql, params=None):
    """SELECT helper — returns a list of dicts."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()
    finally:
        conn.close()


def run_write(sql, params=None):
    """INSERT/UPDATE/DELETE helper — no return value."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
        conn.commit()
    finally:
        conn.close()


def run_write_returning(sql, params=None):
    """INSERT ... RETURNING helper — returns the single resulting row as a dict."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
        conn.commit()
        return row
    finally:
        conn.close()
