"""
utils/db.py
-----------
Neon PostgreSQL connection pool and query helpers.
Loads DATABASE_URL from environment via python-dotenv.
"""

import os
import logging
from contextlib import contextmanager

import psycopg2
import psycopg2.pool
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

_pool: psycopg2.pool.SimpleConnectionPool | None = None


def _get_pool() -> psycopg2.pool.SimpleConnectionPool:
    """Lazily initialize and return the connection pool."""
    global _pool
    if _pool is None:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL environment variable is not set")
        try:
            _pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=database_url,
            )
            log.info("PostgreSQL connection pool initialized (minconn=1, maxconn=10)")
        except Exception as exc:
            log.error("Failed to initialize PostgreSQL connection pool: %s", exc)
            raise
    return _pool


@contextmanager
def get_conn():
    """
    Context manager that borrows a connection from the pool and returns it
    when the block exits.

    Usage:
        with get_conn() as conn:
            cur = conn.cursor()
            ...
    """
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def execute_query(sql: str, params=None) -> list[dict]:
    """
    Execute a SELECT statement and return a list of dicts (one per row).
    Column names come from the cursor description.
    """
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                return [dict(row) for row in rows]
    except Exception as exc:
        log.error("execute_query failed: %s | sql=%s", exc, sql[:200])
        raise


def execute_write(sql: str, params=None) -> None:
    """
    Execute an INSERT / UPDATE / DELETE statement and commit.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()
    except Exception as exc:
        log.error("execute_write failed: %s | sql=%s", exc, sql[:200])
        raise


def execute_many(sql: str, rows: list) -> None:
    """
    Bulk-execute sql with a list of row tuples (e.g. for upserts).
    Uses executemany and commits once at the end.
    """
    if not rows:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
    except Exception as exc:
        log.error("execute_many failed: %s | sql=%s rows=%d", exc, sql[:200], len(rows))
        raise
