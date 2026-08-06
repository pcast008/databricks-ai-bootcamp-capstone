"""
Lakebase (Databricks-managed Postgres) connection helper.

Connects using a single LAKEBASE_URL (a standard Postgres connection URL,
e.g. postgresql://role:password@host:5432/databricks_postgres?sslmode=require)
pointing at a native Postgres role with a static, non-expiring password.
This keeps setup to a single secret instead of five separate env vars.

Connections are served from a process-wide pool so the Flask app and the
ingestion notebooks share one bounded set of Lakebase connections instead of
opening a fresh socket per query.
"""

import base64
import os
import threading
from contextlib import contextmanager
from functools import lru_cache

from databricks.sdk import WorkspaceClient
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

_w = WorkspaceClient()

_SCOPE = os.environ.get("LAKEBASE_SECRET_SCOPE", "database")
_KEY = os.environ.get("LAKEBASE_SECRET_KEY", "lakebase-url")

_MIN_CONN = int(os.environ.get("LAKEBASE_MIN_CONN", 1))
_MAX_CONN = int(os.environ.get("LAKEBASE_MAX_CONN", 8))

_pool: ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


@lru_cache(maxsize=1)
def _lakebase_url() -> str:
    """Resolve the Lakebase connection URL.

    Prefers a LAKEBASE_URL environment variable (that's the local-dev path the
    README and .env.example describe) and otherwise reads the Databricks secret
    scope, which is how it resolves when deployed as a Databricks App.

    Cached for the life of the process: the URL points at a native Postgres role
    with a static, non-expiring password, so there is nothing to refresh. Without
    the cache every single query costs an extra Databricks SDK round-trip.
    """
    url = os.environ.get("LAKEBASE_URL")
    if url:
        return url
    secret = _w.secrets.get_secret(scope=_SCOPE, key=_KEY)
    return base64.b64decode(secret.value).decode("utf-8")


def _get_pool() -> ThreadedConnectionPool:
    """Lazily build the process-wide connection pool (double-checked locking)."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = ThreadedConnectionPool(
                    _MIN_CONN,
                    _MAX_CONN,
                    dsn=_lakebase_url(),
                    cursor_factory=RealDictCursor,
                )
    return _pool


@contextmanager
def get_connection():
    """Yield a pooled psycopg2 connection with a RealDictCursor factory.

    The connection is returned to the pool (not closed) on exit. psycopg2's pool
    rolls back any connection handed back mid-transaction, so a caller that
    forgets to commit can't leave a poisoned connection behind for the next one.
    """
    pool = _get_pool()
    conn = pool.getconn()
    # Lakebase may reap idle server-side connections; swap out a dead one.
    if conn.closed:
        pool.putconn(conn, close=True)
        conn = pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def run_query(sql: str, params: tuple | dict | None = None) -> list[dict]:
    """Run a read query against Lakebase and return rows as list[dict]."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def run_write(sql: str, params: tuple | dict | None = None) -> int:
    """Run an INSERT/UPDATE/DELETE against Lakebase, return affected row count."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount
