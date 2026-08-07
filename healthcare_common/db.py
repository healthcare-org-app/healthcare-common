"""Postgres connection pool + JSONB-record helpers.

Every service owns its own database. This module gives you:

    pool = db_pool(dsn)
    row = pool.query_one("SELECT data FROM patients WHERE id=%s", (pid,))
    pool.execute("INSERT INTO patients(id, data) VALUES(%s, %s)", (pid, Json(rec)))

DSN default: reads DATABASE_URL. Service names inherit their DB from
`DATABASE_URL` (e.g. `postgresql://healthcare:...@postgres:5432/patients`).
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Iterator, Optional

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

log = logging.getLogger("healthcare_common.db")


class DBPool:
    """Thin wrapper on `psycopg_pool.ConnectionPool` with query helpers."""

    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 10) -> None:
        self.dsn = dsn
        self._pool = ConnectionPool(
            conninfo=dsn,
            min_size=min_size,
            max_size=max_size,
            kwargs={"row_factory": dict_row, "autocommit": True},
            open=True,
        )

    @contextmanager
    def connection(self) -> Iterator[Connection]:
        with self._pool.connection() as conn:
            yield conn

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if cur.description is None:
                    return []
                return list(cur.fetchall())

    def query_one(self, sql: str, params: tuple = ()) -> Optional[dict]:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: tuple = ()) -> int:
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.rowcount

    def close(self) -> None:
        self._pool.close()


def db_pool(dsn: Optional[str] = None) -> DBPool:
    """Build a pool from the given DSN or DATABASE_URL."""
    dsn = dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")
    return DBPool(dsn)
