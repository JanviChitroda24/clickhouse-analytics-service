"""
SQL Server client wrapper using pymssql.

Singleton connection per target database. Used for setup, benchmarks, and
row-store comparison against ClickHouse (columnar) and ElasticSearch (search).
"""

from __future__ import annotations

import logging

import pymssql

from src.config import get_settings

logger = logging.getLogger(__name__)

_conn: pymssql.Connection | None = None
_conn_database: str | None = None


def get_sqlserver_conn(database: str | None = None) -> pymssql.Connection:
    """
    Return a cached SQL Server connection.

    Reconnects if the target database changes or the connection is stale.
    """
    global _conn, _conn_database
    settings = get_settings()
    target_db = database or settings.sqlserver_database

    if _conn is not None and _conn_database == target_db:
        try:
            cursor = _conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return _conn
        except Exception:
            close_sqlserver()

    if _conn is not None:
        _conn.close()
        _conn = None

    _conn = pymssql.connect(
        server=settings.sqlserver_host,
        port=settings.sqlserver_port,
        user=settings.sqlserver_user,
        password=settings.sqlserver_password,
        database=target_db,
        login_timeout=10,
        autocommit=True,
    )
    _conn_database = target_db
    logger.info(
        "Connected to SQL Server at %s:%s (database=%s)",
        settings.sqlserver_host,
        settings.sqlserver_port,
        target_db,
    )
    return _conn


def execute_query(sql: str, database: str | None = None) -> list[tuple]:
    """Execute a SELECT and return all rows."""
    conn = get_sqlserver_conn(database)
    cursor = conn.cursor()
    cursor.execute(sql)
    try:
        return cursor.fetchall()
    finally:
        cursor.close()


def execute_command(sql: str, database: str | None = None) -> None:
    """Execute DDL/DML with no result set."""
    conn = get_sqlserver_conn(database)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    finally:
        cursor.close()


def close_sqlserver() -> None:
    """Close the cached connection (e.g. on app shutdown)."""
    global _conn, _conn_database
    if _conn is not None:
        _conn.close()
        _conn = None
        _conn_database = None
