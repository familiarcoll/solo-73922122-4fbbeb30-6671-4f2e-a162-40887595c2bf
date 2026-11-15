"""SQLite connection handling and schema management.

The database is the shared coordination point between the two API instances.
It stores only indexes and status; feature values live in CSV files on the
shared volume.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS samples (
    row_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id    TEXT NOT NULL,
    sample_id     TEXT NOT NULL UNIQUE,
    shard_id      INTEGER NOT NULL,
    source_file   TEXT NOT NULL,
    source_row_no INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_samples_subject ON samples(subject_id);
CREATE INDEX IF NOT EXISTS idx_samples_shard ON samples(shard_id, row_id);

CREATE TABLE IF NOT EXISTS shards (
    shard_id        INTEGER PRIMARY KEY,
    status          TEXT NOT NULL CHECK (status IN ('AVAILABLE', 'WITHHELD')),
    total_rows      INTEGER NOT NULL,
    current_file    TEXT NOT NULL,
    staging_file    TEXT,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS withdrawals (
    task_id          TEXT PRIMARY KEY,
    subject_id       TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')),
    affected_shards  INTEGER NOT NULL,
    affected_rows    INTEGER NOT NULL,
    purged_rows      INTEGER NOT NULL DEFAULT 0,
    source_purged    INTEGER NOT NULL DEFAULT 0,
    error            TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_withdrawals_subject ON withdrawals(subject_id);

CREATE TABLE IF NOT EXISTS withdrawal_shards (
    task_id   TEXT NOT NULL REFERENCES withdrawals(task_id),
    shard_id  INTEGER NOT NULL,
    state     TEXT NOT NULL CHECK (state IN ('PENDING', 'RUNNING', 'DONE')),
    purged_rows_in_shard INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (task_id, shard_id)
);
CREATE INDEX IF NOT EXISTS idx_ws_shard ON withdrawal_shards(shard_id);

CREATE TABLE IF NOT EXISTS shard_leases (
    shard_id    INTEGER PRIMARY KEY,
    owner       TEXT NOT NULL,
    task_id     TEXT NOT NULL,
    leased_at   TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS withdrawal_sources (
    task_id      TEXT NOT NULL REFERENCES withdrawals(task_id),
    source_file  TEXT NOT NULL,
    purged       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (task_id, source_file)
);
"""


def connect() -> sqlite3.Connection:
    config.ensure_dirs()
    conn = sqlite3.connect(
        config.DB_PATH,
        timeout=30.0,
        isolation_level=None,  # explicit BEGIN/COMMIT
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


@contextmanager
def tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Serialised write transaction (database-level lock)."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
