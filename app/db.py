"""SQLite 持久层：WAL 模式，短事务，所有关键状态变更在事务内完成。"""
from __future__ import annotations

import sqlite3
import threading
import time
from typing import Any, Iterable

from . import config

_local = threading.local()

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA busy_timeout=30000;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL,
    published_at    TEXT,
    row_count       INTEGER NOT NULL DEFAULT 0,
    current_path    TEXT,
    -- rewriting：撤回正在重写本批，禁止下载；published：可下载
    status          TEXT NOT NULL DEFAULT 'published'
                    CHECK (status IN ('published', 'rewriting', 'failed'))
);

CREATE TABLE IF NOT EXISTS batch_subjects (
    batch_id    INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    subject_id  TEXT NOT NULL,
    PRIMARY KEY (batch_id, subject_id)
);
CREATE INDEX IF NOT EXISTS idx_batch_subjects_subject ON batch_subjects(subject_id);

CREATE TABLE IF NOT EXISTS imports (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'staging'
                    CHECK (status IN ('staging', 'published', 'rejected')),
    path            TEXT NOT NULL,
    row_count       INTEGER NOT NULL,
    batch_id        INTEGER REFERENCES batches(id) ON DELETE SET NULL,
    note            TEXT
);
CREATE INDEX IF NOT EXISTS idx_imports_status ON imports(status);

CREATE TABLE IF NOT EXISTS withdrawals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id      TEXT NOT NULL UNIQUE,
    requested_at    TEXT NOT NULL,
    -- accepted：已受理（此后永久拒收）；processing：清理中；completed：全部完成
    status          TEXT NOT NULL
                    CHECK (status IN ('accepted', 'processing', 'completed', 'failed')),
    batches_total   INTEGER NOT NULL DEFAULT 0,
    batches_done    INTEGER NOT NULL DEFAULT 0,
    files_removed   INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS withdrawal_batches (
    withdrawal_id   INTEGER NOT NULL REFERENCES withdrawals(id) ON DELETE CASCADE,
    batch_id        INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'rewriting', 'done')),
    PRIMARY KEY (withdrawal_id, batch_id)
);
CREATE INDEX IF NOT EXISTS idx_wb_state ON withdrawal_batches(state);
"""


def get_conn() -> sqlite3.Connection:
    """每个线程一个连接（worker 线程与请求线程各自独立）。"""
    conn = getattr(_local, "conn", None)
    if conn is None:
        config.ensure_dirs()
        conn = sqlite3.connect(config.DB_PATH, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        _apply_pragmas(conn)
        _local.conn = conn
    return conn


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    # 两个实例可能同时首次打开数据库：journal_mode=WAL 的切换需要排他锁，
    # 撞锁时退避重试，避免启动失败。
    for attempt in range(30):
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            break
        except sqlite3.OperationalError:
            if attempt == 29:
                raise
            time.sleep(0.2)
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")


def init_db() -> None:
    conn = get_conn()
    # DDL 也可能与另一实例的初始化撞锁，重试
    for attempt in range(30):
        try:
            conn.executescript(SCHEMA)
            return
        except sqlite3.OperationalError:
            if attempt == 29:
                raise
            time.sleep(0.2)


def query(sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    return get_conn().execute(sql, tuple(params)).fetchall()


def query_one(sql: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
    return get_conn().execute(sql, tuple(params)).fetchone()
