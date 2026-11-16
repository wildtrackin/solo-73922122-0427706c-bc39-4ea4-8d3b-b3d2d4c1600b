"""业务服务层：导入发布、撤回受理与清理、崩溃恢复。

锁约定：
- PUBLISH_LOCK 串行化"导入发布"与"撤回受理"，保证受理时刻的全序关系；
- WITHDRAW_LOCK 保证同一时刻只有一个实例在执行清理；
- 下载只依赖 SQLite 中批次的 status，无需互斥锁。
"""
from __future__ import annotations

import datetime as dt
import os
import uuid
from pathlib import Path

import pyarrow as pa

from . import config, db
from .locks import file_lock
from .parquet_store import (
    atomic_write_parquet,
    distinct_subjects,
    list_parquet_files,
    parquet_contains_subject,
    rewrite_without_subject,
)


class ServiceError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def utcnow() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------- 导入发布

def _subject_is_withdrawn(conn, subject_ids: list[str]) -> str | None:
    placeholders = ",".join("?" * len(subject_ids))
    row = conn.execute(
        f"SELECT subject_id FROM withdrawals WHERE subject_id IN ({placeholders}) LIMIT 1",
        subject_ids,
    ).fetchone()
    return row[0] if row else None


def publish_import(table: pa.Table) -> dict:
    """在 PUBLISH_LOCK 下原子发布一批数据，返回批次信息。"""
    subjects = distinct_subjects(table)
    conn = db.get_conn()

    with file_lock(config.PUBLISH_LOCK):
        # 受理后永久拒收：含已撤回受试者的整批一律拒绝
        withdrawn = _subject_is_withdrawn(conn, subjects)
        if withdrawn is not None:
            raise ServiceError(
                f"整批拒绝：受试者 {withdrawn} 已撤回，任何包含该人的批次均不得导入",
                status_code=409,
            )

        now = utcnow()
        # 1. 先落暂存 Parquet（CSV 从不落盘，落盘的只有 Parquet）
        staging_name = f"import-{uuid.uuid4().hex}.parquet"
        staging_path = config.STAGING_DIR / staging_name
        atomic_write_parquet(table, staging_path)
        row_count = table.num_rows

        try:
            conn.execute("BEGIN IMMEDIATE")
            # 2. 登记暂存导入
            cur = conn.execute(
                "INSERT INTO imports (created_at, status, path, row_count, note) "
                "VALUES (?, 'staging', ?, ?, NULL)",
                (now, str(staging_path), row_count),
            )
            import_id = cur.lastrowid
            # 3. 建立批次
            cur = conn.execute(
                "INSERT INTO batches (created_at, row_count, status) VALUES (?, ?, 'published')",
                (now, row_count),
            )
            batch_id = cur.lastrowid
            # 4. 原子移入批次目录（版本号从 v1 起；先建目录再 rename）
            batch_dir = config.BATCHES_DIR / f"batch-{batch_id:08d}"
            batch_dir.mkdir(parents=True, exist_ok=True)
            final_path = batch_dir / "v1.parquet"
            os.replace(staging_path, final_path)
            # 5. 回填路径与受试者索引
            conn.execute(
                "UPDATE batches SET current_path=?, published_at=? WHERE id=?",
                (str(final_path), now, batch_id),
            )
            conn.execute(
                "UPDATE imports SET status='published', batch_id=? WHERE id=?",
                (batch_id, import_id),
            )
            conn.executemany(
                "INSERT OR IGNORE INTO batch_subjects (batch_id, subject_id) VALUES (?, ?)",
                [(batch_id, s) for s in subjects],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            # 暂存文件可能已被 rename 走；清理仍存在的残留
            if staging_path.exists():
                staging_path.unlink()
            raise

    return {
        "batch_id": batch_id,
        "import_id": import_id,
        "row_count": row_count,
        "subjects": subjects,
        "path": str(final_path),
    }


# ---------------------------------------------------------------- 撤回受理

def accept_withdrawal(subject_id: str) -> dict:
    """受理撤回：在 PUBLISH_LOCK 下登记，与导入发布构成全序。

    重复撤回返回既有任务（接续原任务，不会新建）。
    """
    conn = db.get_conn()
    with file_lock(config.PUBLISH_LOCK):
        existing = conn.execute(
            "SELECT * FROM withdrawals WHERE subject_id=?", (subject_id,)
        ).fetchone()
        if existing is not None:
            return _withdrawal_dict(existing)

        now = utcnow()
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            "INSERT INTO withdrawals (subject_id, requested_at, status, updated_at) "
            "VALUES (?, ?, 'accepted', ?)",
            (subject_id, now, now),
        )
        withdrawal_id = cur.lastrowid
        batch_rows = conn.execute(
            "SELECT batch_id FROM batch_subjects WHERE subject_id=? ORDER BY batch_id",
            (subject_id,),
        ).fetchall()
        conn.executemany(
            "INSERT INTO withdrawal_batches (withdrawal_id, batch_id, state) VALUES (?, ?, 'pending')",
            [(withdrawal_id, r[0]) for r in batch_rows],
        )
        conn.execute(
            "UPDATE withdrawals SET batches_total=? WHERE id=?",
            (len(batch_rows), withdrawal_id),
        )
        conn.execute("COMMIT")

    row = conn.execute("SELECT * FROM withdrawals WHERE id=?", (withdrawal_id,)).fetchone()
    return _withdrawal_dict(row)


def _withdrawal_dict(row) -> dict:
    return {
        "id": row["id"],
        "subject_id": row["subject_id"],
        "requested_at": row["requested_at"],
        "status": row["status"],
        "batches_total": row["batches_total"],
        "batches_done": row["batches_done"],
        "files_removed": row["files_removed"],
        "last_error": row["last_error"],
        "updated_at": row["updated_at"],
    }


def get_withdrawal(subject_id: str) -> dict | None:
    row = db.query_one("SELECT * FROM withdrawals WHERE subject_id=?", (subject_id,))
    return _withdrawal_dict(row) if row else None


def list_withdrawals() -> list[dict]:
    return [_withdrawal_dict(r) for r in db.query("SELECT * FROM withdrawals ORDER BY id")]


# ---------------------------------------------------------------- 后台清理

def _next_version_path(batch_dir: Path, current_path: str | None) -> Path:
    version = 1
    if current_path:
        stem = Path(current_path).stem
        if stem.startswith("v") and stem[1:].isdigit():
            version = int(stem[1:]) + 1
    return batch_dir / f"v{version}.parquet"


def process_due_withdrawal() -> bool:
    """处理一条未完成的撤回任务。返回是否处理了任务。"""
    conn = db.get_conn()
    with file_lock(config.WITHDRAW_LOCK, timeout=5):
        w = conn.execute(
            "SELECT * FROM withdrawals WHERE status != 'completed' ORDER BY id LIMIT 1"
        ).fetchone()
        if w is None:
            return False

        withdrawal_id = w["id"]
        subject_id = w["subject_id"]
        conn.execute(
            "UPDATE withdrawals SET status='processing', updated_at=? WHERE id=?",
            (utcnow(), withdrawal_id),
        )
        try:
            # 1. 逐批重写（每批独立事务，崩溃后可从 rewriting 状态接续）
            wb_rows = conn.execute(
                "SELECT batch_id, state FROM withdrawal_batches "
                "WHERE withdrawal_id=? AND state != 'done' ORDER BY batch_id",
                (withdrawal_id,),
            ).fetchall()
            for wb in wb_rows:
                batch_id = wb["batch_id"]
                _rewrite_batch(withdrawal_id, batch_id, subject_id)

            # 2. 全卷清扫：旧版本与暂存件中含该人的文件全部清除
            removed = _final_sweep(subject_id)

            # 3. 完成
            conn.execute(
                "UPDATE withdrawals SET status='completed', files_removed=?, "
                "last_error=NULL, updated_at=? WHERE id=?",
                (removed, utcnow(), withdrawal_id),
            )
        except Exception as exc:
            conn.execute(
                "UPDATE withdrawals SET status='failed', last_error=?, updated_at=? WHERE id=?",
                (repr(exc), utcnow(), withdrawal_id),
            )
            raise
        return True


def _rewrite_batch(withdrawal_id: int, batch_id: int, subject_id: str) -> None:
    conn = db.get_conn()
    batch = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
    if batch is None:
        # 批次已不存在，直接标记完成
        conn.execute(
            "UPDATE withdrawal_batches SET state='done' WHERE withdrawal_id=? AND batch_id=?",
            (withdrawal_id, batch_id),
        )
        return

    # 状态翻转与下载封锁在同一事务
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("UPDATE batches SET status='rewriting' WHERE id=?", (batch_id,))
    conn.execute(
        "UPDATE withdrawal_batches SET state='rewriting' "
        "WHERE withdrawal_id=? AND batch_id=?",
        (withdrawal_id, batch_id),
    )
    current_path = conn.execute(
        "SELECT current_path FROM batches WHERE id=?", (batch_id,)
    ).fetchone()[0]
    conn.execute("COMMIT")

    batch_dir = config.BATCHES_DIR / f"batch-{batch_id:08d}"
    new_path = _next_version_path(batch_dir, current_path)
    old_path = Path(current_path) if current_path else None

    # 幂等：崩溃重启后可能 new_path 已存在，重算一遍结果相同
    kept = rewrite_without_subject(old_path, new_path, subject_id)

    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "UPDATE batches SET current_path=?, row_count=?, status='published' WHERE id=?",
        (str(new_path), kept, batch_id),
    )
    conn.execute(
        "DELETE FROM batch_subjects WHERE batch_id=? AND subject_id=?",
        (batch_id, subject_id),
    )
    conn.execute(
        "UPDATE withdrawal_batches SET state='done' WHERE withdrawal_id=? AND batch_id=?",
        (withdrawal_id, batch_id),
    )
    # 直接以 done 行数回填，崩溃接续重复执行结果也一致
    conn.execute(
        "UPDATE withdrawals SET "
        "batches_done=(SELECT COUNT(*) FROM withdrawal_batches "
        "WHERE withdrawal_id=? AND state='done'), updated_at=? WHERE id=?",
        (withdrawal_id, utcnow(), withdrawal_id),
    )
    conn.execute("COMMIT")

    # 旧版本立即删除（崩溃残留由最终清扫兜底）
    if old_path and old_path.exists() and old_path != new_path:
        old_path.unlink()


def _final_sweep(subject_id: str) -> int:
    """扫描共享卷全部 Parquet，清除含该人的旧件/暂存件，并做最终校验。"""
    conn = db.get_conn()
    valid_paths = {
        r[0] for r in conn.execute(
            "SELECT current_path FROM batches WHERE current_path IS NOT NULL"
        ).fetchall()
    }
    removed = 0

    # 2a. 批次目录下不属于任何批次当前版本的文件 = 历史旧版本，删除
    for p in list_parquet_files():
        sp = str(p)
        if sp.startswith(str(config.BATCHES_DIR)) and sp not in valid_paths:
            p.unlink()
            removed += 1

    # 2b. 暂存件：含该人的整份删除，并把对应导入标记为 rejected
    for p in list_parquet_files():
        sp = str(p)
        if not sp.startswith(str(config.STAGING_DIR)):
            continue
        if parquet_contains_subject(p, subject_id):
            p.unlink()
            removed += 1
            conn.execute(
                "UPDATE imports SET status='rejected', "
                "note='withdrawal sweep: contained withdrawn subject' WHERE path=?",
                (sp,),
            )

    # 2c. 最终校验：任何现存 Parquet 都不得再含该人，否则任务失败不得报完成
    for p in list_parquet_files():
        if parquet_contains_subject(p, subject_id):
            raise ServiceError(
                f"撤回清扫后仍发现含受试者 {subject_id} 的文件：{p}", status_code=500
            )
    return removed


# ---------------------------------------------------------------- 下载与列表

def get_downloadable_batch(batch_id: int):
    row = db.query_one("SELECT * FROM batches WHERE id=?", (batch_id,))
    if row is None:
        raise ServiceError(f"批次 {batch_id} 不存在", status_code=404)
    if row["status"] != "published":
        raise ServiceError(
            f"批次 {batch_id} 正在撤回重写，暂时禁止下载", status_code=409
        )
    path = Path(row["current_path"])
    if not path.exists():
        # 绝不允许把丢失/损坏的文件当成功返回
        raise ServiceError(f"批次 {batch_id} 的数据文件缺失", status_code=500)
    return row, path


def list_batches() -> list[dict]:
    rows = db.query(
        "SELECT b.*, "
        "(SELECT GROUP_CONCAT(subject_id, ',') FROM batch_subjects WHERE batch_id=b.id) AS subjects "
        "FROM batches b ORDER BY b.id"
    )
    return [
        {
            "id": r["id"],
            "created_at": r["created_at"],
            "row_count": r["row_count"],
            "status": r["status"],
            "subjects": (r["subjects"] or "").split(",") if r["subjects"] else [],
        }
        for r in rows
    ]


# ---------------------------------------------------------------- 崩溃恢复

def recover_on_startup() -> dict:
    """进程启动时接续未完成状态，绝不误报完成。"""
    config.ensure_dirs()
    db.init_db()
    conn = db.get_conn()
    cleaned_tmp = 0
    rejected_imports = 0
    deleted_orphans = 0

    # 1. 清理写入中途崩溃留下的临时文件
    for root in (config.BATCHES_DIR, config.STAGING_DIR):
        if not root.exists():
            continue
        for p in root.rglob(".tmp.*"):
            if p.is_file():
                p.unlink()
                cleaned_tmp += 1

    # 2. 暂存导入：文件丢失或没有关联批次（发布途中崩溃）→ 作废并清理
    for imp in conn.execute("SELECT id, path FROM imports WHERE status='staging'").fetchall():
        p = Path(imp["path"])
        if not p.exists():
            conn.execute(
                "UPDATE imports SET status='rejected', note=? WHERE id=?",
                ("startup recovery: staging file lost", imp["id"]),
            )
            rejected_imports += 1
        else:
            linked = conn.execute(
                "SELECT 1 FROM imports WHERE id=? AND batch_id IS NOT NULL", (imp["id"],)
            ).fetchone()
            if linked is None:
                p.unlink()
                conn.execute(
                    "UPDATE imports SET status='rejected', note=? WHERE id=?",
                    ("startup recovery: unfinished publish", imp["id"]),
                )
                rejected_imports += 1

    # 3. 批次状态恢复：rewriting 留给 worker 接续；文件丢失的 published 标记 failed
    for b in conn.execute("SELECT id, status, current_path FROM batches").fetchall():
        if b["status"] == "published" and (not b["current_path"] or not Path(b["current_path"]).exists()):
            conn.execute("UPDATE batches SET status='failed' WHERE id=?", (b["id"],))

    # 4. 清理批次目录中无任何 DB 引用的孤儿 Parquet
    valid_paths = {
        r[0] for r in conn.execute(
            "SELECT current_path FROM batches WHERE current_path IS NOT NULL"
        ).fetchall()
    }
    if config.BATCHES_DIR.exists():
        for p in config.BATCHES_DIR.rglob("*.parquet"):
            if str(p) not in valid_paths:
                # 注意：rewriting 中的新版本（vN 已写但事务未提交）也会被清，
                # worker 接续时会基于当前版本幂等重写，安全。
                p.unlink()
                deleted_orphans += 1

    return {
        "cleaned_tmp_files": cleaned_tmp,
        "rejected_imports": rejected_imports,
        "deleted_orphan_parquets": deleted_orphans,
    }
