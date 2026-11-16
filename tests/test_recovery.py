"""崩溃恢复：处理中进程退出后重启可接续，不误报完成，不恢复旧下载。"""
from app import config, db, services
from app.parquet_store import list_parquet_files, parquet_contains_subject
from tests.conftest import csv_upload, wait_completed


def test_recovery_after_crash_mid_rewrite(client, monkeypatch):
    csv_upload(client, [
        ("S001", "2026-01-01", 1),
        ("S001", "2026-01-02", 2),
        ("S002", "2026-01-01", 3),
    ])
    client.post("/api/withdrawals", json={"subject_id": "S001"})

    # 模拟进程在批次状态翻转为 rewriting 之后、重写完成之前崩溃
    def crash(src, dst, subject):
        raise RuntimeError("simulated process crash during rewrite")

    monkeypatch.setattr(services, "rewrite_without_subject", crash)
    import pytest

    with pytest.raises(RuntimeError, match="simulated process crash"):
        services.process_due_withdrawal()

    w = client.get("/api/withdrawals/S001").json()
    assert w["status"] == "failed"  # 绝不能报 completed

    # 批次处于 rewriting，旧下载无法恢复（禁止新下载）
    r = client.get("/api/batches/1/download")
    assert r.status_code == 409

    # 模拟进程重启
    monkeypatch.undo()
    recovery = services.recover_on_startup()
    assert isinstance(recovery, dict)

    # 接续完成
    final = wait_completed(client, "S001")
    assert final["status"] == "completed"

    for p in list_parquet_files():
        assert not parquet_contains_subject(p, "S001")

    import io
    import pyarrow.parquet as pq

    r = client.get("/api/batches/1/download")
    assert r.status_code == 200
    t = pq.read_table(io.BytesIO(r.content))
    assert t.column("subject_id").to_pylist() == ["S002"]


def test_recovery_cleans_tmp_and_orphan_files(client):
    csv_upload(client, [("S007", "2026-01-01", 1)])
    # 伪造写入中途崩溃的临时文件与孤儿 parquet
    tmp = config.BATCHES_DIR / "batch-00000001" / ".tmp.999.v2.parquet"
    tmp.write_bytes(b"junk")
    orphan = config.BATCHES_DIR / "batch-00000001" / "v99-old.parquet"
    orphan.write_bytes(b"junk")

    rec = services.recover_on_startup()
    assert rec["cleaned_tmp_files"] >= 1
    assert rec["deleted_orphan_parquets"] >= 1
    assert not tmp.exists()
    assert not orphan.exists()


def test_recovery_drops_unfinished_staging_import(client):
    """发布途中崩溃：暂存 parquet 无关联批次，重启作废并清除。"""
    import datetime as dt

    import pyarrow as pa

    from app.parquet_store import SCHEMA, atomic_write_parquet

    table = pa.table(
        {
            "subject_id": ["S010"],
            "date": [dt.date(2026, 1, 1)],
            "steps": [5],
        },
        schema=SCHEMA,
    )
    staging = config.STAGING_DIR / "import-orphan.parquet"
    atomic_write_parquet(table, staging)
    conn = db.get_conn()
    conn.execute(
        "INSERT INTO imports (created_at, status, path, row_count) VALUES (?, 'staging', ?, 1)",
        (dt.datetime.now(dt.timezone.utc).isoformat(), str(staging)),
    )

    rec = services.recover_on_startup()
    assert rec["rejected_imports"] >= 1
    assert not staging.exists()
    row = conn.execute("SELECT status FROM imports WHERE path=?", (str(staging),)).fetchone()
    assert row[0] == "rejected"


def test_inflight_staging_import_with_subject_is_swept(client):
    """受理前已开始但尚未发布的导入，受理后也必须被拒绝并清除。"""
    import datetime as dt

    import pyarrow as pa

    from app.parquet_store import SCHEMA, atomic_write_parquet

    # 构造一个"受理前开始、尚未发布"的暂存导入（含 S001）
    table = pa.table(
        {
            "subject_id": ["S001", "S003"],
            "date": [dt.date(2026, 1, 1), dt.date(2026, 1, 1)],
            "steps": [1, 2],
        },
        schema=SCHEMA,
    )
    staging = config.STAGING_DIR / "import-inflight.parquet"
    atomic_write_parquet(table, staging)
    conn = db.get_conn()
    conn.execute(
        "INSERT INTO imports (created_at, status, path, row_count) VALUES (?, 'staging', ?, 2)",
        (dt.datetime.now(dt.timezone.utc).isoformat(), str(staging)),
    )

    client.post("/api/withdrawals", json={"subject_id": "S001"})
    final = wait_completed(client, "S001")
    assert final["status"] == "completed"
    assert not staging.exists()
    row = conn.execute("SELECT status FROM imports WHERE path=?", (str(staging),)).fetchone()
    assert row[0] == "rejected"
    assert final["files_removed"] >= 1
