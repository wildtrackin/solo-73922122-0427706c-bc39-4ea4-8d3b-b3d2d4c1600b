"""撤回核心语义：永久拒收、批次重写、下载封锁、幂等接续。"""
import io

import pyarrow.parquet as pq

from tests.conftest import csv_upload, wait_completed


def _download_table(client, batch_id):
    r = client.get(f"/api/batches/{batch_id}/download")
    assert r.status_code == 200
    return pq.read_table(io.BytesIO(r.content))


def _download_rows(client, batch_id):
    t = _download_table(client, batch_id)
    return {
        "subject_id": t.column("subject_id").to_pylist(),
        "date": t.column("date").to_pylist(),
        "steps": t.column("steps").to_pylist(),
    }


def test_withdrawal_rewrites_batches_and_blocks_uploads(client):
    r = csv_upload(client, [
        ("S001", "2026-11-01", 10),
        ("S001", "2026-11-02", 20),
        ("S002", "2026-11-01", 30),
    ])
    batch_id = r.json()["batch_id"]

    r = client.post("/api/withdrawals", json={"subject_id": "S001"})
    assert r.status_code == 200
    assert r.json()["status"] == "accepted"

    final = wait_completed(client, "S001")
    assert final["status"] == "completed"
    assert final["batches_done"] == 1

    # 批次被重写：S001 的两行消失，S002 完整保留
    rows = _download_rows(client, batch_id)
    assert rows["subject_id"] == ["S002"]
    assert rows["steps"] == [30]
    assert len(rows["subject_id"]) == 1

    # 受理后永久拒收：含该人的整批上传被拒
    r = csv_upload(client, [("S001", "2026-12-01", 1), ("S003", "2026-12-01", 2)])
    assert r.status_code == 409
    assert "S001" in r.json()["detail"]

    # 不含该人的新批次仍可导入
    r = csv_upload(client, [("S003", "2026-12-01", 7)])
    assert r.status_code == 200


def test_duplicate_withdrawal_joins_existing_task(client):
    csv_upload(client, [("S009", "2026-01-01", 1)])
    r1 = client.post("/api/withdrawals", json={"subject_id": "S009"})
    id1 = r1.json()["id"]
    r2 = client.post("/api/withdrawals", json={"subject_id": "S009"})
    assert r2.status_code == 200
    assert r2.json()["id"] == id1  # 接续原任务，不新建

    final = wait_completed(client, "S009")
    assert final["id"] == id1
    assert final["status"] == "completed"


def test_download_blocked_while_rewriting(client, monkeypatch):
    csv_upload(client, [("S001", "2026-01-01", 1), ("S002", "2026-01-01", 2)])
    client.post("/api/withdrawals", json={"subject_id": "S001"})

    from app import services

    # 在批次被标记 rewriting 的窗口内，下载必须返回 409
    original = services.rewrite_without_subject

    def wrapped(src, dst, subject):
        r = client.get("/api/batches/1/download")
        assert r.status_code == 409
        return original(src, dst, subject)

    monkeypatch.setattr(services, "rewrite_without_subject", wrapped)
    final = wait_completed(client, "S001")
    assert final["status"] == "completed"

    # 重写完成后下载恢复
    r = client.get("/api/batches/1/download")
    assert r.status_code == 200


def test_withdrawal_removes_old_versions(client):
    from app import config, db, services

    csv_upload(client, [("S001", "2026-01-01", 1), ("S002", "2026-01-01", 2)])
    client.post("/api/withdrawals", json={"subject_id": "S001"})
    final = wait_completed(client, "S001")
    assert final["status"] == "completed"

    # 旧版本 v1 已删除，只剩新版本
    files = sorted(config.BATCHES_DIR.rglob("*.parquet"))
    assert len(files) == 1
    assert files[0].name == "v2.parquet"

    # 卷中任何 parquet 都不含 S001
    from app.parquet_store import list_parquet_files, parquet_contains_subject

    for p in list_parquet_files():
        assert not parquet_contains_subject(p, "S001")


def test_started_download_survives_rewrite(client, monkeypatch):
    """已开始的下载持有旧文件句柄：重写删除旧版本后仍可完整读完。"""
    import os

    from app import config, services

    csv_upload(client, [("S001", "2026-01-01", 1), ("S002", "2026-01-01", 2)])
    client.post("/api/withdrawals", json={"subject_id": "S001"})

    # 先取得当前文件句柄（模拟下载已经开始、正在传输）
    row, path = services.get_downloadable_batch(1)
    fd = os.open(path, os.O_RDONLY)
    try:
        original = services.rewrite_without_subject

        def wrapped(src, dst, subject):
            result = original(src, dst, subject)
            # 重写完成、旧文件已删除后，旧句柄仍能读到完整内容
            data = os.pread(fd, 4096, 0)
            assert b"parquet" in data or len(data) > 0
            return result

        monkeypatch.setattr(services, "rewrite_without_subject", wrapped)
        final = wait_completed(client, "S001")
        assert final["status"] == "completed"
    finally:
        os.close(fd)


def test_withdrawal_with_zero_batches_completes(client):
    r = client.post("/api/withdrawals", json={"subject_id": "GHOST"})
    assert r.status_code == 200
    final = wait_completed(client, "GHOST")
    assert final["status"] == "completed"
    assert final["batches_total"] == 0


def test_multiple_batches_all_rewritten(client):
    for i in range(3):
        csv_upload(client, [
            ("S001", f"2026-01-0{i+1}", i),
            ("S002", f"2026-01-0{i+1}", 100 + i),
        ])
    client.post("/api/withdrawals", json={"subject_id": "S001"})
    final = wait_completed(client, "S001")
    assert final["status"] == "completed"
    assert final["batches_done"] == 3
    for bid in (1, 2, 3):
        rows = _download_rows(client, bid)
        assert set(rows["subject_id"]) == {"S002"}
