"""并发与双实例语义：锁互斥、受理/发布全序、重复撤回安全。"""
import threading

from app import config, services
from tests.conftest import csv_upload, make_csv, wait_completed


def test_concurrent_publish_and_withdrawal_ordering(client):
    """导入发布与撤回受理在 PUBLISH_LOCK 下全序：先发布则入批，先受理则被拒。"""
    results = {}

    def publish():
        r = csv_upload(client, [("SX1", "2026-01-01", 1), ("SX2", "2026-01-01", 2)])
        results["publish"] = (r.status_code, r.json())

    def withdraw():
        r = client.post("/api/withdrawals", json={"subject_id": "SX1"})
        results["withdraw"] = r.status_code

    t1 = threading.Thread(target=publish)
    t2 = threading.Thread(target=withdraw)
    t1.start(); t2.start()
    t1.join(); t2.join()

    wait_completed(client, "SX1")
    pub_status, pub_body = results["publish"]

    if pub_status == 200:
        # 发布先于受理：该批次必须在撤回中被重写干净
        bid = pub_body["batch_id"]
        import io
        import pyarrow.parquet as pq

        r = client.get(f"/api/batches/{bid}/download")
        t = pq.read_table(io.BytesIO(r.content))
        assert set(t.column("subject_id").to_pylist()) == {"SX2"}
    else:
        # 受理先于发布：整批拒收
        assert pub_status == 409


def test_two_workers_only_one_runs_at_a_time(client):
    """两个实例的 worker 并发处理：WITHDRAW_LOCK 保证互斥且任务只完成一次。"""
    csv_upload(client, [("S001", "2026-01-01", 1), ("S002", "2026-01-01", 2)])
    client.post("/api/withdrawals", json={"subject_id": "S001"})

    outcomes = []

    def worker():
        try:
            outcomes.append(services.process_due_withdrawal())
        except Exception:
            outcomes.append(False)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start(); t2.start()
    t1.join(); t2.join()

    # 恰好一个线程拿到任务（另一个查不到未完成任务）
    assert sorted(outcomes) == [False, True]
    final = client.get("/api/withdrawals/S001").json()
    assert final["status"] == "completed"


def test_repeated_withdrawal_after_completion_returns_same_record(client):
    csv_upload(client, [("S020", "2026-01-01", 1)])
    client.post("/api/withdrawals", json={"subject_id": "S020"})
    wait_completed(client, "S020")
    r = client.post("/api/withdrawals", json={"subject_id": "S020"})
    assert r.status_code == 200
    assert r.json()["status"] == "completed"
    # 再次完成态检查：后续上传仍被永久拒收
    r = csv_upload(client, [("S020", "2026-02-01", 9)])
    assert r.status_code == 409


def test_unknown_batch_download_404(client):
    r = client.get("/api/batches/9999/download")
    assert r.status_code == 404
