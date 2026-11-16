import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("STEP_WORKER_ENABLED", "0")

from app import config, db  # noqa: E402
from app.main import app  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


def _activate_data_dir(path: str) -> None:
    """把整套路径切换到新的隔离数据目录，并丢弃线程旧连接。"""
    data = config.Path(path)
    config.DATA_DIR = data
    config.DB_PATH = data / "stepstore.db"
    config.BATCHES_DIR = data / "batches"
    config.STAGING_DIR = data / "staging"
    config.LOCK_DIR = data / "locks"
    config.PUBLISH_LOCK = config.LOCK_DIR / "publish.lock"
    config.WITHDRAW_LOCK = config.LOCK_DIR / "withdraw.lock"
    if hasattr(db._local, "conn"):
        try:
            db._local.conn.close()
        finally:
            del db._local.conn
    config.ensure_dirs()


@pytest.fixture
def client(tmp_path):
    _activate_data_dir(str(tmp_path / "data"))
    with TestClient(app) as c:
        yield c


def make_csv(rows, header=("subject_id", "date", "steps")):
    lines = [",".join(header)]
    lines.extend(",".join(str(v) for v in r) for r in rows)
    return ("\n".join(lines) + "\n").encode("utf-8")


def csv_upload(client, rows, filename="batch.csv", header=("subject_id", "date", "steps")):
    data = make_csv(rows, header)
    return client.post(
        "/api/imports",
        files={"file": (filename, data, "text/csv")},
    )


def wait_completed(client, subject_id, timeout_seconds=10.0):
    """同步驱动 worker（测试中后台线程关闭），直到撤回完成。"""
    import time

    from app import services

    deadline = time.monotonic() + timeout_seconds
    last = None
    while time.monotonic() < deadline:
        services.process_due_withdrawal()
        r = client.get(f"/api/withdrawals/{subject_id}")
        last = r.json()
        if last["status"] in ("completed", "failed"):
            return last
    raise AssertionError(f"withdrawal {subject_id} did not finish: {last}")
