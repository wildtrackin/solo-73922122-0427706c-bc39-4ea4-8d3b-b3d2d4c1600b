"""上传校验：大小、行数、字段、编码。"""
from app import config
from tests.conftest import csv_upload, make_csv


def test_basic_upload_and_list(client):
    r = csv_upload(client, [("S001", "2026-11-01", 100), ("S002", "2026-11-02", 0)])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["row_count"] == 2
    assert body["subjects"] == ["S001", "S002"]

    r = client.get("/api/batches")
    assert r.status_code == 200
    batches = r.json()["batches"]
    assert len(batches) == 1
    assert batches[0]["status"] == "published"


def test_chinese_header_accepted(client):
    r = csv_upload(client, [("A01", "2026-01-01", 50)], header=("受试者号", "日期", "步数"))
    assert r.status_code == 200, r.text


def test_oversize_rejected(client):
    big = make_csv([("S1", "2026-01-01", 1)])
    big += b"x" * (config.MAX_UPLOAD_BYTES + 10)
    r = client.post("/api/imports", files={"file": ("big.csv", big, "text/csv")})
    assert r.status_code == 413


def test_too_many_rows_rejected(client):
    rows = [("S%04d" % i, "2026-01-01", i) for i in range(1001)]
    r = csv_upload(client, rows)
    assert r.status_code == 422
    assert "1000" in r.json()["detail"]


def test_negative_steps_rejected(client):
    r = csv_upload(client, [("S1", "2026-01-01", -5)])
    assert r.status_code == 422


def test_bad_date_and_encoding(client):
    r = csv_upload(client, [("S1", "2026/01/01", 5)])
    assert r.status_code == 422
    r = client.post(
        "/api/imports",
        files={"file": ("b.csv", "subject_id,date,steps\nS1,2026-01-01,1\n".encode("utf-16"), "text/csv")},
    )
    assert r.status_code == 422


def test_empty_and_bad_header(client):
    r = client.post("/api/imports", files={"file": ("e.csv", b"", "text/csv")})
    assert r.status_code == 400
    r = client.post(
        "/api/imports",
        files={"file": ("h.csv", "foo,bar,baz\n1,2,3\n", "text/csv")},
    )
    assert r.status_code == 422


def test_no_spill_csv_on_disk(client, tmp_path):
    """CSV 仅在内存解析：数据卷内不得出现任何 .csv 落盘。"""
    r = csv_upload(client, [("S1", "2026-01-01", 9)])
    assert r.status_code == 200
    csv_files = list(config.DATA_DIR.rglob("*.csv"))
    assert csv_files == []
    pq = list(config.BATCHES_DIR.rglob("*.parquet"))
    assert len(pq) == 1
