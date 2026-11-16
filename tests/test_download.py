"""下载与撤回交接：放行即持句柄、断流修复、Range 一致性、资源释放。

对应缺陷：A 已收到 200 响应头尚未读正文，B 重写同批并删除旧件，
A 随后 FileNotFoundError、正文为空。修复后：成功响应必须完整交付放行时版本，
长度与 Range 区间与正文一致；重写中拒绝新下载，完成后供给净化版。
"""
import io
import os
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from app import config, db, services
from app.downloads import CHUNK_SIZE, OpenedFileResponse, _parse_single_range
from tests.conftest import csv_upload, wait_completed


def _upload_batch(client):
    r = csv_upload(
        client,
        [("S001", "2026-01-01", 10), ("S001", "2026-01-02", 20), ("S002", "2026-01-01", 30)],
    )
    assert r.status_code == 200
    return r.json()["batch_id"]


def _read_fd(fd: int) -> bytes:
    """从句柄当前位置分块读完（模拟响应流式发送），读完关闭。"""
    chunks = []
    try:
        while True:
            chunk = os.read(fd, CHUNK_SIZE)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)


# ---------------------------------------------------------------- 断流修复

def test_admitted_download_survives_unlink_and_delivers_released_version(client):
    """放行（句柄已开）后撤回重写并删除旧件：下载仍完整交付放行时版本。"""
    bid = _upload_batch(client)
    row, fd, path = services.open_batch_download(bid)
    size = os.fstat(fd).st_size
    try:
        # 撤回处理完成：批次被重写，旧版本文件被物理删除
        client.post("/api/withdrawals", json={"subject_id": "S001"})
        final = wait_completed(client, "S001")
        assert final["status"] == "completed"
        assert not Path(path).exists()
        # 已放行下载从句柄流式读完：长度与内容完整，且是放行时版本（含 S001）
        data = _read_fd(fd)
    finally:
        try:
            os.close(fd)  # 已关闭则 OSError，忽略
        except OSError:
            pass
    assert len(data) == size
    table = pq.read_table(io.BytesIO(data))
    assert sorted(set(table.column("subject_id").to_pylist())) == ["S001", "S002"]


def test_http_headers_sent_then_rewrite_body_still_complete(client):
    """200 响应头已发出、正文未读时发生重写删件：正文仍完整且与 Content-Length 一致。"""
    bid = _upload_batch(client)
    with client.stream("GET", f"/api/batches/{bid}/download") as resp:
        assert resp.status_code == 200
        declared = int(resp.headers["content-length"])
        assert resp.headers["accept-ranges"] == "bytes"
        # 响应头已收到（服务端句柄已持有），此时撤回重写并删除旧版本
        client.post("/api/withdrawals", json={"subject_id": "S001"})
        assert wait_completed(client, "S001")["status"] == "completed"
        body = b"".join(resp.iter_bytes())
    assert len(body) == declared
    table = pq.read_table(io.BytesIO(body))
    assert sorted(set(table.column("subject_id").to_pylist())) == ["S001", "S002"]


def test_open_retries_when_version_switches_mid_race(client, monkeypatch):
    """准入后旧文件恰被重写切换删除：按新路径重试，供给净化版。"""
    bid = _upload_batch(client)
    # 真实执行一次撤回重写：DB 指向 v2（净化版），v1 已删除
    client.post("/api/withdrawals", json={"subject_id": "S001"})
    assert wait_completed(client, "S001")["status"] == "completed"
    stale_path = str(config.BATCHES_DIR / f"batch-{bid:08d}" / "v1.parquet")
    assert not Path(stale_path).exists()

    # 构造"准入读到旧路径"的竞态窗口：首次准入返回已删除的 v1，
    # open 失败后必须重读状态、切到 v2 并成功
    real_admit = services._admit_download
    calls = {"n": 0}

    def stale_first(batch_id):
        calls["n"] += 1
        if calls["n"] == 1:
            row = db.query_one("SELECT * FROM batches WHERE id=?", (batch_id,))
            return row, stale_path
        return real_admit(batch_id)

    monkeypatch.setattr(services, "_admit_download", stale_first)
    row, fd, path = services.open_batch_download(bid)
    assert path.endswith("v2.parquet")
    data = _read_fd(fd)
    table = pq.read_table(io.BytesIO(data))
    assert set(table.column("subject_id").to_pylist()) == {"S002"}


def test_open_missing_file_is_500_not_empty_success(client):
    """DB 未变而文件真实丢失：报 500，绝不把缺失文件当成功返回。"""
    bid = _upload_batch(client)
    row = db.query_one("SELECT current_path FROM batches WHERE id=?", (bid,))
    os.unlink(row["current_path"])
    with pytest.raises(services.ServiceError) as exc_info:
        services.open_batch_download(bid)
    assert exc_info.value.status_code == 500
    r = client.get(f"/api/batches/{bid}/download")
    assert r.status_code == 500


def test_download_stays_blocked_when_withdrawal_failed(client, monkeypatch):
    """撤回处理崩溃：批次停在 rewriting 持续停供（409），重启接续后恢复。"""
    bid = _upload_batch(client)
    client.post("/api/withdrawals", json={"subject_id": "S001"})

    def crash(src, dst, subject):
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(services, "rewrite_without_subject", crash)
    with pytest.raises(RuntimeError):
        services.process_due_withdrawal()
    r = client.get(f"/api/batches/{bid}/download")
    assert r.status_code == 409

    # 重启接续完成后恢复下载，且是净化版
    monkeypatch.undo()
    services.recover_on_startup()
    assert wait_completed(client, "S001")["status"] == "completed"
    r = client.get(f"/api/batches/{bid}/download")
    assert r.status_code == 200
    table = pq.read_table(io.BytesIO(r.content))
    assert set(table.column("subject_id").to_pylist()) == {"S002"}


# ---------------------------------------------------------------- Range 一致性

def test_range_requests_are_consistent_with_body(client):
    bid = _upload_batch(client)
    full = client.get(f"/api/batches/{bid}/download")
    assert full.status_code == 200
    size = len(full.content)
    assert int(full.headers["content-length"]) == size
    assert full.headers["accept-ranges"] == "bytes"
    assert "etag" in full.headers

    r = client.get(f"/api/batches/{bid}/download", headers={"Range": "bytes=10-29"})
    assert r.status_code == 206
    assert r.headers["content-range"] == f"bytes 10-29/{size}"
    assert int(r.headers["content-length"]) == 20
    assert r.content == full.content[10:30]

    r = client.get(f"/api/batches/{bid}/download", headers={"Range": f"bytes={size - 10}-"})
    assert r.status_code == 206
    assert r.headers["content-range"] == f"bytes {size - 10}-{size - 1}/{size}"
    assert r.content == full.content[-10:]

    r = client.get(f"/api/batches/{bid}/download", headers={"Range": "bytes=-16"})
    assert r.status_code == 206
    assert r.headers["content-range"] == f"bytes {size - 16}-{size - 1}/{size}"
    assert r.content == full.content[-16:]

    # end 超出末尾被夹紧到文件尾
    r = client.get(f"/api/batches/{bid}/download", headers={"Range": f"bytes=0-{size + 100}"})
    assert r.status_code == 206
    assert r.headers["content-range"] == f"bytes 0-{size - 1}/{size}"
    assert r.content == full.content


def test_range_unsatisfiable_returns_416(client):
    bid = _upload_batch(client)
    size = len(client.get(f"/api/batches/{bid}/download").content)

    r = client.get(f"/api/batches/{bid}/download", headers={"Range": f"bytes={size}-"})
    assert r.status_code == 416
    assert r.headers["content-range"] == f"bytes */{size}"

    r = client.get(f"/api/batches/{bid}/download", headers={"Range": "bytes=20-10"})
    assert r.status_code == 416

    r = client.get(f"/api/batches/{bid}/download", headers={"Range": "bytes=-0"})
    assert r.status_code == 416


def test_range_malformed_or_multi_falls_back_to_full_body(client):
    bid = _upload_batch(client)
    full = client.get(f"/api/batches/{bid}/download").content
    for header in ("bytes=abc-def", "items=0-9", "bytes=0-9,20-29", "bytes="):
        r = client.get(f"/api/batches/{bid}/download", headers={"Range": header})
        assert r.status_code == 200, header
        assert r.content == full, header


def test_if_range_validator_mismatch_returns_full_body(client):
    bid = _upload_batch(client)
    full = client.get(f"/api/batches/{bid}/download")
    etag = full.headers["etag"]

    r = client.get(
        f"/api/batches/{bid}/download", headers={"Range": "bytes=0-9", "If-Range": etag}
    )
    assert r.status_code == 206
    assert r.content == full.content[:10]

    r = client.get(
        f"/api/batches/{bid}/download", headers={"Range": "bytes=0-9", "If-Range": '"stale"'}
    )
    assert r.status_code == 200
    assert r.content == full.content


def test_head_returns_headers_without_body(client):
    bid = _upload_batch(client)
    get_full = client.get(f"/api/batches/{bid}/download")
    r = client.head(f"/api/batches/{bid}/download")
    assert r.status_code == 200
    assert r.headers["content-length"] == get_full.headers["content-length"]
    assert r.headers["accept-ranges"] == "bytes"
    assert r.headers["etag"] == get_full.headers["etag"]
    assert not r.content

    r = client.head(f"/api/batches/{bid}/download", headers={"Range": "bytes=0-9"})
    assert r.status_code == 206
    assert r.headers["content-length"] == "10"
    assert not r.content


# ---------------------------------------------------------------- 资源释放

def test_response_call_closes_fd_on_completion_and_on_send_failure(client):
    """正常发完与发送失败（客户端断连）两条路径都必须关闭句柄。"""
    import anyio

    bid = _upload_batch(client)

    async def run_response(fail_at: int | None) -> int:
        row, fd, path = services.open_batch_download(bid)
        size = os.fstat(fd).st_size
        resp = OpenedFileResponse(fd, size, status_code=200, headers={})
        sent = {"n": 0}

        async def send(message):
            sent["n"] += 1
            if fail_at is not None and sent["n"] > fail_at:
                raise RuntimeError("client disconnected")

        async def receive():
            return {"type": "http.request"}

        scope = {"type": "http", "method": "GET", "asgi": {"spec_version": "2.4"}}
        if fail_at is None:
            await resp(scope, receive, send)
        else:
            with pytest.raises(RuntimeError, match="client disconnected"):
                await resp(scope, receive, send)
        return fd

    fd = anyio.run(run_response, None)
    with pytest.raises(OSError):
        os.fstat(fd)  # 正常发完：句柄已释放

    fd = anyio.run(run_response, 2)
    with pytest.raises(OSError):
        os.fstat(fd)  # 正文发送中途失败：句柄同样已释放

    fd = anyio.run(run_response, 0)
    with pytest.raises(OSError):
        os.fstat(fd)  # 响应头发送即失败（客户端先断连）：句柄也已释放


def test_index_page_served(client):
    """单页前端可访问，且包含导入/撤回/进度/下载四个功能区。"""
    r = client.get("/")
    assert r.status_code == 200
    html = r.text
    for marker in ("/api/imports", "/api/withdrawals", "/api/batches", "download"):
        assert marker in html


def test_range_parse_units():
    assert _parse_single_range(None, 100) is None
    assert _parse_single_range("bytes=10-19", 100) == (10, 19)
    assert _parse_single_range("bytes=90-", 100) == (90, 99)
    assert _parse_single_range("bytes=-10", 100) == (90, 99)
    assert _parse_single_range("bytes=-200", 100) == (0, 99)  # 超过全长给全量
    assert _parse_single_range("bytes=0-999", 100) == (0, 99)
    assert _parse_single_range("bytes=0-9,20-29", 100) is None
    assert _parse_single_range("items=0-9", 100) is None
    with pytest.raises(Exception):
        _parse_single_range("bytes=100-", 100)
    with pytest.raises(Exception):
        _parse_single_range("bytes=50-40", 100)
