"""下载响应：基于已打开文件句柄的流式响应，支持单区间 Range 与 HEAD。

修复"撤回重写删件导致下载断流"的关键约定：

- 文件句柄在路由准入时即打开（services.open_batch_download），响应头随后才发出；
  撤回重写随后 unlink 旧文件不影响已持有的句柄，成功响应必完整交付放行时版本；
- Content-Length / Content-Range / ETag / Last-Modified 全部取自该句柄的 fstat，
  与正文同源，长度和 Range 区间始终与正文一致；
- 分块流式发送：不把整件载入内存、不另存旧副本、不持任何锁，
  慢下载不会拖住撤回、旧路径清理或其他批次请求；
- __call__ 的 finally 关闭句柄：正常发完、客户端断连、发送失败都会释放读取资源。
"""
from __future__ import annotations

import os
from email.utils import formatdate
from functools import partial

import anyio
from starlette.requests import Request
from starlette.responses import Response

# 流式块大小：单次读盘不超过 512 KiB，绝不整件载入内存
CHUNK_SIZE = 512 * 1024


class _RangeUnsatisfiable(Exception):
    """区间语法合法但不可满足（响应 416）。"""


def _parse_single_range(header: str | None, size: int) -> tuple[int, int] | None:
    """解析单个 bytes 区间，返回含端点的 (start, end)。

    返回 None 表示忽略 Range（回 200 全量）：无头、非 bytes 单位、语法非法、
    或多区间（不支持 multipart/byteranges，RFC 7233 允许整体忽略）。
    区间合法但不可满足时抛 _RangeUnsatisfiable（回 416）。
    """
    if not header:
        return None
    unit, _, spec = header.partition("=")
    if unit.strip().lower() != "bytes" or not spec:
        return None
    spec = spec.strip()
    if "," in spec:
        return None  # 多区间：忽略，回 200 全量
    first, _, last = spec.partition("-")
    first, last = first.strip(), last.strip()
    if not first and not last:
        return None
    try:
        if first:
            start = int(first)
            end = int(last) if last else size - 1
        else:  # bytes=-suffix：最后 suffix 字节
            suffix = int(last)
            if suffix <= 0:
                raise _RangeUnsatisfiable
            start = max(size - suffix, 0)
            end = size - 1
    except ValueError:
        return None  # 非数字：语法非法，忽略
    if start < 0 or end < 0 or start > end or start >= size:
        raise _RangeUnsatisfiable
    return start, min(end, size - 1)


class OpenedFileResponse(Response):
    """已打开句柄的流式文件响应：finally 保证句柄在任何退出路径下关闭。"""

    def __init__(self, fd: int, length: int, **kwargs):
        super().__init__(**kwargs)
        self._fd = fd
        self._remaining = length

    async def __call__(self, scope, receive, send) -> None:
        fd = self._fd
        try:
            await send(
                {
                    "type": "http.response.start",
                    "status": self.status_code,
                    "headers": self.raw_headers,
                }
            )
            remaining = self._remaining
            while remaining > 0:
                chunk = await anyio.to_thread.run_sync(
                    partial(os.read, fd, min(CHUNK_SIZE, remaining))
                )
                if not chunk:
                    # 批次文件落盘后不再原地变化，正常不会提前 EOF；
                    # 防御性提前结束，绝不编造字节凑数
                    break
                remaining -= len(chunk)
                await send(
                    {
                        "type": "http.response.body",
                        "body": chunk,
                        "more_body": True,
                    }
                )
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        finally:
            # 正常发完、响应头/正文发送失败、客户端断连都会走到这里
            os.close(fd)


def build_download_response(
    request: Request, fd: int, *, filename: str, media_type: str
) -> Response:
    """基于已打开句柄构造下载响应（200/206/416），接管句柄生命周期。

    无论返回哪个响应，句柄都已被接管：200/206 由 OpenedFileResponse 发送后关闭，
    416/HEAD 在此立即关闭。
    """
    st = os.fstat(fd)
    size = st.st_size
    etag = f'"{st.st_ino:x}-{st.st_size:x}-{st.st_mtime_ns:x}"'
    headers = {
        "Content-Type": media_type,
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Accept-Ranges": "bytes",
        "ETag": etag,
        "Last-Modified": formatdate(st.st_mtime, usegmt=True),
        # 撤回语义下旧版本会被物理删除，禁止任何缓存留存
        "Cache-Control": "no-store",
    }

    # If-Range：验证器不匹配说明客户端持有的是旧版本，忽略 Range 回 200 全量，
    # 保证区间字节与正文一致
    range_header = request.headers.get("range")
    if range_header and "if-range" in request.headers:
        if request.headers["if-range"].strip() != etag:
            range_header = None

    status = 200
    start, length = 0, size
    try:
        parsed = _parse_single_range(range_header, size)
    except _RangeUnsatisfiable:
        os.close(fd)
        return Response(
            status_code=416,
            headers={
                **headers,
                "Content-Range": f"bytes */{size}",
                "Content-Length": "0",
            },
        )
    if parsed is not None:
        start, end = parsed
        length = end - start + 1
        status = 206
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    headers["Content-Length"] = str(length)

    if request.method == "HEAD":
        os.close(fd)
        return Response(status_code=status, headers=headers)

    # 定位到区间起点（句柄为本请求独占，seek 无副作用），随后顺序流式发送
    os.lseek(fd, start, os.SEEK_SET)
    return OpenedFileResponse(fd, length, status_code=status, headers=headers)
