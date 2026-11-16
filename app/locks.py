"""跨进程文件锁（fcntl.flock），保证两个应用实例互斥。"""
from __future__ import annotations

import fcntl
import os
import time
from contextlib import contextmanager
from typing import Iterator


@contextmanager
def file_lock(path: str | os.PathLike[str], timeout: float | None = None) -> Iterator[None]:
    """阻塞式排他锁。timeout=None 时一直等待；超时抛 TimeoutError。"""
    path = os.fspath(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        if timeout is None:
            fcntl.flock(fd, fcntl.LOCK_EX)
        else:
            deadline = time.monotonic() + timeout
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"lock timeout: {path}")
                    time.sleep(0.1)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
