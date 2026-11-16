"""全局配置：所有路径都位于共享数据卷内，便于两个实例共用。"""
from __future__ import annotations

import os
from pathlib import Path

# 共享数据卷根目录（docker-compose 挂载的命名卷）
DATA_DIR = Path(os.environ.get("STEP_DATA_DIR", "/data"))
DB_PATH = DATA_DIR / "stepstore.db"
BATCHES_DIR = DATA_DIR / "batches"
STAGING_DIR = DATA_DIR / "staging"
LOCK_DIR = DATA_DIR / "locks"
PUBLISH_LOCK = LOCK_DIR / "publish.lock"
WITHDRAW_LOCK = LOCK_DIR / "withdraw.lock"

# 上传限制：每批不超过 256 KiB、1000 行
MAX_UPLOAD_BYTES = 256 * 1024
MAX_ROWS = 1000

# 后台 worker 轮询间隔与失败退避
WORKER_ENABLED = os.environ.get("STEP_WORKER_ENABLED", "1") not in ("0", "false", "False")
WORKER_POLL_SECONDS = float(os.environ.get("STEP_WORKER_POLL", "0.5"))
WORKER_BACKOFF_SECONDS = 3.0

PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"


def ensure_dirs() -> None:
    for p in (DATA_DIR, BATCHES_DIR, STAGING_DIR, LOCK_DIR):
        p.mkdir(parents=True, exist_ok=True)
