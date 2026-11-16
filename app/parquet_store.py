"""Parquet 读写与 CSV 内存解析。

CSV 只在内存字节流中解析，绝不落盘；落盘的只有 Parquet。
"""
from __future__ import annotations

import datetime as dt
import io
import os
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import config

SUBJECT_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")
STEPS_RE = re.compile(r"^[0-9]+$")

SCHEMA = pa.schema(
    [
        pa.field("subject_id", pa.string(), nullable=False),
        pa.field("date", pa.date32(), nullable=False),
        pa.field("steps", pa.int64(), nullable=False),
    ]
)

# 同时支持中英文表头
HEADER_ALIASES = {
    "subject_id": "subject_id",
    "受试者号": "subject_id",
    "受试者": "subject_id",
    "date": "date",
    "日期": "date",
    "step_date": "date",
    "steps": "steps",
    "步数": "steps",
    "step_count": "steps",
}


class ValidationError(ValueError):
    """CSV 内容或结构不合法。"""


def parse_csv(data: bytes) -> pa.Table:
    """解析内存中的 CSV 字节，校验后返回 Parquet 用的 Arrow 表。"""
    if len(data) > config.MAX_UPLOAD_BYTES:
        raise ValidationError(
            f"文件超过 {config.MAX_UPLOAD_BYTES} 字节限制（实际 {len(data)} 字节）"
        )
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValidationError("CSV 必须为 UTF-8 编码") from exc

    import csv

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        raise ValidationError("空文件：缺少表头")

    cols = [h.strip().lower() for h in header]
    mapped = [HEADER_ALIASES.get(h) for h in cols]
    if len(cols) != 3 or any(m is None for m in mapped) or len(set(mapped)) != 3:
        raise ValidationError(
            "表头必须且仅包含三列：subject_id,date,steps（或 受试者号,日期,步数）"
        )
    idx = {name: mapped.index(name) for name in ("subject_id", "date", "steps")}

    subjects: list[str] = []
    dates: list[dt.date] = []
    steps: list[int] = []
    row_no = 1
    for raw in reader:
        row_no += 1
        if not raw or all(not c.strip() for c in raw):
            continue  # 跳过空行
        if len(raw) != 3:
            raise ValidationError(f"第 {row_no} 行：列数应为 3，实际 {len(raw)}")
        subject = raw[idx["subject_id"]].strip()
        date_s = raw[idx["date"]].strip()
        steps_s = raw[idx["steps"]].strip()

        if not SUBJECT_RE.match(subject):
            raise ValidationError(f"第 {row_no} 行：受试者号不合法（1-64 位字母数字 _.-）：{subject!r}")
        try:
            parsed_date = dt.date.fromisoformat(date_s)
        except ValueError as exc:
            raise ValidationError(f"第 {row_no} 行：日期必须为 YYYY-MM-DD：{date_s!r}") from exc
        if not STEPS_RE.match(steps_s):
            raise ValidationError(f"第 {row_no} 行：步数必须为非负整数：{steps_s!r}")
        parsed_steps = int(steps_s)
        if parsed_steps > 2**63 - 1:
            raise ValidationError(f"第 {row_no} 行：步数超出 int64 范围")

        subjects.append(subject)
        dates.append(parsed_date)
        steps.append(parsed_steps)

    if not subjects:
        raise ValidationError("CSV 没有任何数据行")
    if len(subjects) > config.MAX_ROWS:
        raise ValidationError(f"每批最多 {config.MAX_ROWS} 行（实际 {len(subjects)} 行）")

    table = pa.table(
        {"subject_id": subjects, "date": dates, "steps": steps},
        schema=SCHEMA,
    )
    return table


def distinct_subjects(table: pa.Table) -> list[str]:
    return sorted(pc.unique(table.column("subject_id")).to_pylist())


def atomic_write_parquet(table: pa.Table, final_path: Path) -> None:
    """同目录临时文件 + fsync + 原子 rename，崩溃不会留下半截正式文件。"""
    final_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = final_path.parent / f".tmp.{os.getpid()}.{final_path.name}"
    pq.write_table(table, tmp_path, compression="snappy")
    fd = os.open(tmp_path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp_path, final_path)
    # fsync 目录，保证 rename 在断电后仍可持久
    dfd = os.open(final_path.parent, os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def rewrite_without_subject(src_path: Path, final_path: Path, subject_id: str) -> int:
    """重写批次：剔除该受试者全部行，返回保留行数。"""
    table = pq.read_table(src_path, schema=SCHEMA)
    mask = pc.not_equal(table.column("subject_id"), subject_id)
    kept = table.filter(mask)
    atomic_write_parquet(kept, final_path)
    return kept.num_rows


def parquet_contains_subject(path: Path, subject_id: str) -> bool:
    table = pq.read_table(path, columns=["subject_id"])
    return pc.any(pc.equal(table.column("subject_id"), subject_id)).as_py() is True


def list_parquet_files() -> list[Path]:
    files: list[Path] = []
    for root in (config.BATCHES_DIR, config.STAGING_DIR):
        if not root.exists():
            continue
        for p in root.rglob("*.parquet"):
            if p.is_file() and not p.name.startswith(".tmp."):
                files.append(p)
    return files
