#!/usr/bin/env bash
# 崩溃重启接续验证：
#   1. 导入含 S001/S002 的批次并受理撤回；
#   2. 用故障注入让 worker 在重写中途抛错（模拟进程退出），任务停在 failed/rewriting；
#   3. 重新执行启动恢复 + worker，必须接续完成，且卷内不再含 S001。
set -euo pipefail

E2E_DIR="$(mktemp -d)"
DATA_DIR="${E2E_DIR}/data"
mkdir -p "${DATA_DIR}"
export STEP_DATA_DIR="${DATA_DIR}"
export STEP_WORKER_ENABLED=0
export PYTHONPATH="${PYLIB:-${PYTHONPATH:-}}"
PYBIN="${PYTHON:-python3}"

"${PYBIN}" - <<'PY'
import csv, io
import pyarrow as pa
from app import config, db, services
from app.parquet_store import parse_csv

config.ensure_dirs()
db.init_db()
services.recover_on_startup()

csv_bytes = ("subject_id,date,steps\n"
             "S001,2026-11-01,1\nS001,2026-11-02,2\nS002,2026-11-01,3\n").encode()
services.publish_import(parse_csv(csv_bytes))
print("阶段1：批次已发布")

services.accept_withdrawal("S001")

# 故障注入：重写第一次必定失败，模拟处理中进程崩溃
import app.services as svc
real = svc.rewrite_without_subject
calls = {"n": 0}

def broken(src, dst, subject):
    calls["n"] += 1
    raise RuntimeError("simulated crash")

svc.rewrite_without_subject = broken
try:
    services.process_due_withdrawal()
except RuntimeError:
    pass
svc.rewrite_without_subject = real

w = services.get_withdrawal("S001")
assert w["status"] == "failed", w
b = db.query_one("SELECT status FROM batches WHERE id=1")
assert b["status"] == "rewriting", b
print("阶段2：处理中崩溃，任务 failed、批次 rewriting，未误报完成")
PY

# 阶段3：全新“进程”启动（同一数据卷），恢复并接续
"${PYBIN}" - <<'PY'
from app import config, db, services
from app.parquet_store import list_parquet_files, parquet_contains_subject

config.ensure_dirs()
db.init_db()
rec = services.recover_on_startup()
print("阶段3a：启动恢复结果", rec)

for _ in range(20):
    if not services.process_due_withdrawal():
        break

w = services.get_withdrawal("S001")
assert w["status"] == "completed", w
assert w["batches_done"] == 1, w
for p in list_parquet_files():
    assert not parquet_contains_subject(p, "S001"), p
print("阶段3b：重启后接续完成，卷内无 S001 残留；files_removed =", w["files_removed"])

# 下载恢复可用且只剩 S002
import pyarrow.parquet as pq
row, path = services.get_downloadable_batch(1)
t = pq.read_table(path)
assert t.column("subject_id").to_pylist() == ["S002"]
print("阶段3c：批次下载恢复，内容 =", t.to_pydict())
PY

# 阶段4：再次“重启”，已完成任务保持 completed，不会重复处理
"${PYBIN}" - <<'PY'
from app import db, services
db.init_db()
services.recover_on_startup()
did = services.process_due_withdrawal()
w = services.get_withdrawal("S001")
assert did is False and w["status"] == "completed"
print("阶段4：再次重启不重复处理，任务保持 completed")
PY

echo ""
echo "✅ 崩溃重启接续验证全部通过（临时目录: ${E2E_DIR}）"
