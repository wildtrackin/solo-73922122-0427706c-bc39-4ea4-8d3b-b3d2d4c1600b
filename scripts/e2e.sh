#!/usr/bin/env bash
# 双实例端到端自测：模拟 docker-compose 中两个应用共用同一数据卷。
# 用法：bash scripts/e2e.sh [PORT1] [PORT2]
# 依赖：python3（含项目依赖，可用 PYTHONPATH 注入）、curl。
set -euo pipefail

PORT1="${1:-}"
PORT2="${2:-}"
if [[ -z "${PORT1}" ]]; then
  PORT1=$("${PYTHON:-python3}" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
fi
if [[ -z "${PORT2}" ]]; then
  PORT2=$("${PYTHON:-python3}" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
fi
BASE1="http://127.0.0.1:${PORT1}"
BASE2="http://127.0.0.1:${PORT2}"
E2E_DIR="$(mktemp -d)"
DATA_DIR="${E2E_DIR}/data"
mkdir -p "${DATA_DIR}"
LOG1="${E2E_DIR}/app1.log"
LOG2="${E2E_DIR}/app2.log"

PYBIN="${PYTHON:-python3}"
if [[ -n "${PYLIB:-}" ]]; then
  export PYTHONPATH="${PYLIB}"
fi

cleanup() {
  [[ -n "${PID1:-}" ]] && kill "${PID1}" 2>/dev/null || true
  [[ -n "${PID2:-}" ]] && kill "${PID2}" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

STEP_DATA_DIR="${DATA_DIR}" STEP_WORKER_POLL=0.2 \
  "${PYBIN}" -m uvicorn app.main:app --host 127.0.0.1 --port "${PORT1}" >"${LOG1}" 2>&1 &
PID1=$!
# 同一数据卷启动第二个实例
STEP_DATA_DIR="${DATA_DIR}" STEP_WORKER_POLL=0.2 \
  "${PYBIN}" -m uvicorn app.main:app --host 127.0.0.1 --port "${PORT2}" >"${LOG2}" 2>&1 &
PID2=$!

wait_http() {
  local url="$1" n=0
  until curl -sf "${url}/healthz" >/dev/null; do
    n=$((n + 1)); [[ ${n} -gt 50 ]] && { echo "启动超时: ${url}"; exit 1; }
    sleep 0.3
  done
}
wait_http "${BASE1}"
wait_http "${BASE2}"
echo "== 两个实例已启动，共享数据卷 ${DATA_DIR}"

fail() { echo "❌ $*"; exit 1; }

# 1. 实例1 导入批次 A（S001 x2 + S002 x1）
printf 'subject_id,date,steps\nS001,2026-11-01,10\nS001,2026-11-02,20\nS002,2026-11-01,30\n' \
  > "${E2E_DIR}/a.csv"
R=$(curl -sf -F "file=@${E2E_DIR}/a.csv" "${BASE1}/api/imports")
echo "== 导入批次 A: ${R}"
BATCH_A=$(printf '%s' "${R}" | sed 's/.*"batch_id":\([0-9]*\).*/\1/')
[[ "${BATCH_A}" =~ ^[0-9]+$ ]] || fail "批次号解析失败"

# 2. 实例2 导入批次 B（S001 + S003）
printf '受试者号,日期,步数\nS001,2026-11-10,5\nS003,2026-11-10,7\n' > "${E2E_DIR}/b.csv"
R=$(curl -sf -F "file=@${E2E_DIR}/b.csv" "${BASE2}/api/imports")
echo "== 导入批次 B: ${R}"
BATCH_B=$(printf '%s' "${R}" | sed 's/.*"batch_id":\([0-9]*\).*/\1/')

# 3. 实例1 发起 S001 撤回
R=$(curl -sf -H 'Content-Type: application/json' -d '{"subject_id":"S001"}' "${BASE1}/api/withdrawals")
echo "== 受理撤回: ${R}"

# 4. 轮询两个实例的进度接口直到 completed（验证共享状态）
for base in "${BASE1}" "${BASE2}"; do
  n=0
  until curl -sf "${base}/api/withdrawals/S001" | grep -q '"status":"completed"'; do
    n=$((n + 1)); [[ ${n} -gt 50 ]] && fail "撤回未在预期时间内完成（${base}）"
    sleep 0.3
  done
done
echo "== 两个实例均观察到撤回完成"

# 5. 下载校验：两批次中 S001 全部消失，其他人保留
check_batch() {
  local base="$1" bid="$2" expect="$3"
  curl -sf "${base}/api/batches/${bid}/download" -o "${E2E_DIR}/dl.parquet"
  PYTHONPATH="${PYLIB:-${PYTHONPATH:-}}" "${PYBIN}" - "${E2E_DIR}/dl.parquet" "${expect}" <<'PY'
import sys
import pyarrow.parquet as pq
path, expect = sys.argv[1], sys.argv[2]
subjects = sorted(set(pq.read_table(path, columns=["subject_id"]).column("subject_id").to_pylist()))
assert subjects == expect.split(","), f"{subjects} != {expect.split(',')}"
print(f"   批次内容校验通过: {subjects}")
PY
}
check_batch "${BASE1}" "${BATCH_A}" "S002"
check_batch "${BASE2}" "${BATCH_B}" "S003"

# 6. 撤回后两个实例都永久拒收含 S001 的整批
printf 'subject_id,date,steps\nS001,2026-12-01,1\nS009,2026-12-01,2\n' > "${E2E_DIR}/c.csv"
for base in "${BASE1}" "${BASE2}"; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -F "file=@${E2E_DIR}/c.csv" "${base}/api/imports")
  [[ "${code}" == "409" ]] || fail "期望 409 拒收，实际 ${code}（${base}）"
done
echo "== 两个实例均永久拒收含 S001 的整批（409）"

# 7. 不含 S001 的批次仍可在任一实例导入下载
printf 'subject_id,date,steps\nS009,2026-12-01,2\n' > "${E2E_DIR}/d.csv"
R=$(curl -sf -F "file=@${E2E_DIR}/d.csv" "${BASE2}/api/imports")
echo "== 干净批次仍可导入: ${R}"

# 8. 重复撤回接续原任务
ID1=$(curl -sf -H 'Content-Type: application/json' -d '{"subject_id":"S001"}' "${BASE1}/api/withdrawals" | sed 's/.*"id":\([0-9]*\).*/\1/')
ID2=$(curl -sf -H 'Content-Type: application/json' -d '{"subject_id":"S001"}' "${BASE2}/api/withdrawals" | sed 's/.*"id":\([0-9]*\).*/\1/')
[[ "${ID1}" == "${ID2}" ]] || fail "重复撤回产生了不同任务 ${ID1} != ${ID2}"
echo "== 重复撤回返回同一任务 #${ID1}"

# 9. 数据卷中不存在任何含 S001 的 parquet，且无 CSV 落盘
PYTHONPATH="${PYLIB:-${PYTHONPATH:-}}" "${PYBIN}" - "${DATA_DIR}" <<'PY'
import sys
import pyarrow.parquet as pq
from pathlib import Path
root = Path(sys.argv[1])
csvs = list(root.rglob("*.csv"))
assert not csvs, f"发现 CSV 落盘: {csvs}"
bad = []
for p in root.rglob("*.parquet"):
    if p.name.startswith(".tmp."):
        continue
    if "S001" in pq.read_table(p, columns=["subject_id"]).column("subject_id").to_pylist():
        bad.append(str(p))
assert not bad, f"仍有文件包含 S001: {bad}"
print(f"   卷内 Parquet 扫描通过（{len(list(root.rglob('*.parquet')))} 个文件，均不含 S001，无 CSV 落盘）")
PY

# 10. 超限文件被拒（>256KiB）
python3 -c "open('${E2E_DIR}/big.csv','wb').write(b'subject_id,date,steps\n'+b'S1,2026-01-01,1\n'+b'x'*300000)"
code=$(curl -s -o /dev/null -w '%{http_code}' -F "file=@${E2E_DIR}/big.csv" "${BASE1}/api/imports")
[[ "${code}" == "413" ]] || fail "超限文件期望 413，实际 ${code}"
echo "== 超过 256 KiB 的上传被拒绝（413）"

echo ""
echo "✅ 端到端全部通过。数据卷保留在: ${DATA_DIR}"
trap - EXIT
cleanup
echo "（实例已停止）"
