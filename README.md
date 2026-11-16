# 步数数据湖 · 受试者撤回（被遗忘权）管理服务

运动研究数据管理员在受试者撤回后，需要从分析用数据湖中彻底清除其步数明细。
本服务用 **FastAPI + PyArrow + SQLite** 实现，支持：

- 上传每批 **≤ 256 KiB、≤ 1000 行** 的 CSV（受试者号、日期、非负整数步数），
  **CSV 仅在内存解析**，落盘的只有 Parquet（Snappy 压缩）；
- 按批次下载 Parquet；
- 按受试者号发起撤回并查看进度；
- 受理后**永久拒收**任何含该受试者的整批上传；受理前已开始但尚未发布的导入同样被清除；
- 撤回时重写受影响批次（版本化文件 + 原子替换），完整保留其他人的行；
- 重写期间禁止这些批次的**新**下载，**已开始**的下载因持有旧文件句柄可正常读完；
- 共享卷内含该受试者的旧版本与暂存件全部清除并复核后，才把任务标记为完成；
- 写入失败或进程退出后重启可接续，不会恢复旧下载、不会误报完成；重复撤回接续同一任务；
- Docker Compose 启动**共用一个数据卷的两个应用实例**，浏览器在任一端口均可操作。

## 目录结构

```
app/
  config.py          # 路径与上传限制（全部位于共享数据卷）
  locks.py           # 跨进程 fcntl.flock 互斥锁
  db.py              # SQLite（WAL）连接、schema 与并发初始化重试
  parquet_store.py   # CSV 内存解析/校验、Parquet 原子写、批次重写、卷扫描
  services.py        # 发布、撤回受理、后台清理、崩溃恢复（核心业务）
  main.py            # FastAPI 路由、后台 worker、内嵌单页前端
tests/               # 23 个 pytest 用例（校验/撤回/恢复/并发）
scripts/
  e2e.sh             # 双实例共享卷端到端自测
  restart_recovery.sh# 崩溃 → 重启 → 接续完成验证
Dockerfile
docker-compose.yml   # app1:8001、app2:8002，共享命名卷 stepdata
```

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 浏览器访问 http://localhost:8001 或 http://localhost:8002
```

两个容器挂载同一命名卷 `stepdata` 到 `/data`，其中包含：

```
/data/
  stepstore.db(-wal/-shm)      # SQLite 数据库（WAL 模式）
  locks/{publish,withdraw}.lock# 跨进程文件锁
  staging/import-<uuid>.parquet# 暂存 Parquet（发布成功即移走）
  batches/batch-00000001/
      v1.parquet               # 版本化批次文件，撤回重写生成 v2、v3…
```

## 本地运行（无 Docker）

```bash
pip install -r requirements.txt
STEP_DATA_DIR=./data uvicorn app.main:app --port 8000
# 第二个实例换端口、同一 STEP_DATA_DIR 即可模拟双实例
STEP_DATA_DIR=./data uvicorn app.main:app --port 8001
```

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/imports` | multipart 上传 CSV；>256 KiB 返回 413，内容非法 422，命中撤回 409 |
| GET  | `/api/batches` | 批次列表（含状态、行数、受试者） |
| GET  | `/api/batches/{id}/download` | 下载 Parquet；重写中返回 409 |
| POST | `/api/withdrawals` | `{"subject_id":"S001"}` 受理撤回（重复调用返回同一任务） |
| GET  | `/api/withdrawals` | 全部撤回任务与进度 |
| GET  | `/api/withdrawals/{subject_id}` | 单个任务进度 |
| GET  | `/healthz` | 健康检查与本次启动恢复统计 |

CSV 表头支持英文 `subject_id,date,steps` 与中文 `受试者号,日期,步数`。

## 关键设计与正确性论证

### 1. 受理与发布的全序（永久拒收 + 在途导入）

- `publish_import`（导入发布）与 `accept_withdrawal`（撤回受理）都先获取
  **同一把 `publish.lock` 排他锁**，因此二者在系统中存在确定先后顺序：
  - 发布先于受理：该批进入数据湖，但会被随后的撤回重写清除；
  - 受理先于发布：发布在锁内查询 `withdrawals` 命中，整批 **409 拒绝**，不落任何批次数据。
- “受理前开始但尚未发布”的暂存导入：撤回最终清扫阶段扫描 `staging/`，
  含该受试者的暂存 Parquet 直接删除，对应 `imports` 行标记 `rejected`。

### 2. 批次重写与下载封锁

- 撤回处理把批次在同一事务内翻转为 `rewriting`，下载接口只对 `published`
  放行，因此重写窗口内的**新**下载得到 409；崩溃停在 `rewriting` 时旧下载同样无法恢复。
- 重写采用“写新版本 `vN.parquet`（同目录临时文件 + fsync + `os.replace`）→
  事务内切换 `current_path` → 删除旧版本”。已经开始的下载持有旧文件的打开句柄，
  Unix 语义下 unlink 不影响其读完（有测试覆盖）。

### 3. 完成前的强制清扫与复核

任务只有在以下全部完成后才置为 `completed`：

1. 每个受影响批次重写并剔除该受试者（保留其他人全部行）；
2. 删除批次目录中的历史旧版本；删除/拒收含该受试者的暂存件；
3. 对共享卷 `batches/` 与 `staging/` 内**所有** Parquet 复核，
   只要还有一个文件含该受试者就抛错并保持 `failed`，**绝不误报完成**。

### 4. 崩溃恢复（重启接续）

启动时 `recover_on_startup()`：

- 清理 `.tmp.*` 半截写入；
- 无关联批次/文件丢失的暂存导入标记 `rejected` 并删除文件；
- `rewriting` 批次保持封锁，由后台 worker 基于当前版本**幂等重写**接续；
- 清理无数据库引用的孤儿 Parquet；
- 已 `completed` 的任务不会被重复处理；重复 POST 撤回返回同一任务行。

后台 worker 两个实例各跑一个，通过 `withdraw.lock` 保证同一时刻只有一个实例执行清理，
另一个实例自动空转；任务失败会保留 `last_error` 并在后续轮询重试。

### 5. CSV 不落盘

上传接口最多读取 256 KiB+1 字节到内存，随后用 `io.StringIO` 在内存解析；
写入磁盘的第一个产物是 `staging/import-<uuid>.parquet`。测试断言数据卷内不存在任何 `.csv`。

## 自测

```bash
pip install -r requirements.txt pytest httpx
pytest tests/ -q
PYLIB=$(python -c 'import site;print(site.getsitepackages()[0])') bash scripts/e2e.sh
PYLIB=... bash scripts/restart_recovery.sh
```

测试覆盖：上传大小/行数/字段/编码校验、CSV 不落盘、撤回重写内容正确性、
重写窗口 409 封锁、已开始下载不受影响、旧版本清理与全卷复核、
重复撤回幂等、在途暂存导入被清扫、模拟崩溃后重启接续、双 worker 锁互斥、
受理/发布并发全序等。
