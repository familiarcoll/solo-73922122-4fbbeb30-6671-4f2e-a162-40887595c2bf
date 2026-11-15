# Feature Store Withdrawal API

训练特征库的**一次性建库**与**按受试者（subject_id）撤回授权**服务。FastAPI + SQLite，
CSV 存共享卷，SQLite 只存索引和状态，Docker Compose 双实例部署。

## 功能契约

| 需求 | 实现 |
| --- | --- |
| 首次上传 1–3 份 CSV（`subject_id,sample_id,value`，每份 ≤1000 行）建库，此后不再追加 | `POST /ingest`，全部校验通过后单事务建库；二次上传返回 `409` |
| CSV 存共享卷，SQLite 只存索引和状态 | 原始 CSV 存 `/data/ingest`，分片存 `/data/shards/current`，暂存 `/data/shards/staging`；DB 不存 value |
| 按 subject_id 撤回并返回可查询任务号 | `POST /withdrawals?subject_id=...` 返回 `task_id`，`GET /withdrawals/{task_id}` 可查进度 |
| 受理后新下载立即停供含该人的分片 | 创建任务的同一事务把受影响分片置 `WITHHELD` 并先提交，下载接口据此返回 `409` |
| 已开始下载可完成，其他分片仍可下载 | 停供只查数据库状态，不删正在传输的文件；未受影响分片状态不变，持续 `200` |
| 重写分片只删目标行 | 净化分片由 SQLite 索引 + 源 CSV **重建**，显式过滤撤回 subject，其余行内容与顺序保持 |
| 完成前清旧件/暂存再开放净化分片 | 换片顺序：写暂存(fsync) → 删旧件与过期暂存 → rename 落位 → 事务删索引并按条件恢复 `AVAILABLE` |
| 双实例并发撤回不同人不得相互恢复数据 | 所有写操作走 `BEGIN IMMEDIATE` 串行化 + 分片租约；分片仅在"无未完成撤回"时才恢复可用，净化数据永远由当前索引重建 |
| 滚动重启：新实例不得清理另一在用实例的文件 | 启动恢复只回收**心跳超时**的租约；活动租约与其暂存文件一律保留，新实例启动即可查询任务，不等待任何在途净化 |
| 迟到旧工作不得删除/覆盖接手方结果 | 租约带进程级 `incarnation` 围栏：暂存、换片、改索引、源文件净化前都在写事务内复核所有权，失主即抛 `LeaseLostError` 放弃；换片用 `os.replace` 原子覆盖，绝不先 unlink 成品 |
| 真中断后自动接续，未净化完保持停供 | 崩溃者租约心跳超时后任意实例可接管续做；完成前分片在 DB 始终 `WITHHELD`，下载持续 `409`，完成后恢复 `200` |
| 写盘失败/进程中断后受影响分片保持停供，重启续做 | 换片前后分片在 DB 中始终 `WITHHELD`；启动恢复清租约、回收 `RUNNING`/`PROCESSING` 状态、清暂存，后台工作器重试 |
| 重复撤回复用任务 | 同 subject 第二次请求直接返回首个 `task_id`（`reused=true`），不新建任务 |
| Docker Compose 双实例 + /docs | 见 `docker-compose.yml`；根路径 `/` 重定向到 `/docs`（Swagger UI） |

## 目录结构

```
app/
  config.py    # 数据目录、限额、租约/超时、测试开关
  db.py        # 连接（WAL、busy_timeout）、schema、事务
  storage.py   # CSV 校验/渲染、原子写、暂存换片、源文件净化
  ingest.py    # 一次性建库：校验 -> 源 CSV -> 分片 -> 索引（单事务，失败回滚清理）
  worker.py    # 撤回任务创建、分片租约、净化工作器、启动恢复
  models.py    # 响应模型
  main.py      # FastAPI 路由与后台工作线程
tests/         # 13 个端到端测试（真实 uvicorn 子进程，含双实例与崩溃恢复）
scripts/demo.sh
Dockerfile
docker-compose.yml
```

## 快速开始（Docker）

```bash
docker compose up --build
# 实例 A: http://localhost:8001/docs
# 实例 B: http://localhost:8002/docs
# 两者挂载同一个 named volume（/data），通过 SQLite 事务和分片租约协调
```

共享卷布局：

```
/data
  app.db                 # SQLite：索引 + 状态（WAL 模式）
  ingest/source-01.csv   # 原始上传 CSV（撤回时原地净化，原子替换）
  shards/current/shard-0000.csv
  shards/staging/        # 净化暂存，发布后清空；崩溃残留启动时清理
```

## 快速开始（本地）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --port 8000
# 打开 http://localhost:8000/docs
```

## API 摘要

| 方法/路径 | 说明 |
| --- | --- |
| `POST /ingest` | multipart 上传 1–3 个 CSV 建库（仅一次，重复 `409`） |
| `POST /withdrawals?subject_id=...` | 创建/复用撤回任务，`202` + `task_id` |
| `GET /withdrawals/{task_id}` | 任务状态与逐分片进度 |
| `GET /shards` | 全部分片状态 |
| `GET /shards/{id}/status` | 单分片状态 |
| `GET /shards/{id}/download` | 下载分片；`WITHHELD` 立即 `409` |
| `GET /healthz` | 健康检查、实例 ID、是否已建库 |
| `GET /docs` | Swagger UI（演示入口） |

任务状态机：`PENDING → PROCESSING → COMPLETED`（无匹配样本时直接 `COMPLETED`）。
分片状态：`AVAILABLE ⇄ WITHHELD`。

## 并发与崩溃安全设计

1. **事务串行化**：所有数据库写操作使用 `BEGIN IMMEDIATE`，两实例在 SQLite 写锁上排队；
   建库的"仅一次"由 `meta.ingested` 在同一事务内保证（并发建库只有一个 201，另一个 409）。
2. **先停供后处理**：撤回受理事务先把分片置 `WITHHELD` 并提交，后台净化随后进行；
   下载是否放行只取决于数据库状态，因此受理后新请求立即被拒。
3. **分片租约**：`shard_leases` 行级互斥（`INSERT OR IGNORE`）保证同一分片同一时刻只有一个
   工作器在净化；带心跳超时，崩溃后可被其他实例接管。
4. **净化只删目标行且永不复活数据**：新分片内容始终由**当前索引**重建并显式排除撤回主体，
   不依赖旧分片文件；分片恢复 `AVAILABLE` 的条件是该分片已无任何未完成撤回。两个不同主体的
   并发撤回因此无法互相把对方的数据写回。
5. **换片顺序与围栏**：暂存文件 `tmp+fsync` 原子落盘 → 在写事务内复核本实例仍持有租约 →
   `os.replace` 原子覆盖成品（目录 fsync），**绝不先 unlink 下载件**，下载者只会看到旧文件
   或新文件，不会遇到缺失路径。若中途失败或租约已失，DB 仍为 `WITHHELD`，下载保持停供。
6. **启动恢复（滚动重启安全）**：只清理**心跳早于 `LEASE_TIMEOUT_SECONDS`** 的死租约，并把
   这些死租约名下的 `RUNNING` 分片/任务回收为 `PENDING`；活动实例的租约、暂存文件一律不动，
   孤儿暂存也只在"无活动租约且文件早于超时"时才回收。新实例启动后立刻可查任务，无需等待
   在途净化。真正崩溃的实例租约到期后由任意实例自动接管续做。
7. **租约围栏（fencing）**：`shard_leases` 记录 `owner` + 进程唯一 `incarnation`。每个触碰
   文件的步骤（写暂存、换片、删索引、源 CSV 重写）前后都在 `BEGIN IMMEDIATE` 事务内校验
   `(owner, incarnation)` 仍匹配，失主即抛 `LeaseLostError` 并静默放弃，因此一个醒来的旧工作
   永远无法删除或覆盖接手方已发布的结果。
8. **源 CSV 净化**：任务全部分片完成后，在单个写事务区间内按 subject 过滤、原子重写源 CSV，
   两实例在 SQLite 写锁上串行，杜绝并发撤回互相覆盖源文件；索引按 `sample_id` 映射源行，
   不受行号变化影响。

> 共享卷需支持 POSIX 文件锁与 `fsync`（Docker named volume / 块存储均可；某些 NFS 默认
> 配置下锁语义较弱，生产应选用支持锁的卷驱动）。

## 测试

```bash
pip install pytest httpx
pytest -q
```

16 个测试全部使用真实 uvicorn 子进程：

- `test_api.py`（7）：文档入口、上传限额/校验、完整撤回流程、重复撤回复用、
  未知主体空任务、1000 行边界与建库封闭、重写只删目标行并保持顺序
- `test_concurrency.py`（3）：两实例并发撤回不同主体、二次撤回不复活首次数据、
  并发建库只有一个赢家
- `test_recovery.py`（3）：净化中途硬退出（`os._exit` 模拟 SIGKILL）后重启续做、
  共享卷只读（写盘失败）时分片持续停供且恢复后自动完成、无待办时重启干净
- `test_rolling_restart.py`（2）：A 净化停滞、B 在租约超时后接管完成，A 醒来不得删除或
  覆盖 B 的成品（复现题面"任务 COMPLETED/分片 AVAILABLE/下载 409"缺陷）；新实例启动不得
  回收活动实例的暂存、且立即能查任务

测试管理端点 `/test-admin/stop`、`/test-admin/hard-exit`、`/test-admin/stall` 仅在
`ENABLE_TEST_ADMIN=1` 时注册（`stall` 还需 `PURGE_STALL_SECONDS>0`），生产镜像不设置这些
变量即不存在、不可触发。

## 已知限制

- 单库上限 3 文件 × 1000 行（按题面约定）；分片目标行数可用 `SHARD_TARGET_ROWS` 调整。
- 当前环境无 Docker 守护进程，`docker compose up --build` 未在本机实跑；compose 文件已通过
  YAML 渲染校验，双实例行为用两个共享同一 `DATA_DIR` 的真实 uvicorn 进程做了等价端到端实测
  （冷启动并行建库、上传/撤回/查询/下载、停滞-接管-复活不破坏成品）。
