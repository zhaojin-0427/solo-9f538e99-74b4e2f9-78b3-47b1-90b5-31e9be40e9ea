# 物流分拣扫码日志分区服务

按 UTC 日期分区管理扫码日志的 FastAPI 服务：**SQLite 管文件索引，事件以本地 NDJSON 文件存储**，
支持按日上传、按日流式读取与分区压实，面向「小文件越来越多、查询越来越慢」的场景。

## 目录结构

```
.
├── app/
│   ├── config.py      # 环境变量配置（DATA_DIR / MAX_UPLOAD_BYTES / REAP_WAIT_SECONDS）
│   ├── storage.py     # 核心：SQLite 索引 + NDJSON 文件、快照读取、压实、故障恢复
│   └── main.py        # FastAPI 路由
├── tests/             # 25 个 pytest 用例（拒收、压实不变性、并发、故障注入、重启恢复）
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── requirements-dev.txt
```

数据目录（默认 `/data`，命名卷 `log-data` 持久化）：

```
/data
├── index.sqlite3        # 文件索引（WAL 模式）
├── events/              # 已发布/压实后的 NDJSON 数据文件
├── tmp/                 # 上传与压实的临时文件（原子 rename 前）
└── quarantine/          # 索引切换失败时隔离的数据文件（绝不静默删除用户数据）
```

## 快速启动

```bash
docker compose up --build -d
curl http://localhost:8000/healthz
```

本地运行：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

运行测试：

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -v
```

## 事件格式（NDJSON，每行一个 JSON 对象）

```json
{"event_id": "E-0001", "scanned_at": "2026-11-14T08:30:00Z", "chute": "A-01"}
```

* `event_id`、`chute`：非空字符串；`event_id` 允许重复（重复按多条事件保留，压实不去重）。
* `scanned_at`：ISO-8601 且**必须显式为 UTC**（`Z` 或 `+00:00`），其他时区/无时区一律拒收。
* 每行必须以换行符结尾，不允许空行。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/partitions/{day}/logs` | 上传某日日志（`application/x-ndjson` 原始字节流） |
| `GET`  | `/partitions/{day}/events` | 流式读取该日完整流水（NDJSON） |
| `POST` | `/partitions/{day}/compact` | 压实该日分区 |
| `GET`  | `/partitions/{day}/stats` | 分区索引与计数 |
| `GET`  | `/healthz` | 健康检查 |

`{day}` 为 UTC 日期，形如 `2026-11-14`。

### 示例

```bash
# 上传
curl -X POST http://localhost:8000/partitions/2026-11-14/logs \
     -H 'Content-Type: application/x-ndjson' \
     --data-binary @scan-2026-11-14.ndjson

# 读取（响应头 X-Partition-Files 给出快照包含的文件数）
curl http://localhost:8000/partitions/2026-11-14/events

# 压实
curl -X POST http://localhost:8000/partitions/2026-11-14/compact

# 计数与文件清单
curl http://localhost:8000/partitions/2026-11-14/stats
```

状态码：成功上传 `201`；混入其他日期/非法行 `422`（**整份拒收**，响应体给出行号与原因）；
超过体积上限 `413`；日期格式错误 `400`；压实完整性失败 `500`（分区保持压实前状态）。

## 关键设计与一致性保证

### 1. 上传：整份校验、原子发布、整份拒收

* 请求体先假脱机（≤1 MiB 在内存，超出自动落临时文件），再**逐行流式校验**到
  `tmp/` 下的临时文件并实时累计 sha256 与行数；任何一行日期不属于目标分区（或 JSON/字段/时区非法）
  立即中止并删除临时文件，**该文件不会有任何一行进入分区**。
* 全部通过后 `fsync` + 原子 `rename` 到 `events/`，再向 SQLite 插入索引（WAL + `synchronous=FULL`）。
* 若索引写入失败：数据文件移入 `quarantine/` 保留并附原因，已发布的旧数据完全不受影响、仍可读。

### 2. 读取：只看到某个完整分区状态（快照隔离）

* 读取开始时在短锁内取出该分区当前全部 `published` 文件清单作为**不可变快照**，
  随后无锁流式回放。压实进行中的索引切换不会影响已经开始的读取：
  读者要么看到压实前的完整状态，要么（新读者）看到压实后的完整状态，不会看到混合或半写状态。
* 读取会话登记在活动集合中，会话结束前其引用的旧文件不会被回收。

### 3. 压实：事件出现次数不变，切换成功才回收旧文件

* 取分区当前全部 `published` 文件为快照，顺序合并到临时文件，**不去重、不重排单行内容**，
  逐源文件核对行数，合并后再整体回读第二遍核对总行数与 sha256。
* 校验通过后原子 `rename`，并在**单事务**内完成「插入合并文件 + 快照内旧文件置为 superseded」：
  - 事务内重新确认快照中的旧文件仍全部是 `published` 且行数总和一致，
    压实期间并发的**新上传不在快照内，保持 published，绝不丢失**；
  - 事务失败则回滚、合并文件隔离，旧文件全部继续可读。
* 仅在索引切换成功后才回收旧数据文件：等待引用它们的在途读取结束
  （最多等 `REAP_WAIT_SECONDS`，超时留给下次压实或重启时回收），避免删掉正在读的文件。
* 压实用 `_compact_lock` 串行化同分区任务；不同分区互不影响。

### 4. 崩溃恢复

进程启动时执行 sweep：清理 `tmp/` 残留临时文件，并回收索引中已 `superseded` 的遗留数据文件；
`published` 文件与 SQLite 索引均为 fsync/原子操作落盘，重启后数据完整。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `/data` | 数据根目录 |
| `MAX_UPLOAD_BYTES` | `104857600`（100 MiB） | 单次上传字节上限，超出返回 413 |
| `REAP_WAIT_SECONDS` | `30` | 压实后等待在途读取结束再回收旧文件的最长秒数 |

## 部署形态说明

当前实现面向**单实例 + 持久化卷**部署（SQLite 与本地 NDJSON 的天然适用形态），
所有并发安全由进程内锁与 SQLite 事务保证。若未来需要多副本水平扩展，
应把数据目录换成共享文件系统/NFS 或对象存储，并以 SQLite 单写者或外置数据库协调索引切换。
