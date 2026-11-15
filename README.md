# 双端 FASTQ.gz 接收 API

测序数据员把仪器导出的双端 `FASTQ.gz` 交给比对流水线前的单进程接收服务：按样本建批，
R1、R2 各以请求体**流式上传一次**，实时守字节上限、逐对校验读段，错配样本一律不放行。

`ready` 批还可调用**一次性拆样 API**：提交两个不同样本号及等长 ACGT 条码，服务按
Illumina 头部空格后的条码把同 lane 混样读对拆给各样本，返回各样本与未归样读对数，
并发布各样本的 R1、R2 下载。拆样结果与原档共用同一单批字节上限。

- 框架：FastAPI + Uvicorn（单进程、单事件循环）
- 存储：SQLite（WAL 模式）+ 数据卷文件
- 交互文档：启动后访问 `http://localhost:8000/docs`（Swagger UI）与 `/openapi.json`

## 1. 启动

### Docker Compose（推荐）

```bash
docker compose up --build
```

数据保存在命名卷 `fastq-data`（容器内 `/data`），SQLite、压缩临时文件、展开后的 FASTQ 都在其中。
单批字节上限通过环境变量 `FASTQ_BATCH_BYTE_LIMIT` 配置（默认 64 MiB）。

### 本地直接运行

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
FASTQ_DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 配置项（均为环境变量，前缀 `FASTQ_`）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `FASTQ_DATA_DIR` | `/data` | 数据卷根目录（SQLite + 批次文件） |
| `FASTQ_BATCH_BYTE_LIMIT` | `67108864`（64 MiB） | 单批文件字节上限，压缩临时文件与展开 FASTQ 合计 |
| `FASTQ_UPLOAD_IDLE_TIMEOUT` | `15`（秒） | 上传请求体两个分块之间的最大静默时间 |

## 2. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/batches` | 按 `sample_id` 创建批次，返回批次 ID |
| `GET` | `/batches` | 列出全部批次 |
| `GET` | `/batches/{id}` | 查询批次状态、已传文件与实时占用字节 |
| `POST` | `/batches/{id}/files/R1` | 流式上传 R1（请求体即 gzip 字节，非 multipart） |
| `POST` | `/batches/{id}/files/R2` | 流式上传 R2 |
| `POST` | `/batches/{id}/cancel` | 取消批次，等待写入停止、文件删除、空间释放后确认 |
| `GET` | `/batches/{id}/files/R1/download` | 下载展开后的 R1（仅 `ready` 批） |
| `GET` | `/batches/{id}/files/R2/download` | 下载展开后的 R2（仅 `ready` 批） |
| `POST` | `/batches/{id}/demux` | 对 `ready` 批一次性拆样（并发最多成功一次，失败可重试） |
| `GET` | `/batches/{id}/demux` | 查询拆样状态、各样本与未归样读对数、下载地址 |
| `GET` | `/batches/{id}/demux/samples/{sample_id}/files/R1/download` | 下载已发布样本的拆样 R1 |
| `GET` | `/batches/{id}/demux/samples/{sample_id}/files/R2/download` | 下载已发布样本的拆样 R2 |
| `GET` | `/batches/{id}/demux/unassigned/files/R1/download` | 下载未归样 R1 |
| `GET` | `/batches/{id}/demux/unassigned/files/R2/download` | 下载未归样 R2 |
| `GET` | `/health` | 健康检查 |

批次状态机：`receiving` →（两端齐全且配对通过）→ `ready`；任何校验失败 → `failed`；
操作员取消 → `cancelled`。三个终态都不可再上传。拆样只读原档，**不改变批次状态**，
原档下载始终可用。

## 3. 校验规则（任一不过，整批拒收并清理）

1. **单成员 gzip**：流式解压，gzip 结束标记后不允许再带数据（多成员归档拒绝）；截断/损坏拒绝。
2. **严格四行制 FASTQ**：`@读段名` / 序列 / `+` / 质量串，序列与质量串等长，文件以换行结尾。
3. **两端配对**：R1、R2 条数相同；逐对读段名取首个空白前的 token、去掉末尾 `/1`、`/2` 后必须一致。
4. 只有两端全部校验并保存成功，批次才置为 `ready` 并允许下载展开后的 FASTQ。

## 4. 字节上限与并发语义

- 接收过程中，**压缩临时文件与展开结果的每一次写入都先向配额器预留字节**，超过
  `FASTQ_BATCH_BYTE_LIMIT` 立即失败（HTTP 413），不会等整包落盘后才检查；解压炸弹同理在途中被拦。
- 配额按批记账。某批超限只清掉该批，其他批继续接收；慢上传占用的是协程而非线程，不挡其他批。
- 取消接口会先置取消信号、等待本批在途上传协程退出（此后不会再落盘），再删除全部文件、
  按磁盘真实占用回收空间、把状态落库为 `cancelled`，最后才返回确认。

## 5. 拆样（demux）

对 `ready` 批调用 `POST /batches/{id}/demux`：

```json
{
  "sample_id_a": "S-DEMUX-A",
  "sample_id_b": "S-DEMUX-B",
  "barcode_a": "ACGTACGT",
  "barcode_b": "ACGTACGA"
}
```

请求校验：两个样本号必须不同；两条码均为非空**等长 ACGT**（大小写会规范为大写）。
不满足返回 422。两条码完全相同也合法，但此时每条读对都等距、全部计入未归样。

### 归样规则

1. **读条码**：取 FASTQ 头部**首个空格之后**的最后一个冒号字段，例如
   `@M00123:...:1101:1000:2000 1:N:0:ACGTACGT` → `ACGTACGT`。头部无空格、字段为空或非 ASCII
   时视为无条码。
2. **两端一致**：R1 与 R2 的头部条码必须完全相同；不一致（或任一端缺失）的读对计入未归样。
3. **最小汉明距离**：两端一致时，与各样本条码比汉明距离（要求等长），**最多允许一位差**。
   取距离最小的样本；距离相同（并列）或全部超过 1 位，均计入未归样。
4. 未归样读对完整保留在 `unassigned` 桶（R1/R2 各一份），不会被丢弃。

响应同时给出每对样本/未归样的读对数与下载地址：

```json
{
  "batch_id": "...", "status": "published",
  "total_pairs": 91, "unassigned_pairs": 12,
  "samples": [
    {"sample_id": "S-DEMUX-A", "barcode": "ACGTACGT", "assigned_pairs": 45,
     "r1_download_url": "/batches/.../demux/samples/S-DEMUX-A/files/R1/download",
     "r2_download_url": "/batches/.../demux/samples/S-DEMUX-A/files/R2/download"}
  ],
  "unassigned": {"pairs": 12, "r1_download_url": "...", "r2_download_url": "..."}
}
```

### 一次性与原子发布

- **全部样本文件成对写完后才共同发布**：先写入 `demux/staging-<token>/`，所有样本与
  unassigned 的 R1、R2 全部关闭落盘后，再把整个目录**一次 rename** 为 `demux/published/`；
  下载只认 `published`，所以不存在「部分样本可下载」的中间态。
- **并发最多成功一次**：同一批的「查状态 → 置 running → 写 → 发布」由批级锁串行化，
  并发请求中只有一个进入执行，其余立即 409；已发布后再请求同样 409。
- **失败可重试**：超限（413）、源 FASTQ 异常（422）、写盘失败（500）都会删除 staging 与
  残留、把该次拆样记为 `failed`，不发布任何下载；下次请求会先清掉失败记录再重新执行。
- **超限守限**：拆样每次落盘前都向同一按批配额器预留字节；拆样结果与原档共用
  `FASTQ_BATCH_BYTE_LIMIT`，途中一旦越界立即中止并回滚，不会等写完才检查。
- **原批不受影响**：拆样过程只读原档，失败或重启中断都不会改动 `batches` 状态；原档
  `R1/R2/download` 保持可用。

## 6. 重启恢复

启动时扫描数据卷：

- `ready` 批：保留展开文件，按磁盘真实字节重建配额账，可继续下载；
- `receiving` 批（未完成）：判为 `failed`，删除残留文件并回收空间后再对外服务；
- `failed` / `cancelled` 批：清理任何残留；磁盘孤儿目录直接回收。
- 拆样：`published` 结果保留并可继续下载；`running`（重启中断）的拆样判为 `failed`，
  删除 staging 与任何未发布产物，原批仍为 `ready`、原档可下载。

## 7. 样例与演示

生成正常、错配、超限、损坏四类样例，以及带**等距条码**的拆样混样：

```bash
python scripts/make_samples.py samples
# samples/demux/{R1,R2}.fastq.gz   91 对混样，两个样本 + 等距/超范围/两端不一致/无条码未归样
# samples/demux/expected.json      建议请求体与应有计数
```

对已启动的服务跑端到端演示（正常放行 / 错配拒收 / 超限 413 / 拆样计数校验）：

```bash
BASE_URL=http://127.0.0.1:8000 SAMPLES_DIR=samples bash scripts/demo.sh
```

典型手工调用：

```bash
BID=$(curl -s -X POST localhost:8000/batches -H 'Content-Type: application/json' \
  -d '{"sample_id":"S-2026-001"}' | python -c 'import json,sys;print(json.load(sys.stdin)["id"])')
curl -X POST localhost:8000/batches/$BID/files/R1 -H 'Content-Type: application/gzip' \
  --data-binary @samples/normal/R1.fastq.gz
curl -X POST localhost:8000/batches/$BID/files/R2 -H 'Content-Type: application/gzip' \
  --data-binary @samples/normal/R2.fastq.gz
curl localhost:8000/batches/$BID
curl -OJ localhost:8000/batches/$BID/files/R1/download

# 拆样（需 ready 批）
curl -X POST localhost:8000/batches/$BID/demux -H 'Content-Type: application/json' -d '{
  "sample_id_a": "S-DEMUX-A", "sample_id_b": "S-DEMUX-B",
  "barcode_a": "ACGTACGT", "barcode_b": "ACGTACGA"}'
curl localhost:8000/batches/$BID/demux
curl -OJ localhost:8000/batches/$BID/demux/samples/S-DEMUX-B/files/R1/download
curl -OJ localhost:8000/batches/$BID/demux/unassigned/files/R1/download
```

## 8. 测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

- `tests/test_api.py` / `tests/test_lifecycle.py`：ASGI 级单元测试（正常、错配、条数不一致、
  序列质量不等长、坏 gzip、多成员 gzip、413 超限、取消中断、配额双计、重启恢复等）。
- `tests/test_demux.py`：拆样规则（等距/超范围/两端不一致）、一次性 API 与发布原子性、
  超限与写盘失败不留半成品、真实 uvicorn 下并发只成功一次、重启中断恢复等。
- `tests/test_server_integration.py`：拉起真实 uvicorn 进程，验证慢上传不挡其他批、
  取消「等待停止→删文件→释放空间」的端到端语义，以及正常/错配/超限全链路。

## 9. 目录结构

```
app/
  config.py       环境变量配置
  database.py     SQLite 连接与建表（WAL）
  models.py       状态枚举与运行期批次/拆样模型
  demux.py        条码解析、汉明距离归样、带配额的拆样写入
  storage.py      按批配额器 SpaceManager、磁盘回收、批次/拆样注册表
  validation.py   流式 gunzip、四行制 FASTQ 结构与配对校验
  schemas.py      请求/响应模型
  service.py      批次生命周期：创建、上传、取消、下载、拆样、重启恢复
  main.py         FastAPI 入口
examples/demux/   已提交的混样样例（含等距条码）与预期计数
scripts/          样例生成与演示脚本
tests/            单元测试与真实服务器集成测试
docker-compose.yml / Dockerfile
```
