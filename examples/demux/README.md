# 拆样演示样例（已提交，无需生成）

`scripts/make_samples.py` 生成的同一份混样，随仓库提供以便直接跑演示：

| 文件 | 说明 |
| --- | --- |
| `R1.fastq.gz` / `R2.fastq.gz` | 91 对同 lane 混样，Illumina 头部空格后带条码 |
| `expected.json` | 建议请求体、条码说明与应有计数 |

## 条码与预期

| 桶 | 条码 | 读对数 |
| --- | --- | --- |
| 样本 `S-DEMUX-A` | `ACGTACGT`（含 5 对一位差错的 `ACGTACTT`） | 45 |
| 样本 `S-DEMUX-B` | `ACGTACGA`（含 4 对一位差错的 `TCGTACGA`） | 34 |
| 未归样：等距 | `ACGTACGG`（与 A、B 各差一位） | 6 |
| 未归样：超范围 | `TTTTTTTT` | 3 |
| 未归样：两端不一致 | R1 `ACGTACGT`、R2 `ACGTACGA` | 2 |
| 未归样：无条码 | 头部无空格字段 | 1 |
| **合计** | | **91**，其中未归样 **12** |

两条码本身只差第 8 位（`G`/`A`），所以一位容错下仍能区分；而 `ACGTACGG` 恰好卡在
两者正中间，必须判为未归样而不是猜一个。

## 用法

```bash
# 服务启动后（默认 8000 端口）
BID=$(curl -s -X POST localhost:8000/batches -H 'Content-Type: application/json' \
  -d '{"sample_id":"demo-demux"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')
curl -X POST localhost:8000/batches/$BID/files/R1 -H 'Content-Type: application/gzip' \
  --data-binary @examples/demux/R1.fastq.gz
curl -X POST localhost:8000/batches/$BID/files/R2 -H 'Content-Type: application/gzip' \
  --data-binary @examples/demux/R2.fastq.gz
curl -X POST localhost:8000/batches/$BID/demux -H 'Content-Type: application/json' \
  -d "$(python3 -c 'import json;print(json.dumps(json.load(open("examples/demux/expected.json"))["request"]))')"
curl localhost:8000/batches/$BID/demux
curl -OJ localhost:8000/batches/$BID/demux/samples/S-DEMUX-A/files/R1/download
curl -OJ localhost:8000/batches/$BID/demux/unassigned/files/R1/download
```

或直接跑演示脚本（会额外覆盖正常 / 错配 / 超限三类接收）：

```bash
BASE_URL=http://127.0.0.1:8000 SAMPLES_DIR=examples bash scripts/demo.sh
```

> `scripts/demo.sh` 读取 `$SAMPLES_DIR/<case>/R1.fastq.gz`；`examples/` 下只有 `demux/`，
> 其余三类样例请先 `python scripts/make_samples.py samples` 再指向 `samples`。
