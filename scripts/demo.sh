#!/usr/bin/env bash
# 端到端演示：正常 / 错配 / 超限三类接收 + 混样拆样。
# 依赖: curl、python3；样例先由 scripts/make_samples.py 生成。
set -euo pipefail

BASE="${BASE_URL:-http://127.0.0.1:8000}"
SAMPLES="${SAMPLES_DIR:-samples}"

post_batch() {  # $1 = sample_id
  curl -sS -X POST "$BASE/batches" \
    -H 'Content-Type: application/json' \
    -d "{\"sample_id\":\"$1\"}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])'
}

upload() {  # $1 = batch_id $2 = R1|R2 $3 = 文件
  echo "  upload $2 ($(wc -c < "$3") compressed bytes)"
  curl -sS -o /tmp/fastq_resp.json -w "  HTTP %{http_code}\n" \
    -X POST "$BASE/batches/$1/files/$2" \
    -H 'Content-Type: application/gzip' \
    --data-binary "@$3"
  python3 -c 'import json; d=json.load(open("/tmp/fastq_resp.json")); print("  ->", d.get("status"), d.get("error") or d.get("detail") or "")'
}

run_case() {  # $1 = 名称 $2 = 样例目录 $3 = 期望状态
  echo "================ $1 ================"
  local bid
  bid="$(post_batch "demo-$1")"
  echo "batch: $bid"
  upload "$bid" R1 "$SAMPLES/$2/R1.fastq.gz"
  if [[ -f "$SAMPLES/$2/R2.fastq.gz" ]]; then
    upload "$bid" R2 "$SAMPLES/$2/R2.fastq.gz"
  fi
  echo "final state:"
  curl -sS "$BASE/batches/$bid" | python3 -m json.tool
}

echo "服务: $BASE"
curl -sS "$BASE/health" | python3 -m json.tool

run_case "normal" "normal"
run_case "mismatch" "mismatch"
run_case "oversized" "oversized"

echo
echo "下载正常批展开后的 R1（前 2 行）:"
NORMAL_BID="$(curl -sS "$BASE/batches" | python3 -c \
  'import json,sys; print(next(b["id"] for b in json.load(sys.stdin)["batches"] if b["status"]=="ready"))')"
curl -sS "$BASE/batches/$NORMAL_BID/files/R1/download" | head -n 2 || true

# --------------------------------------------------------------------------- #
# 拆样演示
# --------------------------------------------------------------------------- #
if [[ -f "$SAMPLES/demux/R1.fastq.gz" ]]; then
  echo
  echo "================ demux ================"
  DEMUX_BID="$(post_batch "demo-demux")"
  echo "batch: $DEMUX_BID"
  upload "$DEMUX_BID" R1 "$SAMPLES/demux/R1.fastq.gz"
  upload "$DEMUX_BID" R2 "$SAMPLES/demux/R2.fastq.gz"

  PAYLOAD="$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["request"]))' \
    "$SAMPLES/demux/expected.json")"
  echo "demux request: $PAYLOAD"
  curl -sS -o /tmp/fastq_demux.json -w "  HTTP %{http_code}\n" \
    -X POST "$BASE/batches/$DEMUX_BID/demux" \
    -H 'Content-Type: application/json' \
    --data-binary "$PAYLOAD"
  python3 - "$SAMPLES/demux/expected.json" <<'PY'
import json, sys
got = json.load(open("/tmp/fastq_demux.json"))
expected = json.load(open(sys.argv[1]))
print("  status:", got.get("status"), got.get("error") or got.get("detail") or "")
print("  total/unassigned:", got.get("total_pairs"), got.get("unassigned_pairs"))
for s in got.get("samples", []):
    print(f"  sample {s['sample_id']} ({s['barcode']}): {s['assigned_pairs']} pairs")
print("  expected:", expected["expected_assigned"], "unassigned", expected["expected_unassigned"])
assert got["status"] == "published", got
assert got["unassigned_pairs"] == expected["expected_unassigned"], got
for s in got["samples"]:
    assert s["assigned_pairs"] == expected["expected_assigned"][s["sample_id"]], (s, expected)
print("  计数与 expected.json 一致 ✓")
PY

  echo
  echo "下载各样本 R1/R2 与未归样 R1/R2:"
  for SAMPLE in S-DEMUX-A S-DEMUX-B; do
    for ROLE in R1 R2; do
      CODE="$(curl -sS -o "/tmp/demux_${SAMPLE}_${ROLE}.fastq" -w '%{http_code}' \
        "$BASE/batches/$DEMUX_BID/demux/samples/$SAMPLE/files/$ROLE/download")"
      echo "  $SAMPLE $ROLE -> HTTP $CODE, $(wc -l < "/tmp/demux_${SAMPLE}_${ROLE}.fastq") lines"
    done
  done
  for ROLE in R1 R2; do
    CODE="$(curl -sS -o "/tmp/demux_unassigned_${ROLE}.fastq" -w '%{http_code}' \
      "$BASE/batches/$DEMUX_BID/demux/unassigned/files/$ROLE/download")"
    echo "  unassigned $ROLE -> HTTP $CODE, $(wc -l < "/tmp/demux_unassigned_${ROLE}.fastq") lines"
  done

  echo
  echo "重复拆样（应 409）:"
  curl -sS -o /tmp/fastq_demux_again.json -w "  HTTP %{http_code}\n" \
    -X POST "$BASE/batches/$DEMUX_BID/demux" \
    -H 'Content-Type: application/json' \
    --data-binary "$PAYLOAD"
  python3 -c 'import json; print("  ->", json.load(open("/tmp/fastq_demux_again.json")).get("detail"))'
fi
