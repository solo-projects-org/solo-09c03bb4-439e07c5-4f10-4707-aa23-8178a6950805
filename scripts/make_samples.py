#!/usr/bin/env python3
"""生成演示样例：正常、错配、超限、损坏，以及拆样混样。

用法:
    python scripts/make_samples.py [输出目录]

输出目录下:
    normal/R1.fastq.gz  normal/R2.fastq.gz       # 合法双端
    mismatch/R1.fastq.gz mismatch/R2.fastq.gz    # R2 第 2 条读段名与 R1 不一致
    oversized/R1.fastq.gz oversized/R2.fastq.gz  # 展开后远超 64 MiB 上限
    corrupt/R1.fastq.gz                          # 损坏的 gzip（截断）
    demux/R1.fastq.gz   demux/R2.fastq.gz        # 同 lane 混样，Illumina 头部带条码
    demux/expected.json                          # 各样本 / 未归样应有读对数、建议请求体
"""

from __future__ import annotations

import gzip
import json
import os
import sys
from pathlib import Path

# 拆样演示用的两条条码：只差一位（第 8 位 G/A），严格一位容错下仍互不干扰。
DEMUX_BARCODE_A = "ACGTACGT"
DEMUX_BARCODE_B = "ACGTACGA"
# 等距观察值：与 A、B 各差一位 → 必须判为未归样（并列不可猜）
DEMUX_BARCODE_AMBIGUOUS = "ACGTACGG"
# 超范围：与两条码都差很多位 → 未归样
DEMUX_BARCODE_FAR = "TTTTTTTT"


def _record(name: bytes, seq: bytes, qual: bytes) -> bytes:
    return b"@" + name + b"\n" + seq + b"\n+\n" + qual + b"\n"


def _illumina_record(name: bytes, barcode: str | None, seq: bytes, qual: bytes) -> bytes:
    header = name + b" 1:N:0:" + (barcode.encode() if barcode else b"")
    return _record(header, seq, qual)


def _pair(index: int, length: int = 80) -> tuple[bytes, bytes]:
    # 确定性生成，避免引入随机依赖
    seq1 = bytes(65 + (i * 7 + index) % 26 for i in range(length))
    seq2 = bytes(65 + (i * 11 + index) % 26 for i in range(length))
    qual = bytes(33 + (i * 3 + index) % 60 for i in range(length))
    base = f"sample-seq-{index:06d}".encode()
    return _record(base + b"/1", seq1, qual), _record(base + b"/2", seq2, qual)


def _write_pair(directory: Path, count: int, *, mismatch_at: int | None = None) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    with gzip.open(directory / "R1.fastq.gz", "wb") as f1, gzip.open(
        directory / "R2.fastq.gz", "wb"
    ) as f2:
        for i in range(count):
            r1, r2 = _pair(i)
            if mismatch_at is not None and i == mismatch_at:
                # 读段名被调包，去掉 /2 后仍与 R1 对不上
                r2 = r2.replace(b"sample-seq-", b"sample-WRONG-", 1)
            f1.write(r1)
            f2.write(r2)


# --------------------------------------------------------------------------- #
# 拆样演示样例
# --------------------------------------------------------------------------- #


def _write_demux(directory: Path) -> dict:
    """写一批同 lane 混样：两个样本 + 三类未归样读对。

    每个读对的 R1/R2 头部条码都按 Illumina 惯例写在空格之后：
        @M00123:...:1101:1000:2000 1:N:0:<barcode>
        @M00123:...:1101:1000:2000 2:N:0:<barcode>
    """
    directory.mkdir(parents=True, exist_ok=True)

    # (R1 条码, R2 条码) 序列；两端不一致时按「未归样」处理
    plan: list[tuple[str | None, str | None]] = []
    counts = {"S-DEMUX-A": 0, "S-DEMUX-B": 0, "unassigned": 0}

    def add(r1_bc: str | None, r2_bc: str | None, bucket: str | None) -> None:
        plan.append((r1_bc, r2_bc))
        counts[bucket if bucket else "unassigned"] += 1

    for i in range(40):
        add(DEMUX_BARCODE_A, DEMUX_BARCODE_A, "S-DEMUX-A")
    for i in range(30):
        add(DEMUX_BARCODE_B, DEMUX_BARCODE_B, "S-DEMUX-B")
    # 一位差错的 A（第 8 位 G→T）：仍在 A 的容错半径内
    for i in range(5):
        add("ACGTACTT", "ACGTACTT", "S-DEMUX-A")
    # 一位差错的 B（第 1 位 A→T）：只落在 B 的容错半径内
    for i in range(4):
        add("TCGTACGA", "TCGTACGA", "S-DEMUX-B")
    # 等距（与 A、B 各差一位）→ 未归样
    for i in range(6):
        add(DEMUX_BARCODE_AMBIGUOUS, DEMUX_BARCODE_AMBIGUOUS, None)
    # 超出容错范围 → 未归样
    for i in range(3):
        add(DEMUX_BARCODE_FAR, DEMUX_BARCODE_FAR, None)
    # 两端条码不一致 → 未归样（即便各自都能归到某样本）
    for i in range(2):
        add(DEMUX_BARCODE_A, DEMUX_BARCODE_B, None)
    # 头部没有条码字段 → 未归样
    for i in range(1):
        add(None, None, None)

    with gzip.open(directory / "R1.fastq.gz", "wb") as f1, gzip.open(
        directory / "R2.fastq.gz", "wb"
    ) as f2:
        for index, (bc1, bc2) in enumerate(plan):
            seq1 = bytes(65 + (index * 3 + j) % 26 for j in range(80))
            seq2 = bytes(65 + (index * 5 + j) % 26 for j in range(80))
            qual = b"J" * 80
            name = f"M00123:1:DEMO:1:1101:{1000 + index:07d}:2000".encode()
            f1.write(_illumina_record(name + b"/1", bc1, seq1, qual))
            f2.write(_illumina_record(name + b"/2", bc2, seq2, qual))

    expected = {
        "request": {
            "sample_id_a": "S-DEMUX-A",
            "sample_id_b": "S-DEMUX-B",
            "barcode_a": DEMUX_BARCODE_A,
            "barcode_b": DEMUX_BARCODE_B,
        },
        "barcodes": {
            "sample_a": DEMUX_BARCODE_A,
            "sample_b": DEMUX_BARCODE_B,
            "ambiguous": DEMUX_BARCODE_AMBIGUOUS,
            "out_of_range": DEMUX_BARCODE_FAR,
        },
        "total_pairs": len(plan),
        "expected_assigned": {"S-DEMUX-A": counts["S-DEMUX-A"], "S-DEMUX-B": counts["S-DEMUX-B"]},
        "expected_unassigned": counts["unassigned"],
        "notes": [
            "条码位于读段名首个空格之后，取最后一个冒号字段。",
            "两端条码必须完全一致；最小汉明距离最多容忍 1 位。",
            "等距（ambiguous）与超范围（out_of_range）都计入未归样。",
        ],
    }
    (directory / "expected.json").write_text(
        json.dumps(expected, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return expected


def main(out: Path) -> None:
    # 1) 正常样例：1000 对，约 330 KB 展开数据
    _write_pair(out / "normal", 1000)

    # 2) 错配样例：第 2 条（index=1）读段名不一致
    _write_pair(out / "mismatch", 10, mismatch_at=1)

    # 3) 超限样例：每条记录 4*82≈328 字节，40 万对 ≈ 128 MiB，超过 64 MiB
    _write_pair(out / "oversized", 400_000)

    # 4) 损坏 gzip：合法流被截断
    corrupt_dir = out / "corrupt"
    corrupt_dir.mkdir(parents=True, exist_ok=True)
    tmp_gz = out / "_tmp_corrupt.gz"
    with gzip.open(tmp_gz, "wb") as f:
        for i in range(5):
            r1, _ = _pair(i)
            f.write(r1)
    blob = tmp_gz.read_bytes()
    tmp_gz.unlink()
    (corrupt_dir / "R1.fastq.gz").write_bytes(blob[: len(blob) // 2])

    # 5) 拆样混样：91 对，含等距条码
    expected = _write_demux(out / "demux")
    print(
        f"demux: total={expected['total_pairs']} "
        f"assigned={expected['expected_assigned']} "
        f"unassigned={expected['expected_unassigned']}"
    )

    for path in sorted(out.rglob("*.gz")):
        print(f"{path.relative_to(out)}  {os.path.getsize(path)} bytes")


if __name__ == "__main__":
    target = Path(sys.argv[1] if len(sys.argv) > 1 else "samples")
    main(target)
