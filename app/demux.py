"""拆样（demultiplex）核心逻辑。

从 Illumina 读段头部「空格之后的最后一个冒号字段」读取条码，按汉明距离把
读对归入提交的样本桶：

* R1、R2 头部条码必须完全一致，否则该读对计为未归样；
* 两端一致时按与各样本条码的最小汉明距离归样，最多容忍 1 位差异；
* 最小距离相同（并列）或全部超过 1 位差异，均计为未归样。

所有输出先写到 staging 目录，由调用方在全部样本的 R1/R2 成对写完后一次性
发布（目录原子 rename），任何异常都不会留下可下载的半成品。
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Sequence

from .storage import SpaceManager

# 每个桶攒够多少字节再向配额器预留并落盘
FLUSH_BYTES = 64 * 1024

# 允许的最大汉明距离
MAX_MISMATCHES = 1

UNASSIGNED_KEY = "unassigned"


class DemuxError(Exception):
    """拆样过程中发现源 FASTQ 结构异常。"""


class DemuxCancelled(DemuxError):
    """拆样被取消。"""


@dataclass(frozen=True)
class BarcodeSpec:
    """一个待拆分样本：输出目录序号、样本号与条码。"""

    index: int
    sample_id: str
    barcode: str


@dataclass
class DemuxOutcome:
    sample_counts: dict[int, int] = field(default_factory=dict)
    unassigned_pairs: int = 0
    total_pairs: int = 0


# --------------------------------------------------------------------------- #
# 条码解析与归样
# --------------------------------------------------------------------------- #


def parse_header_barcode(header: bytes) -> str | None:
    """取 FASTQ 头部首个空格之后的最后一个冒号字段作为条码。

    Illumina 风格：``@INSTR:...:LANE:TILE:X:Y 1:N:0:ACGTACGT`` → ``ACGTACGT``。
    双端 index 形如 ``1:N:0:ACGTACGT+TTTTTTTT`` 时取前一个（i7）。
    头部没有空格、条码字段为空或非 ASCII 时返回 None（视为无法归样）。
    """
    parts = header.split(None, 1)
    if len(parts) != 2:
        return None
    # Illumina 头部第二个 token 形如 ``1:N:0:ACGTACGT``；条码是最后一个冒号字段
    field = parts[1].split()[0]
    token = field.rsplit(b":", 1)[-1].split(b"+", 1)[0]
    if not token:
        return None
    try:
        return token.decode("ascii").upper()
    except UnicodeDecodeError:
        return None


def hamming_distance(a: str, b: str) -> int | None:
    """等长字符串的汉明距离；长度不同返回 None（无法比较）。"""
    if len(a) != len(b):
        return None
    return sum(1 for x, y in zip(a, b) if x != y)


def assign_sample(observed: str, specs: Sequence[BarcodeSpec]) -> int | None:
    """按最小汉明距离归样；并列或全部超范围时返回 None（未归样）。"""
    distances: list[tuple[int, int]] = []
    for spec in specs:
        distance = hamming_distance(observed, spec.barcode)
        if distance is not None:
            distances.append((distance, spec.index))
    if not distances:
        return None
    best = min(distance for distance, _ in distances)
    if best > MAX_MISMATCHES:
        return None
    winners = {index for distance, index in distances if distance == best}
    if len(winners) != 1:
        return None
    return winners.pop()


# --------------------------------------------------------------------------- #
# 带配额的输出桶
# --------------------------------------------------------------------------- #


class _FastqSink:
    """缓冲写入一个 FASTQ 文件，每次落盘前向 SpaceManager 预留字节。"""

    def __init__(self, path: Path, space: SpaceManager, batch_id: str) -> None:
        self.path = path
        self._space = space
        self._batch_id = batch_id
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(path, "wb")
        self._buffer = bytearray()

    def write(self, data: bytes) -> None:
        self._buffer.extend(data)
        if len(self._buffer) >= FLUSH_BYTES:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        chunk = bytes(self._buffer)
        # 先预留再落盘：超限时本次写入不会发生
        self._space.reserve_sync(self._batch_id, len(chunk))
        try:
            self._fh.write(chunk)
        except BaseException:
            self._space.release_sync(self._batch_id, len(chunk))
            raise
        self._buffer.clear()

    def close(self) -> None:
        self.flush()
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()

    def abort(self) -> None:
        try:
            self._fh.close()
        except OSError:
            pass


def _read_record(fh: BinaryIO, side: str) -> tuple[bytes, bytes, bytes, bytes] | None:
    """读一条四行制记录（保留换行），文件结束返回 None。"""
    header = fh.readline()
    if not header:
        return None
    seq = fh.readline()
    plus = fh.readline()
    qual = fh.readline()
    if not seq.endswith(b"\n") or not plus.endswith(b"\n") or not qual.endswith(b"\n"):
        raise DemuxError(f"{side} record is truncated or does not end with newline")
    if not header.endswith(b"\n"):
        raise DemuxError(f"{side} header line does not end with newline")
    if not header.startswith(b"@"):
        raise DemuxError(f"{side} record header must start with '@'")
    if plus.rstrip(b"\r\n") != b"+":
        raise DemuxError(f"{side} record third line must be '+'")
    if len(seq.rstrip(b"\r\n")) != len(qual.rstrip(b"\r\n")):
        raise DemuxError(f"{side} record sequence and quality lengths differ")
    return header, seq, plus, qual


def demultiplex_pair(
    *,
    r1_path: Path,
    r2_path: Path,
    specs: Sequence[BarcodeSpec],
    out_dir: Path,
    space: SpaceManager,
    batch_id: str,
    cancel_event: threading.Event | None = None,
) -> DemuxOutcome:
    """把成对 FASTQ 拆到 ``out_dir/<index>/{R1,R2}.fastq`` 与 ``out_dir/unassigned``。

    调用方负责把 out_dir 放在 staging 位置，成功后整体发布。
    超限抛 :class:`QuotaExceededError`，源文件异常抛 :class:`DemuxError`。
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    sinks: dict[str, tuple[_FastqSink, _FastqSink]] = {}

    def _new_sink(key: str) -> tuple[_FastqSink, _FastqSink]:
        first = _FastqSink(out_dir / key / "R1.fastq", space, batch_id)
        try:
            second = _FastqSink(out_dir / key / "R2.fastq", space, batch_id)
        except BaseException:
            first.abort()
            raise
        pair = (first, second)
        sinks[key] = pair
        return pair

    counts = {spec.index: 0 for spec in specs}
    unassigned = 0
    total = 0

    try:
        for spec in specs:
            _new_sink(str(spec.index))
        unassigned_sinks = _new_sink(UNASSIGNED_KEY)

        with open(r1_path, "rb") as f1, open(r2_path, "rb") as f2:
            while True:
                if cancel_event is not None and cancel_event.is_set():
                    raise DemuxCancelled("demux cancelled")
                rec1 = _read_record(f1, r1_path.name)
                rec2 = _read_record(f2, r2_path.name)
                if rec1 is None and rec2 is None:
                    break
                if rec1 is None or rec2 is None:
                    raise DemuxError("R1 and R2 have different record counts")

                barcode1 = parse_header_barcode(rec1[0])
                barcode2 = parse_header_barcode(rec2[0])
                target: int | None = None
                if barcode1 is not None and barcode2 is not None and barcode1 == barcode2:
                    target = assign_sample(barcode1, specs)

                if target is None:
                    unassigned += 1
                    sinks_pair = unassigned_sinks
                else:
                    counts[target] += 1
                    sinks_pair = sinks[str(target)]

                sinks_pair[0].write(rec1[0] + rec1[1] + rec1[2] + rec1[3])
                sinks_pair[1].write(rec2[0] + rec2[1] + rec2[2] + rec2[3])
                total += 1

        if total == 0:
            raise DemuxError("source FASTQ pair contains no records")

        for pair in sinks.values():
            pair[0].close()
            pair[1].close()
    except BaseException:
        for pair in sinks.values():
            pair[0].abort()
            pair[1].abort()
        raise

    return DemuxOutcome(
        sample_counts=counts,
        unassigned_pairs=unassigned,
        total_pairs=total,
    )
