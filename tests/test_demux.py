"""拆样（demultiplex）功能测试。

覆盖：条码解析、归样规则（等距/超范围/两端不一致）、一次性 API、发布原子性、
并发只成功一次、失败重试、超限与写盘失败不留半成品、重启中断恢复。
"""

from __future__ import annotations

import asyncio
import gzip
import io
import threading
from pathlib import Path

import pytest

from app.config import Settings
from app.demux import (
    BarcodeSpec,
    DemuxError,
    assign_sample,
    demultiplex_pair,
    hamming_distance,
    parse_header_barcode,
)
from app.database import get_conn
from app.models import FileRole
from app.service import BatchService
from app.storage import QuotaExceededError, SpaceManager
from tests.helpers import create_batch as _create_batch
from tests.helpers import upload_bytes as _upload_bytes


# --------------------------------------------------------------------------- #
# 构造带 Illumina 条码头部的 FASTQ
# --------------------------------------------------------------------------- #

QUAL = b"J" * 40


def _illumina_record(name: bytes, barcode: str | None, seq: bytes, qual: bytes) -> bytes:
    header = name
    if barcode is not None:
        header = name + b" 1:N:0:" + barcode.encode()
    return b"@" + header + b"\n" + seq + b"\n+\n" + qual + b"\n"


def _pair_records(
    plan: list[tuple[str | None, str | None]],
    *,
    tag: str = "read",
):
    """plan 每项为 (R1 条码, R2 条码)；None 表示头部无条码字段。"""
    r1, r2 = [], []
    for i, (bc1, bc2) in enumerate(plan):
        seq = bytes(65 + (i + j) % 26 for j in range(40))
        r1.append(_illumina_record(f"{tag}-{i}/1".encode(), bc1, seq, QUAL))
        r2.append(_illumina_record(f"{tag}-{i}/2".encode(), bc2, seq, QUAL))
    return b"".join(r1), b"".join(r2)


def _gz(blob: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as fh:
        fh.write(blob)
    return buf.getvalue()


def _ready_batch(client, plan, sample_id="mixed-lane"):
    r1, r2 = _pair_records(plan)
    bid = _create_batch(client, sample_id)
    assert _upload_bytes(client, bid, "R1", _gz(r1)).status_code == 200
    assert _upload_bytes(client, bid, "R2", _gz(r2)).status_code == 200
    return bid


def _demux(client, bid, a="S-A", b="S-B", ba="ACGTACGT", bb="ACGTACGA"):
    return client.post(
        f"/batches/{bid}/demux",
        json={"sample_id_a": a, "sample_id_b": b, "barcode_a": ba, "barcode_b": bb},
    )


def _lines(blob: bytes) -> list[bytes]:
    return blob.splitlines()


# --------------------------------------------------------------------------- #
# 纯函数：条码解析 / 归样
# --------------------------------------------------------------------------- #


def test_parse_illumina_header_barcode():
    assert parse_header_barcode(b"@M00123:1:ABC:1:1101:1000:2000 1:N:0:ACGTACGT") == "ACGTACGT"
    assert parse_header_barcode(b"@read 2:N:0:TTGGCCAA") == "TTGGCCAA"
    # 双端 index（i7+i5）：取 i7 作为本轮条码
    assert parse_header_barcode(b"@read 1:N:0:ACGTACGT+TTTTTTTT") == "ACGTACGT"
    # 无空格 / 无条码字段 / 空条码
    assert parse_header_barcode(b"@read/1") is None
    assert parse_header_barcode(b"@read 1:N:0:") is None
    assert parse_header_barcode(b"@read") is None


def test_hamming_and_assignment_rules():
    a = BarcodeSpec(0, "S-A", "ACGTACGT")
    b = BarcodeSpec(1, "S-B", "TTTTTTTT")
    assert hamming_distance("ACGTACGT", "ACGTACGA") == 1
    assert hamming_distance("ACGT", "ACG") is None

    assert assign_sample("ACGTACGT", [a, b]) == 0  # 精确
    assert assign_sample("ACGTACGA", [a, b]) == 0  # 差一位
    assert assign_sample("TTTTTTTA", [a, b]) == 1  # 差一位
    assert assign_sample("GGGGGGGG", [a, b]) is None  # 全部超范围
    assert assign_sample("ACGTACGA", [a, BarcodeSpec(1, "S-B", "ACGTACGG")]) is None  # 等距


def test_equal_distance_returns_unassigned():
    """等距条码：观察值同时距两条码一位 → 未归样。"""
    a = BarcodeSpec(0, "S-A", "ACGTACGT")
    b = BarcodeSpec(1, "S-B", "ACGTACGA")  # 与 A 差 1
    # 与 A 差 1、与 B 差 1 的序列
    observed = "ACGTACGG"
    assert hamming_distance(observed, a.barcode) == 1
    assert hamming_distance(observed, b.barcode) == 1
    assert assign_sample(observed, [a, b]) is None


# --------------------------------------------------------------------------- #
# 核心拆样：计数与分桶
# --------------------------------------------------------------------------- #


def test_demultiplex_pair_buckets(tmp_path):
    # 第二条码选「与 A 只差一位」的 ACGTACGA，才能构造真正的等距观察值
    plan = [
        ("ACGTACGT", "ACGTACGT"),  # -> A 精确
        ("ACGTACGA", "ACGTACGA"),  # -> B 精确（距 A 一位、距 B 零位）
        ("TTTTTTTT", "TTTTTTTT"),  # -> 超范围 -> 未归样
        ("ACGTACGG", "ACGTACGG"),  # 距 A 一位、距 B 一位 -> 等距未归样
        ("ACGTACGT", "ACGTACGA"),  # 两端不一致 -> 未归样
        ("GGGGGGGG", "GGGGGGGG"),  # 超范围 -> 未归样
        (None, None),  # 无条码 -> 未归样
        ("ACGTACGT", None),  # 只有一端有条码 -> 未归样
    ]
    r1, r2 = _pair_records(plan)
    p1 = tmp_path / "R1.fastq"
    p2 = tmp_path / "R2.fastq"
    p1.write_bytes(r1)
    p2.write_bytes(r2)
    space = SpaceManager(10 * 1024 * 1024)
    specs = [BarcodeSpec(0, "S-A", "ACGTACGT"), BarcodeSpec(1, "S-B", "ACGTACGA")]

    outcome = demultiplex_pair(
        r1_path=p1,
        r2_path=p2,
        specs=specs,
        out_dir=tmp_path / "out",
        space=space,
        batch_id="b1",
    )
    assert outcome.total_pairs == 8
    assert outcome.sample_counts == {0: 1, 1: 1}
    assert outcome.unassigned_pairs == 6

    # 每个样本的 R1/R2 成对存在，且读对数量一致
    for slot in ("0", "1"):
        assert (tmp_path / "out" / slot / "R1.fastq").exists()
        assert (tmp_path / "out" / slot / "R2.fastq").exists()
    assert (tmp_path / "out" / "unassigned" / "R1.fastq").exists()
    assert (tmp_path / "out" / "unassigned" / "R2.fastq").exists()

    a_r1 = _lines((tmp_path / "out" / "0" / "R1.fastq").read_bytes())
    a_r2 = _lines((tmp_path / "out" / "0" / "R2.fastq").read_bytes())
    assert len(a_r1) == 4 and len(a_r2) == 4  # 1 条读对 × 4 行
    assert a_r1[0] == b"@read-0/1 1:N:0:ACGTACGT"
    b_r1 = _lines((tmp_path / "out" / "1" / "R1.fastq").read_bytes())
    assert b_r1[0] == b"@read-1/1 1:N:0:ACGTACGA"
    # 未归样保留全部未归读对
    un_r1 = _lines((tmp_path / "out" / "unassigned" / "R1.fastq").read_bytes())
    assert len(un_r1) == 6 * 4


def test_demultiplex_pair_quota_guard(tmp_path):
    plan = [("ACGTACGT", "ACGTACGT")] * 50
    r1, r2 = _pair_records(plan)
    p1, p2 = tmp_path / "R1.fastq", tmp_path / "R2.fastq"
    p1.write_bytes(r1)
    p2.write_bytes(r2)
    # 上限只够写一部分
    space = SpaceManager(512)
    with pytest.raises(QuotaExceededError):
        demultiplex_pair(
            r1_path=p1,
            r2_path=p2,
            specs=[BarcodeSpec(0, "S-A", "ACGTACGT")],
            out_dir=tmp_path / "out",
            space=space,
            batch_id="b1",
        )
    # 没有未预留的多余写入：账本不超过上限
    assert space._allocated.get("b1", 0) <= 512


def test_demultiplex_pair_rejects_malformed_source(tmp_path):
    p1, p2 = tmp_path / "R1.fastq", tmp_path / "R2.fastq"
    p1.write_bytes(_illumina_record(b"r/1", "ACGTACGT", b"ACGT", b"JJJJ"))
    p2.write_bytes(b"@r/2 1:N:0:ACGTACGT\nACGT\n+\n")  # 缺质量行
    space = SpaceManager(1024)
    with pytest.raises(DemuxError):
        demultiplex_pair(
            r1_path=p1,
            r2_path=p2,
            specs=[BarcodeSpec(0, "S-A", "ACGTACGT")],
            out_dir=tmp_path / "out",
            space=space,
            batch_id="b1",
        )


# --------------------------------------------------------------------------- #
# API：请求校验
# --------------------------------------------------------------------------- #


def test_demux_request_validation(client):
    bid = _ready_batch(client, [("ACGTACGT", "ACGTACGT")])
    # 条码含非 ACGT
    assert client.post(
        f"/batches/{bid}/demux",
        json={"sample_id_a": "A", "sample_id_b": "B", "barcode_a": "ACGTN", "barcode_b": "ACGTA"},
    ).status_code == 422
    # 样本号相同
    assert _demux(client, bid, a="S", b="S").status_code == 422
    # 等长要求
    assert _demux(client, bid, ba="ACGT", bb="ACGTACGT").status_code == 422
    # 小写会被规范化接受
    r = _demux(client, bid, ba="acgtacgt", bb="acgtacga")
    assert r.status_code == 200, r.text


def test_identical_barcodes_yield_all_unassigned(client):
    """两条码完全相同时每个观察值都等距 → 全部未归样（不误归任一样本）。"""
    plan = [("ACGTACGT", "ACGTACGT")] * 4
    bid = _ready_batch(client, plan)
    r = _demux(client, bid, ba="ACGTACGT", bb="ACGTACGT")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total_pairs"] == 4
    assert body["unassigned_pairs"] == 4
    assert {s["assigned_pairs"] for s in body["samples"]} == {0}


def test_demux_requires_ready_batch(client):
    bid = _create_batch(client, "not-ready")
    assert _demux(client, bid).status_code == 409
    assert _demux(client, "missing").status_code == 404


# --------------------------------------------------------------------------- #
# API：完整拆样与下载
# --------------------------------------------------------------------------- #


def test_demux_publishes_and_downloads(client, settings):
    plan = [
        ("ACGTACGT", "ACGTACGT"),
        ("ACGTACGT", "ACGTACGT"),
        ("TTTTTTTT", "TTTTTTTT"),
        ("ACGTACGT", "TTTTTTTT"),  # 未归样
    ]
    bid = _ready_batch(client, plan)
    r = _demux(client, bid, ba="ACGTACGT", bb="TTTTTTTT")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "published"
    assert body["total_pairs"] == 4
    assert body["unassigned_pairs"] == 1
    counts = {s["sample_id"]: s["assigned_pairs"] for s in body["samples"]}
    assert counts == {"S-A": 2, "S-B": 1}
    assert body["unassigned"]["pairs"] == 1

    # 各样本 R1/R2 可下载且成对；S-A 只含前两条读对
    for sample, expected in (("S-A", 2), ("S-B", 1)):
        d1 = client.get(f"/batches/{bid}/demux/samples/{sample}/files/R1/download")
        d2 = client.get(f"/batches/{bid}/demux/samples/{sample}/files/R2/download")
        assert d1.status_code == 200 and d2.status_code == 200
        assert len(_lines(d1.content)) == expected * 4
        assert len(_lines(d2.content)) == expected * 4
        if sample == "S-A":
            assert d1.content.startswith(b"@read-0/1 1:N:0:ACGTACGT")

    u1 = client.get(f"/batches/{bid}/demux/unassigned/files/R1/download")
    u2 = client.get(f"/batches/{bid}/demux/unassigned/files/R2/download")
    assert u1.status_code == u2.status_code == 200
    assert len(_lines(u1.content)) == 4

    # 原档仍可下载且不受影响
    assert client.get(f"/batches/{bid}/files/R1/download").status_code == 200

    # staging 已消失，只剩发布目录
    root = settings.batch_dir / bid / "demux"
    assert sorted(p.name for p in root.iterdir()) == ["published"]

    # 重复拆样被拒绝
    assert _demux(client, bid).status_code == 409


def test_demux_status_endpoint_and_batch_summary(client):
    bid = _ready_batch(client, [("ACGTACGT", "ACGTACGT")])
    assert client.get(f"/batches/{bid}/demux").status_code == 404
    _demux(client, bid)
    status = client.get(f"/batches/{bid}/demux").json()
    assert status["status"] == "published"
    detail = client.get(f"/batches/{bid}").json()
    assert detail["demux"]["status"] == "published"
    assert detail["demux"]["total_pairs"] == 1


def test_unknown_sample_download_404(client):
    bid = _ready_batch(client, [("ACGTACGT", "ACGTACGT")])
    _demux(client, bid)
    assert client.get(f"/batches/{bid}/demux/samples/nope/files/R1/download").status_code == 404


# --------------------------------------------------------------------------- #
# 原子性 / 超限 / 写盘失败
# --------------------------------------------------------------------------- #


def test_demux_over_quota_leaves_no_download_and_original_intact(client, settings):
    from app.main import service

    plan = [("ACGTACGT", "ACGTACGT")] * 200
    bid = _ready_batch(client, plan)
    original_usage = (settings.batch_dir / bid / "R1.fastq").stat().st_size

    original_limit = service.space._limit
    service.space._limit = original_usage * 2 + 1024  # 只够原档 + 一点余量
    try:
        r = _demux(client, bid, ba="ACGTACGT", bb="TTTTTTTT")
        assert r.status_code == 413, r.text
    finally:
        service.space._limit = original_limit

    # 没有可下载的拆样结果
    assert client.get(f"/batches/{bid}/demux").json()["status"] == "failed"
    assert client.get(f"/batches/{bid}/demux/samples/S-A/files/R1/download").status_code == 409
    assert not (settings.batch_dir / bid / "demux" / "published").exists()
    # 原批状态不变，原档可下载
    assert client.get(f"/batches/{bid}").json()["status"] == "ready"
    assert client.get(f"/batches/{bid}/files/R1/download").status_code == 200
    # 配额账不超过新的上限
    assert service.space._allocated.get(bid, 0) <= service.space._limit

    # 放宽上限后可重试成功
    r = _demux(client, bid, ba="ACGTACGT", bb="TTTTTTTT")
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "published"


def test_demux_disk_failure_leaves_no_partial(client, settings, monkeypatch):
    from app import demux as demux_mod

    plan = [("ACGTACGT", "ACGTACGT")] * 20
    bid = _ready_batch(client, plan)

    real_write = demux_mod._FastqSink.write
    calls = {"n": 0}

    def flaky_write(self, data):
        calls["n"] += 1
        if calls["n"] > 3:
            raise OSError("disk full (simulated)")
        return real_write(self, data)

    monkeypatch.setattr(demux_mod._FastqSink, "write", flaky_write)
    r = _demux(client, bid, ba="ACGTACGT", bb="TTTTTTTT")
    assert r.status_code == 500, r.text
    assert r.json()["detail"].startswith("demux write failed")

    assert not (settings.batch_dir / bid / "demux" / "published").exists()
    # staging 也已清理
    demux_root = settings.batch_dir / bid / "demux"
    leftovers = [p for p in demux_root.iterdir()] if demux_root.exists() else []
    assert leftovers == []
    assert client.get(f"/batches/{bid}").json()["status"] == "ready"
    assert client.get(f"/batches/{bid}/files/R2/download").status_code == 200

    # 失败后允许重试（恢复正常的写入）
    monkeypatch.setattr(demux_mod._FastqSink, "write", real_write)
    assert _demux(client, bid, ba="ACGTACGT", bb="TTTTTTTT").status_code == 200


def test_demux_retry_after_failure_clears_stale_state(client):
    """一次失败（源文件被破坏）后可换参数重试成功，不复用脏记录。"""
    bid = _ready_batch(client, [("ACGTACGT", "ACGTACGT")] * 3)
    # 绕过 API 直接改盘，让 R2 条数与 R1 不一致，拆样在结构校验处失败
    from app.main import service as svc

    r2_path = svc._fastq_path(bid, FileRole.R2)
    original = r2_path.read_bytes()
    r2_path.write_bytes(b"@only-one/2 1:N:0:ACGTACGT\nAC\n+\nJJ\n")
    r = _demux(client, bid, ba="ACGTACGT", bb="TTTTTTTT")
    assert r.status_code == 422, r.text
    assert client.get(f"/batches/{bid}/demux").json()["status"] == "failed"
    r2_path.write_bytes(original)
    r = _demux(client, bid, ba="ACGTACGT", bb="TTTTTTTT")
    assert r.status_code == 200, r.text
    assert r.json()["total_pairs"] == 3


# --------------------------------------------------------------------------- #
# 并发：最多成功一次
# --------------------------------------------------------------------------- #


def test_concurrent_demux_only_one_succeeds(settings):
    """真实 uvicorn 进程下并发请求，最多一个 200，其余 409。"""
    import json
    import os
    import socket
    import subprocess
    import sys
    import tempfile
    import time
    import urllib.error
    import urllib.request
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[1]

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    data_dir = Path(tempfile.mkdtemp(prefix="fastq-demux-conc-")) / "data"
    env = {
        **os.environ,
        "FASTQ_DATA_DIR": str(data_dir),
        "FASTQ_BATCH_BYTE_LIMIT": str(64 * 1024 * 1024),
        "PYTHONPATH": str(repo_root),
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1",
         "--port", str(port), "--log-level", "warning"],
        cwd=repo_root, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(base + "/health", timeout=1):
                    break
            except OSError:
                time.sleep(0.2)

        def req(method, path, body=None):
            r = urllib.request.Request(
                base + path,
                data=body,
                headers={"Content-Type": "application/json"} if body else {},
                method=method,
            )
            try:
                with urllib.request.urlopen(r, timeout=60) as resp:
                    return resp.status, json.loads(resp.read() or b"{}")
            except urllib.error.HTTPError as e:
                return e.code, json.loads(e.read() or b"{}")

        r1, r2 = _pair_records([("ACGTACGT", "ACGTACGT")] * 3000)
        _, body = req("POST", "/batches", json.dumps({"sample_id": "conc"}).encode())
        bid = body["id"]
        req("POST", f"/batches/{bid}/files/R1", _gz(r1))
        req("POST", f"/batches/{bid}/files/R2", _gz(r2))

        payload = json.dumps(
            {"sample_id_a": "S-A", "sample_id_b": "S-B",
             "barcode_a": "ACGTACGT", "barcode_b": "TTTTTTTT"}
        ).encode()
        results: list[int] = []
        lock = threading.Lock()

        def worker():
            status, _ = req("POST", f"/batches/{bid}/demux", payload)
            with lock:
                results.append(status)

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)

        assert results.count(200) == 1, results
        assert all(s == 409 for s in results if s != 200), results
        status, body = req("GET", f"/batches/{bid}/demux")
        assert status == 200 and body["status"] == "published"
        assert body["total_pairs"] == 3000
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


# --------------------------------------------------------------------------- #
# 重启中断
# --------------------------------------------------------------------------- #


def test_restart_interrupts_running_demux_and_preserves_ready(client, settings):
    plan = [("ACGTACGT", "ACGTACGT")] * 5
    bid = _ready_batch(client, plan)
    # 直接构造一个「running 且只写了一半」的拆样状态
    root = settings.batch_dir / bid / "demux"
    staging = root / "staging-deadbeef"
    (staging / "0").mkdir(parents=True)
    (staging / "0" / "R1.fastq").write_bytes(b"@half\nAC\n+\nJJ\n")
    (staging / "unassigned").mkdir(parents=True)
    (staging / "unassigned" / "R2.fastq").write_bytes(b"@half\nAC\n+\nJJ\n")
    from app.models import utcnow_iso

    with get_conn(settings.db_path) as conn:
        cur = conn.execute(
            "INSERT INTO demux_runs (batch_id, sample_a, sample_b, barcode_a, barcode_b,"
            " status, error, total_pairs, unassigned, started_at, published_at)"
            " VALUES (?, 'S-A', 'S-B', 'ACGTACGT', 'TTTTTTTT', 'running', NULL, 0, 0, ?, NULL)",
            (bid, utcnow_iso()),
        )
        run_id = cur.lastrowid
        conn.executemany(
            "INSERT INTO demux_samples (run_id, sample_id, barcode, slot, assigned)"
            " VALUES (?, ?, ?, ?, 0)",
            [(run_id, "S-A", "ACGTACGT", 0), (run_id, "S-B", "TTTTTTTT", 1)],
        )

    new_service = BatchService(Settings())
    summary = asyncio.run(new_service.recover_on_startup())
    assert summary.get("demux_failed", 0) >= 1

    # 半成品被清掉，没有可下载结果
    assert not (root / "published").exists()
    assert not staging.exists()
    assert client.get(f"/batches/{bid}/demux").json()["status"] == "failed"
    assert client.get(f"/batches/{bid}/demux/samples/S-A/files/R1/download").status_code == 409
    # 原批仍是 ready，原档可下载
    assert client.get(f"/batches/{bid}").json()["status"] == "ready"
    assert client.get(f"/batches/{bid}/files/R1/download").status_code == 200
    # 配额账与实际磁盘一致
    from app.storage import directory_size

    usage = asyncio.run(new_service.space.usage())
    assert usage.get(bid) == directory_size(settings.batch_dir / bid)


def test_restart_keeps_published_demux(client, settings):
    bid = _ready_batch(client, [("ACGTACGT", "ACGTACGT")] * 4)
    assert _demux(client, bid, ba="ACGTACGT", bb="TTTTTTTT").status_code == 200
    new_service = BatchService(Settings())
    summary = asyncio.run(new_service.recover_on_startup())
    assert summary.get("demux_published", 0) >= 1
    assert client.get(f"/batches/{bid}/demux").json()["status"] == "published"
    assert client.get(f"/batches/{bid}/demux/samples/S-A/files/R1/download").status_code == 200


# --------------------------------------------------------------------------- #
# 配额与取消
# --------------------------------------------------------------------------- #


def test_demux_files_share_batch_quota_accounting(client, settings):
    """拆样结果计入同一批配额账，且与实际磁盘占用一致。"""
    from app.main import service
    from app.storage import directory_size

    plan = [("ACGTACGT", "ACGTACGT")] * 30
    bid = _ready_batch(client, plan)
    assert _demux(client, bid, ba="ACGTACGT", bb="TTTTTTTT").status_code == 200
    on_disk = directory_size(settings.batch_dir / bid)
    assert service.space._allocated[bid] == on_disk
    # 拆样确实增加了占用（原档 + 样本 + unassigned 双份）
    assert on_disk > 2 * (settings.batch_dir / bid / "R1.fastq").stat().st_size
