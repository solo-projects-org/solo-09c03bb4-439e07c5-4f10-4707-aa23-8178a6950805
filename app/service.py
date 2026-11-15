"""批次生命周期服务：创建、上传校验、取消、回收、重启恢复、拆样。"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

from fastapi import HTTPException

from .database import get_conn, init_db
from .demux import (
    BarcodeSpec,
    DemuxCancelled,
    DemuxError,
    DemuxOutcome,
    demultiplex_pair,
)
from .models import BatchStatus, DemuxStatus, FileRole, utcnow_iso
from .storage import (
    BatchRegistry,
    DemuxRegistry,
    QuotaExceededError,
    SpaceManager,
    directory_size,
    remove_batch_files,
    remove_path,
)
from .validation import StreamError, stream_gunzip, validate_fastq_pair, validate_fastq_structure

TERMINAL_STATUSES = {BatchStatus.READY.value, BatchStatus.FAILED.value, BatchStatus.CANCELLED.value}

DEMUX_DIR = "demux"
DEMUX_STAGING = "staging"
DEMUX_PUBLISHED = "published"


def _consume_task_exception(task: asyncio.Task) -> None:
    """吞掉后台拆样任务的异常，避免「Task exception was never retrieved」告警。

    异常本身已由 _demux_job 落库为 failed；等待方（start_demux / cancel）自行捕获。
    """
    if task.cancelled():
        return
    task.exception()


class BatchService:
    def __init__(self, settings) -> None:
        self.settings = settings
        init_db(settings.db_path)
        self.space = SpaceManager(settings.batch_byte_limit)
        self.registry = BatchRegistry()
        self.demux_registry = DemuxRegistry()
        self._recovered = False

    # ------------------------------------------------------------------ #
    # 路径
    # ------------------------------------------------------------------ #
    def _dir(self, batch_id: str) -> Path:
        return self.settings.batch_dir / batch_id

    def _gz_path(self, batch_id: str, role: FileRole) -> Path:
        return self._dir(batch_id) / f"{role.value}.fastq.gz.uploading"

    def _fastq_path(self, batch_id: str, role: FileRole) -> Path:
        return self._dir(batch_id) / f"{role.value}.fastq"

    def _demux_root(self, batch_id: str) -> Path:
        return self._dir(batch_id) / DEMUX_DIR

    def _demux_staging(self, batch_id: str, token: str) -> Path:
        return self._demux_root(batch_id) / f"{DEMUX_STAGING}-{token}"

    def _demux_published(self, batch_id: str) -> Path:
        return self._demux_root(batch_id) / DEMUX_PUBLISHED

    def _demux_sample_path(self, batch_id: str, slot: int, role: FileRole) -> Path:
        return self._demux_published(batch_id) / str(slot) / f"{role.value}.fastq"

    def _demux_unassigned_path(self, batch_id: str, role: FileRole) -> Path:
        return self._demux_published(batch_id) / "unassigned" / f"{role.value}.fastq"

    # ------------------------------------------------------------------ #
    # DB 小工具
    # ------------------------------------------------------------------ #
    def _row(self, batch_id: str) -> dict | None:
        with get_conn(self.settings.db_path) as conn:
            row = conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
            return dict(row) if row else None

    def _files(self, batch_id: str) -> list[dict]:
        with get_conn(self.settings.db_path) as conn:
            rows = conn.execute(
                "SELECT role, size, uploaded_at FROM files WHERE batch_id = ? ORDER BY role",
                (batch_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def _demux_summary(self, batch_id: str) -> dict | None:
        with get_conn(self.settings.db_path) as conn:
            row = conn.execute(
                "SELECT status, total_pairs, unassigned, error FROM demux_runs"
                " WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            return dict(row) if row else None

    # ------------------------------------------------------------------ #
    # 创建 / 查询
    # ------------------------------------------------------------------ #
    async def create_batch(self, sample_id: str) -> dict:
        batch_id = uuid.uuid4().hex
        now = utcnow_iso()
        self._dir(batch_id).mkdir(parents=True, exist_ok=True)
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "INSERT INTO batches (id, sample_id, status, byte_limit, error, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, NULL, ?, ?)",
                (batch_id, sample_id, BatchStatus.RECEIVING.value, self.settings.batch_byte_limit, now, now),
            )
        await self.registry.create(batch_id)
        return await self.get_batch_detail(batch_id)

    async def get_batch_detail(self, batch_id: str) -> dict:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        row["files"] = self._files(batch_id)
        row["bytes_used"] = directory_size(self._dir(batch_id))
        row["demux"] = self._demux_summary(batch_id)
        return row

    async def list_batches(self) -> list[dict]:
        with get_conn(self.settings.db_path) as conn:
            rows = conn.execute("SELECT id FROM batches ORDER BY created_at").fetchall()
        return [await self.get_batch_detail(r["id"]) for r in rows]

    # ------------------------------------------------------------------ #
    # 终态处理
    # ------------------------------------------------------------------ #
    def _mark_failed(self, batch_id: str, reason: str) -> None:
        now = utcnow_iso()
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE batches SET status = ?, error = ?, updated_at = ? WHERE id = ?",
                (BatchStatus.FAILED.value, reason[:2000], now, batch_id),
            )
            conn.execute("DELETE FROM files WHERE batch_id = ?", (batch_id,))

    async def _cleanup_files_only(self, batch_id: str) -> int:
        """删除本批全部文件并按磁盘真实占用回收配额，但不改批次状态。"""
        freed = await asyncio.to_thread(remove_batch_files, self._dir(batch_id))
        await self.space.release(batch_id, freed)
        await self.space.set_batch_usage(batch_id, directory_size(self._dir(batch_id)))
        return freed

    async def fail_and_cleanup(self, batch_id: str, reason: str) -> int:
        """标记失败，删除本批全部文件并按磁盘真实占用回收配额。"""
        await self._cleanup_files_only(batch_id)
        self._mark_failed(batch_id, reason)
        return 0

    # ------------------------------------------------------------------ #
    # 上传
    # ------------------------------------------------------------------ #
    async def upload(self, batch_id: str, role: FileRole, request) -> dict:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if row["status"] != BatchStatus.RECEIVING.value:
            raise HTTPException(status_code=409, detail=f"batch is {row['status']}, upload rejected")

        with get_conn(self.settings.db_path) as conn:
            dup = conn.execute(
                "SELECT 1 FROM files WHERE batch_id = ? AND role = ?", (batch_id, role.value)
            ).fetchone()
        if dup:
            raise HTTPException(status_code=409, detail=f"{role.value} already uploaded")

        runtime = await self.registry.create(batch_id)
        if runtime.cancel_event.is_set():
            raise HTTPException(status_code=409, detail="batch is cancelling")

        current_task = asyncio.current_task()
        if current_task is not None:
            runtime.uploads[role] = current_task

        gz_path = self._gz_path(batch_id, role)
        out_path = self._fastq_path(batch_id, role)
        gz_bytes = 0
        try:
            try:
                out_bytes = await stream_gunzip(
                    request=request,
                    gz_path=gz_path,
                    out_path=out_path,
                    space=self.space,
                    batch_id=batch_id,
                    cancel_event=runtime.cancel_event,
                    idle_timeout=self.settings.upload_idle_timeout,
                )
                gz_bytes = gz_path.stat().st_size

                # 单端四行制结构校验（线程内做，避免阻塞事件循环）
                await asyncio.to_thread(validate_fastq_structure, out_path)

                # 压缩临时文件已完成使命，立刻删除并回收其占用配额
                gz_path.unlink()
                await self.space.release(batch_id, gz_bytes)

                now = utcnow_iso()
                with get_conn(self.settings.db_path) as conn:
                    conn.execute(
                        "INSERT INTO files (batch_id, role, path, size, uploaded_at)"
                        " VALUES (?, ?, ?, ?, ?)",
                        (batch_id, role.value, str(out_path), out_bytes, now),
                    )
                    conn.execute(
                        "UPDATE batches SET updated_at = ? WHERE id = ?", (now, batch_id)
                    )
            except QuotaExceededError as exc:
                await self.fail_and_cleanup(batch_id, str(exc))
                raise HTTPException(status_code=413, detail=str(exc)) from exc
            except (StreamError, ValueError, OSError) as exc:
                if runtime.cancel_event.is_set():
                    # 取消导致的中断：只清理磁盘，终态由 cancel() 落库，避免抢占为 failed
                    await self._cleanup_files_only(batch_id)
                    raise HTTPException(status_code=409, detail="batch cancelled") from exc
                await self.fail_and_cleanup(batch_id, f"{role.value} rejected: {exc}")
                raise HTTPException(
                    status_code=422, detail=f"{role.value} rejected: {exc}"
                ) from exc
            except HTTPException:
                raise
            except Exception as exc:  # 客户端断连等兜底
                if runtime.cancel_event.is_set():
                    await self._cleanup_files_only(batch_id)
                    raise HTTPException(status_code=409, detail="batch cancelled") from exc
                await self.fail_and_cleanup(batch_id, f"{role.value} upload failed: {exc}")
                raise HTTPException(
                    status_code=500, detail=f"{role.value} upload failed: {exc}"
                ) from exc

            # 两端都到齐则做配对校验
            detail = await self.get_batch_detail(batch_id)
            roles = {f["role"] for f in detail["files"]}
            if {FileRole.R1, FileRole.R2}.issubset(roles):
                try:
                    await asyncio.to_thread(
                        validate_fastq_pair,
                        self._fastq_path(batch_id, FileRole.R1),
                        self._fastq_path(batch_id, FileRole.R2),
                    )
                except ValueError as exc:
                    await self.fail_and_cleanup(batch_id, f"pair validation failed: {exc}")
                    raise HTTPException(
                        status_code=422, detail=f"pair validation failed: {exc}"
                    ) from exc
                now = utcnow_iso()
                with get_conn(self.settings.db_path) as conn:
                    conn.execute(
                        "UPDATE batches SET status = ?, error = NULL, updated_at = ? WHERE id = ?",
                        (BatchStatus.READY.value, now, batch_id),
                    )
            return await self.get_batch_detail(batch_id)
        finally:
            runtime.uploads.pop(role, None)

    # ------------------------------------------------------------------ #
    # 取消
    # ------------------------------------------------------------------ #
    async def cancel(self, batch_id: str) -> dict:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if row["status"] in TERMINAL_STATUSES:
            raise HTTPException(status_code=409, detail=f"batch already {row['status']}")

        runtime = await self.registry.create(batch_id)
        runtime.cancel_event.set()

        # 拆样任务（无论源批处于何种状态）先停下，避免与下面的删除/状态落库竞争。
        # 只置取消信号、不 cancel 任务：工作线程会按记录检查信号后退出，
        # 等到任务真正结束（线程已退出）再删除目录，避免「取消返回后仍有落盘」。
        demux_runtime = await self.demux_registry.get(batch_id)
        if demux_runtime is not None and demux_runtime.task is not None:
            demux_runtime.cancel_event.set()
            await asyncio.gather(demux_runtime.task, return_exceptions=True)
            await self._discard_demux(batch_id, delete_record=True)

        # 等待本批正在进行的写入停止（上传任务退出后不会再落盘）
        if runtime.uploads:
            await asyncio.gather(*runtime.uploads.values(), return_exceptions=True)

        # 上传任务可能在取消信号到达前恰好完成（ready）或自行失败，重新确认终态
        refreshed = self._row(batch_id)
        if refreshed is not None and refreshed["status"] in TERMINAL_STATUSES:
            raise HTTPException(
                status_code=409,
                detail=f"batch already {refreshed['status']} before cancel took effect",
            )

        # 文件删除、空间释放、状态落库全部完成后才确认
        freed = await asyncio.to_thread(remove_batch_files, self._dir(batch_id))
        await self.space.release(batch_id, freed)
        await self.space.set_batch_usage(batch_id, directory_size(self._dir(batch_id)))
        now = utcnow_iso()
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE batches SET status = ?, error = ?, updated_at = ? WHERE id = ?",
                (BatchStatus.CANCELLED.value, "cancelled by operator", now, batch_id),
            )
            conn.execute("DELETE FROM files WHERE batch_id = ?", (batch_id,))
            conn.execute("DELETE FROM demux_runs WHERE batch_id = ?", (batch_id,))
        await self.registry.remove(batch_id)
        await self.demux_registry.remove(batch_id)
        return {
            "id": batch_id,
            "status": BatchStatus.CANCELLED,
            "freed_bytes": freed,
            "message": "all writes stopped, files deleted and space released",
        }

    # ------------------------------------------------------------------ #
    # 下载
    # ------------------------------------------------------------------ #
    def download_path(self, batch_id: str, role: FileRole) -> tuple[Path, str]:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if row["status"] != BatchStatus.READY.value:
            raise HTTPException(status_code=409, detail=f"batch is {row['status']}, not downloadable")
        path = self._fastq_path(batch_id, role)
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"{role.value} file missing on disk")
        return path, f"{row['sample_id']}_{role.value}.fastq"

    # ------------------------------------------------------------------ #
    # 拆样
    # ------------------------------------------------------------------ #
    def _run_row(self, batch_id: str) -> dict | None:
        with get_conn(self.settings.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM demux_runs WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            return dict(row) if row else None

    def _run_samples(self, run_id: int) -> list[dict]:
        with get_conn(self.settings.db_path) as conn:
            rows = conn.execute(
                "SELECT sample_id, barcode, slot, assigned FROM demux_samples"
                " WHERE run_id = ? ORDER BY slot",
                (run_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    async def start_demux(
        self,
        batch_id: str,
        sample_id_a: str,
        sample_id_b: str,
        barcode_a: str,
        barcode_b: str,
    ) -> dict:
        """认领一次拆样并同步执行；并发请求最多一个成功，失败后允许重试。"""
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if row["status"] != BatchStatus.READY.value:
            raise HTTPException(
                status_code=409,
                detail=f"batch is {row['status']}, only ready batches can be demultiplexed",
            )

        runtime = await self.demux_registry.create(batch_id)
        async with runtime.lock:
            existing = self._run_row(batch_id)
            if existing is not None:
                if existing["status"] == DemuxStatus.RUNNING.value:
                    raise HTTPException(status_code=409, detail="demultiplex already running")
                if existing["status"] == DemuxStatus.PUBLISHED.value:
                    raise HTTPException(
                        status_code=409, detail="batch already demultiplexed and published"
                    )
                # 上次失败：清掉残留（staging / 未发布目录）与记录后重试
                await self._discard_demux(batch_id, delete_record=True)
            else:
                await self._discard_demux(batch_id, delete_record=False)

            run_id = self._insert_run(batch_id, sample_id_a, sample_id_b, barcode_a, barcode_b)
            token = uuid.uuid4().hex

        specs = [
            BarcodeSpec(index=0, sample_id=sample_id_a, barcode=barcode_a),
            BarcodeSpec(index=1, sample_id=sample_id_b, barcode=barcode_b),
        ]
        task = asyncio.create_task(self._demux_job(batch_id, run_id, specs, token))
        runtime.task = task
        task.add_done_callback(_consume_task_exception)

        # 客户端断连不打断拆样任务：任务照常完成或失败，状态始终一致
        await asyncio.shield(task)
        return await self.get_demux(batch_id)


    def _insert_run(
        self, batch_id: str, sample_id_a: str, sample_id_b: str, barcode_a: str, barcode_b: str
    ) -> int:
        now = utcnow_iso()
        with get_conn(self.settings.db_path) as conn:
            cur = conn.execute(
                "INSERT INTO demux_runs"
                " (batch_id, sample_a, sample_b, barcode_a, barcode_b, status, error,"
                "  total_pairs, unassigned, started_at, published_at)"
                " VALUES (?, ?, ?, ?, ?, ?, NULL, 0, 0, ?, NULL)",
                (
                    batch_id,
                    sample_id_a,
                    sample_id_b,
                    barcode_a,
                    barcode_b,
                    DemuxStatus.RUNNING.value,
                    now,
                ),
            )
            run_id = cur.lastrowid
            conn.executemany(
                "INSERT INTO demux_samples (run_id, sample_id, barcode, slot, assigned)"
                " VALUES (?, ?, ?, ?, 0)",
                [
                    (run_id, sample_id_a, barcode_a, 0),
                    (run_id, sample_id_b, barcode_b, 1),
                ],
            )
        return run_id

    async def _discard_demux(self, batch_id: str, *, delete_record: bool) -> int:
        """删除本批的拆样文件（staging 与 published）并按磁盘真实占用对账。"""
        root = self._demux_root(batch_id)
        freed = await asyncio.to_thread(remove_path, root)
        if delete_record:
            with get_conn(self.settings.db_path) as conn:
                conn.execute("DELETE FROM demux_runs WHERE batch_id = ?", (batch_id,))
        await self.space.set_batch_usage(batch_id, directory_size(self._dir(batch_id)))
        return freed

    def _mark_run_failed(self, run_id: int, reason: str) -> None:
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE demux_runs SET status = ?, error = ? WHERE id = ?",
                (DemuxStatus.FAILED.value, reason[:2000], run_id),
            )

    async def _demux_job(
        self, batch_id: str, run_id: int, specs: list[BarcodeSpec], token: str
    ) -> None:
        """工作线程拆样 → staging 完整后原子发布；任何失败都不留可下载的半成品。"""
        staging = self._demux_staging(batch_id, token)
        published = self._demux_published(batch_id)
        try:
            outcome: DemuxOutcome = await asyncio.to_thread(
                demultiplex_pair,
                r1_path=self._fastq_path(batch_id, FileRole.R1),
                r2_path=self._fastq_path(batch_id, FileRole.R2),
                specs=specs,
                out_dir=staging,
                space=self.space,
                batch_id=batch_id,
            )
            # 全部样本的 R1/R2 均已关闭落盘，整体 rename 后其它请求才看得到
            if published.exists():
                raise DemuxError("published demux directory already exists")
            staging.rename(published)
            self._publish_counts(run_id, outcome)
            await self.space.set_batch_usage(batch_id, directory_size(self._dir(batch_id)))
        except QuotaExceededError as exc:
            await self._demux_failed(batch_id, run_id, staging, str(exc))
            raise HTTPException(status_code=413, detail=str(exc)) from exc
        except DemuxCancelled as exc:
            await self._demux_failed(batch_id, run_id, staging, str(exc))
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except DemuxError as exc:
            await self._demux_failed(batch_id, run_id, staging, str(exc))
            raise HTTPException(
                status_code=422, detail=f"demux failed: {exc}"
            ) from exc
        except asyncio.CancelledError:
            # 进程关闭等取消：不在这里删目录（工作线程可能仍在写），
            # 保持 DB 为 running，启动恢复时统一判失败并清理
            raise
        except HTTPException:
            raise
        except Exception as exc:
            await self._demux_failed(batch_id, run_id, staging, f"demux write failed: {exc}")
            raise HTTPException(
                status_code=500, detail=f"demux write failed: {exc}"
            ) from exc

    async def _demux_failed(self, batch_id: str, run_id: int, staging: Path, reason: str) -> None:
        await asyncio.to_thread(remove_path, staging)
        # published 不应存在（只会在 rename 成功后写 DB）；保险起见一并清掉
        await asyncio.to_thread(remove_path, self._demux_published(batch_id))
        self._mark_run_failed(run_id, reason)
        await self.space.set_batch_usage(batch_id, directory_size(self._dir(batch_id)))

    def _publish_counts(self, run_id: int, outcome: DemuxOutcome) -> None:
        now = utcnow_iso()
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE demux_runs SET status = ?, error = NULL, total_pairs = ?,"
                " unassigned = ?, published_at = ? WHERE id = ?",
                (
                    DemuxStatus.PUBLISHED.value,
                    outcome.total_pairs,
                    outcome.unassigned_pairs,
                    now,
                    run_id,
                ),
            )
            for slot, count in outcome.sample_counts.items():
                conn.execute(
                    "UPDATE demux_samples SET assigned = ? WHERE run_id = ? AND slot = ?",
                    (count, run_id, slot),
                )

    async def get_demux(self, batch_id: str) -> dict:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        run = self._run_row(batch_id)
        if run is None:
            raise HTTPException(status_code=404, detail="no demultiplex run for this batch")
        published = run["status"] == DemuxStatus.PUBLISHED.value
        samples = []
        if published:
            for sample in self._run_samples(run["id"]):
                samples.append(
                    {
                        "sample_id": sample["sample_id"],
                        "barcode": sample["barcode"],
                        "assigned_pairs": sample["assigned"],
                        "r1_download_url": f"{self._demux_prefix(batch_id)}samples/"
                        f"{sample['sample_id']}/files/R1/download",
                        "r2_download_url": f"{self._demux_prefix(batch_id)}samples/"
                        f"{sample['sample_id']}/files/R2/download",
                    }
                )
        unassigned = None
        if published:
            unassigned = {
                "pairs": run["unassigned"],
                "r1_download_url": f"{self._demux_prefix(batch_id)}unassigned/files/R1/download",
                "r2_download_url": f"{self._demux_prefix(batch_id)}unassigned/files/R2/download",
            }
        return {
            "batch_id": batch_id,
            "status": run["status"],
            "total_pairs": run["total_pairs"],
            "unassigned_pairs": run["unassigned"],
            "samples": samples,
            "unassigned": unassigned,
            "error": run["error"],
            "started_at": run["started_at"],
            "published_at": run["published_at"],
        }

    @staticmethod
    def _demux_prefix(batch_id: str) -> str:
        return f"/batches/{batch_id}/demux/"

    def _demux_run_published(self, batch_id: str) -> dict:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        run = self._run_row(batch_id)
        if run is None:
            raise HTTPException(status_code=404, detail="no demultiplex run for this batch")
        if run["status"] != DemuxStatus.PUBLISHED.value:
            raise HTTPException(
                status_code=409, detail=f"demultiplex is {run['status']}, not downloadable"
            )
        return run

    def demux_sample_download(self, batch_id: str, sample_id: str, role: FileRole) -> tuple[Path, str]:
        self._demux_run_published(batch_id)
        with get_conn(self.settings.db_path) as conn:
            sample = conn.execute(
                "SELECT slot, barcode FROM demux_samples WHERE run_id ="
                " (SELECT id FROM demux_runs WHERE batch_id = ?) AND sample_id = ?",
                (batch_id, sample_id),
            ).fetchone()
        if sample is None:
            raise HTTPException(status_code=404, detail="sample not part of this demultiplex run")
        path = self._demux_sample_path(batch_id, sample["slot"], role)
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"{role.value} file missing on disk")
        return path, f"{sample_id}_{role.value}.fastq"

    def demux_unassigned_download(self, batch_id: str, role: FileRole) -> tuple[Path, str]:
        self._demux_run_published(batch_id)
        path = self._demux_unassigned_path(batch_id, role)
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"unassigned {role.value} missing on disk")
        return path, f"{batch_id}_unassigned_{role.value}.fastq"


    # ------------------------------------------------------------------ #
    # 重启恢复
    # ------------------------------------------------------------------ #
    async def recover_on_startup(self) -> dict:
        """通过批保留；未完成批（receiving）一律判失败并回收磁盘后再对外服务。"""
        # 同一 service 实例只恢复一次，避免测试中多个 ASGI 客户端重跑 lifespan 误判在途批
        if self._recovered:
            return {"ready": 0, "aborted": 0, "orphaned": 0, "skipped": "already-recovered"}
        self._recovered = True
        self.settings.batch_dir.mkdir(parents=True, exist_ok=True)
        recovered = {"ready": 0, "aborted": 0, "orphaned": 0}

        with get_conn(self.settings.db_path) as conn:
            rows = conn.execute("SELECT id, status FROM batches").fetchall()
            known = {r["id"]: r["status"] for r in rows}

        for batch_id, status in known.items():
            bdir = self._dir(batch_id)
            if status == BatchStatus.READY.value:
                # 以磁盘真实文件为准重建配额账
                await self.space.set_batch_usage(batch_id, directory_size(bdir))
                recovered["ready"] += 1
                continue
            if status == BatchStatus.RECEIVING.value:
                await asyncio.to_thread(remove_batch_files, bdir)
                await self.space.remove_batch(batch_id)
                self._mark_failed(batch_id, "interrupted by server restart")
                recovered["aborted"] += 1
            # failed / cancelled：清理可能残留的文件
            else:
                await asyncio.to_thread(remove_batch_files, bdir)
                await self.space.remove_batch(batch_id)

        # 磁盘上有目录但 DB 无记录的孤儿数据，直接回收
        for child in self.settings.batch_dir.iterdir():
            if child.is_dir() and child.name not in known:
                await asyncio.to_thread(remove_batch_files, child)
                recovered["orphaned"] += 1

        # 拆样：未完结（running）一律判失败并删掉 staging/未发布产物；
        # 已发布结果保留，按磁盘真实占用重建配额账。原批 ready 状态不受影响。
        with get_conn(self.settings.db_path) as conn:
            runs = conn.execute(
                "SELECT id, batch_id, status FROM demux_runs"
            ).fetchall()

        for run in runs:
            bdir = self._dir(run["batch_id"])
            published = self._demux_published(run["batch_id"])
            root = self._demux_root(run["batch_id"])
            token_dirs = list(root.glob(f"{DEMUX_STAGING}-*")) if root.exists() else []
            if run["status"] == DemuxStatus.PUBLISHED.value:
                if not published.exists():
                    # 发布记录在、文件不见了：判失败，避免提供坏下载
                    self._mark_run_failed(run["id"], "demux files missing after restart")
                    recovered["demux_failed"] = recovered.get("demux_failed", 0) + 1
                else:
                    recovered["demux_published"] = recovered.get("demux_published", 0) + 1
            else:
                for token_dir in token_dirs:
                    await asyncio.to_thread(remove_path, token_dir)
                await asyncio.to_thread(remove_path, published)
                if run["status"] == DemuxStatus.RUNNING.value:
                    self._mark_run_failed(run["id"], "interrupted by server restart")
                    recovered["demux_failed"] = recovered.get("demux_failed", 0) + 1
            if bdir.exists():
                await self.space.set_batch_usage(run["batch_id"], directory_size(bdir))
        return recovered
