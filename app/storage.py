"""磁盘空间配额管理与批次运行期注册表。"""

from __future__ import annotations

import asyncio
import os
import shutil
import threading
from pathlib import Path

from .models import BatchRuntime, DemuxRuntime


class QuotaExceededError(Exception):
    """单批累计字节超过配置上限。"""

    def __init__(self, batch_id: str, limit: int, attempted: int, current: int) -> None:
        self.batch_id = batch_id
        self.limit = limit
        self.attempted = attempted
        self.current = current
        super().__init__(
            f"batch {batch_id} byte quota exceeded: "
            f"current={current} attempted={attempted} limit={limit}"
        )


class SpaceManager:
    """按批记账的字节配额。

    记账内容 = 该批数据卷目录下已写入的全部字节（压缩临时文件 + 展开 FASTQ +
    拆样结果）。写入前预留、失败/删除时回收。锁是线程锁：既有事件循环里的
    异步入账，也有拆样工作线程里的同步入账，二者共享同一份账本。
    """

    def __init__(self, batch_byte_limit: int) -> None:
        self._limit = batch_byte_limit
        self._allocated: dict[str, int] = {}
        self._lock = threading.RLock()

    @property
    def limit(self) -> int:
        return self._limit

    def _reserve(self, batch_id: str, additional: int) -> None:
        with self._lock:
            current = self._allocated.get(batch_id, 0)
            if current + additional > self._limit:
                raise QuotaExceededError(batch_id, self._limit, additional, current)
            self._allocated[batch_id] = current + additional

    def _release(self, batch_id: str, bytes_: int) -> None:
        if bytes_ <= 0:
            return
        with self._lock:
            current = self._allocated.get(batch_id, 0)
            self._allocated[batch_id] = max(0, current - bytes_)

    async def reserve(self, batch_id: str, additional: int) -> None:
        self._reserve(batch_id, additional)

    async def release(self, batch_id: str, bytes_: int) -> None:
        self._release(batch_id, bytes_)

    def reserve_sync(self, batch_id: str, additional: int) -> None:
        """工作线程用的同步预留（拆样落盘时使用）。"""
        self._reserve(batch_id, additional)

    def release_sync(self, batch_id: str, bytes_: int) -> None:
        """工作线程用的同步回收。"""
        self._release(batch_id, bytes_)

    async def set_batch_usage(self, batch_id: str, usage: int) -> None:
        """以磁盘真实占用为准，重置该批记账（重启恢复 / 清理后对账用）。"""
        with self._lock:
            if usage <= 0:
                self._allocated.pop(batch_id, None)
            else:
                self._allocated[batch_id] = usage

    async def remove_batch(self, batch_id: str) -> None:
        with self._lock:
            self._allocated.pop(batch_id, None)

    async def usage(self) -> dict[str, int]:
        with self._lock:
            return dict(self._allocated)


def directory_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            fp = Path(root) / name
            try:
                total += fp.stat().st_size
            except FileNotFoundError:
                pass
    return total


def remove_batch_files(batch_dir: Path) -> int:
    """删除批次目录下的全部文件与子目录（含拆样结果），返回释放的字节数。"""
    freed = directory_size(batch_dir)
    if batch_dir.exists():
        for child in batch_dir.iterdir():
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child, ignore_errors=True)
            else:
                try:
                    child.unlink()
                except FileNotFoundError:
                    pass
    return freed


def remove_path(path: Path) -> int:
    """删除单个文件或整棵子目录，返回释放的字节数（不存在则为 0）。"""
    if not path.exists() and not path.is_symlink():
        return 0
    if path.is_dir() and not path.is_symlink():
        freed = directory_size(path)
        shutil.rmtree(path, ignore_errors=True)
        return freed
    try:
        freed = path.stat().st_size
        path.unlink()
    except FileNotFoundError:
        return 0
    return freed


class BatchRegistry:
    """batch_id -> 运行期协调对象。"""

    def __init__(self) -> None:
        self._runtimes: dict[str, BatchRuntime] = {}
        self._lock = asyncio.Lock()

    async def create(self, batch_id: str) -> BatchRuntime:
        async with self._lock:
            rt = self._runtimes.get(batch_id)
            if rt is None:
                rt = BatchRuntime(batch_id=batch_id)
                self._runtimes[batch_id] = rt
            return rt

    async def get(self, batch_id: str) -> BatchRuntime | None:
        return self._runtimes.get(batch_id)

    async def remove(self, batch_id: str) -> None:
        async with self._lock:
            self._runtimes.pop(batch_id, None)


class DemuxRegistry:
    """batch_id -> 拆样运行期协调对象（并发守卫 + 取消事件 + 在途任务）。"""

    def __init__(self) -> None:
        self._runtimes: dict[str, DemuxRuntime] = {}
        self._lock = asyncio.Lock()

    async def create(self, batch_id: str) -> DemuxRuntime:
        async with self._lock:
            rt = self._runtimes.get(batch_id)
            if rt is None:
                rt = DemuxRuntime(batch_id=batch_id)
                self._runtimes[batch_id] = rt
            return rt

    async def get(self, batch_id: str) -> DemuxRuntime | None:
        return self._runtimes.get(batch_id)

    async def remove(self, batch_id: str) -> None:
        async with self._lock:
            self._runtimes.pop(batch_id, None)
