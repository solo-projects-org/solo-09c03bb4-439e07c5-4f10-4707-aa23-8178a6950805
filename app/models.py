"""批次状态模型与运行期注册表。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


class BatchStatus(str, Enum):
    RECEIVING = "receiving"
    READY = "ready"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FileRole(str, Enum):
    R1 = "R1"
    R2 = "R2"


class DemuxStatus(str, Enum):
    RUNNING = "running"
    PUBLISHED = "published"
    FAILED = "failed"


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass
class ActiveUpload:
    role: FileRole
    task: asyncio.Task


@dataclass
class BatchRuntime:
    """内存中的批次协调对象：取消事件与正在进行的上传任务。"""

    batch_id: str
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    uploads: dict[FileRole, asyncio.Task] = field(default_factory=dict)


@dataclass
class DemuxRuntime:
    """一次拆样任务的内存协调对象：并发守卫、取消事件与在途任务。"""

    batch_id: str
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task | None = None
    # 串行化同一批的「检查状态 → 置 running → 写文件 → 发布」全过程
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
