"""Pydantic 请求与响应模型。"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from .models import BatchStatus, DemuxStatus, FileRole

_BARCODE_RE = re.compile(r"^[ACGT]+$")


class BatchCreateRequest(BaseModel):
    sample_id: str = Field(min_length=1, max_length=128, description="样本编号")


class FileInfo(BaseModel):
    role: FileRole
    size: int
    uploaded_at: str


class DemuxSummary(BaseModel):
    """批次详情里附带的拆样摘要。"""

    status: DemuxStatus
    total_pairs: int
    unassigned: int
    error: str | None = None


class BatchResponse(BaseModel):
    id: str
    sample_id: str
    status: BatchStatus
    byte_limit: int
    bytes_used: int
    error: str | None = None
    files: list[FileInfo] = Field(default_factory=list)
    demux: DemuxSummary | None = Field(
        default=None, description="最近一次拆样摘要；从未拆样为 null"
    )
    created_at: str
    updated_at: str


class BatchListResponse(BaseModel):
    batches: list[BatchResponse]


class CancelResponse(BaseModel):
    id: str
    status: BatchStatus
    freed_bytes: int
    message: str


class DemuxCreateRequest(BaseModel):
    """一次性拆样请求：两个不同样本号及各自的等长 ACGT 条码。"""

    sample_id_a: str = Field(min_length=1, max_length=128, description="样本 A 编号")
    sample_id_b: str = Field(min_length=1, max_length=128, description="样本 B 编号")
    barcode_a: str = Field(min_length=1, max_length=64, description="样本 A 条码（仅 ACGT）")
    barcode_b: str = Field(min_length=1, max_length=64, description="样本 B 条码（仅 ACGT）")

    @field_validator("barcode_a", "barcode_b")
    @classmethod
    def _valid_barcode(cls, value: str) -> str:
        value = value.strip().upper()
        if not _BARCODE_RE.match(value):
            raise ValueError("barcode must contain only A/C/G/T")
        return value

    @model_validator(mode="after")
    def _check_distinct(self):
        if self.sample_id_a == self.sample_id_b:
            raise ValueError("sample_id_a and sample_id_b must be different")
        if len(self.barcode_a) != len(self.barcode_b):
            raise ValueError("barcode_a and barcode_b must have equal length")
        return self


class DemuxSampleResult(BaseModel):
    sample_id: str
    barcode: str
    assigned_pairs: int
    r1_download_url: str
    r2_download_url: str


class DemuxUnassignedInfo(BaseModel):
    pairs: int
    r1_download_url: str
    r2_download_url: str


class DemuxResponse(BaseModel):
    batch_id: str
    status: DemuxStatus
    total_pairs: int = 0
    unassigned_pairs: int = 0
    samples: list[DemuxSampleResult] = Field(default_factory=list)
    unassigned: DemuxUnassignedInfo | None = None
    error: str | None = None
    started_at: str | None = None
    published_at: str | None = None


class DemuxAcceptedResponse(BaseModel):
    batch_id: str
    status: Literal["running"]
    message: str
