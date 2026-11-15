"""Pydantic response models."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class IngestResponse(BaseModel):
    status: str
    files_ingested: int
    total_rows: int
    shard_count: int
    shards: list["ShardInfo"]


class ShardInfo(BaseModel):
    shard_id: int
    status: str
    total_rows: int
    file: str


class WithdrawalCreated(BaseModel):
    task_id: str
    subject_id: str
    status: str
    affected_shards: int
    affected_rows: int
    reused: bool


class WithdrawalStatus(BaseModel):
    task_id: str
    subject_id: str
    status: str
    affected_shards: int
    affected_rows: int
    purged_rows: int
    source_purged: int
    error: Optional[str] = None
    created_at: str
    updated_at: str
    shards: list["WithdrawalShardState"]


class WithdrawalShardState(BaseModel):
    shard_id: int
    state: str
    purged_rows_in_shard: int


class ShardStatus(BaseModel):
    shard_id: int
    status: str
    total_rows: int
    available: bool
    file: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    instance_id: str
    ingested: bool


IngestResponse.model_rebuild()
WithdrawalStatus.model_rebuild()
