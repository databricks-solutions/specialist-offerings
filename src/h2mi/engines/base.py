"""Engine interface (design §8.5, ARC-03, ARC-04). Everything outside `run_batch` is shared."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol


@dataclass
class BatchPlan:
    table_id: str
    target_fq: str
    mode: str                                   # iceberg-append | iceberg-upsert | iceberg-mor | hive-partition
    batch_id: str
    cycle_id: int
    from_wm: str | None
    to_wm: str | None
    partitions: list[str] = field(default_factory=list)
    merge_executor: str = "databricks"
    tags: dict[str, str] = field(default_factory=dict)      # h2mi.* snapshot-summary properties (AUD-03)


@dataclass
class CommitResult:
    target_snapshot_id: int | None
    rows_written: int
    bytes_written: int
    tags_found: bool


class RunContext(Protocol):
    run_id: str


class Engine(Protocol):
    def run_batch(self, plan: BatchPlan, ctx: RunContext) -> CommitResult: ...
