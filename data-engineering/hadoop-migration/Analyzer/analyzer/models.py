"""Data models for the Hadoop workload inventory."""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class WorkloadType(Enum):
    SPARK = "spark"
    HIVE = "hive"
    SQOOP = "sqoop"
    MAPREDUCE = "mapreduce"
    SHELL = "shell"
    HBASE = "hbase"
    IMPALA = "impala"
    UNKNOWN = "unknown"


@dataclass
class CodeArtifact:
    """Represents a code artifact (JAR, script, SQL file, etc.)."""
    path: str
    location_type: str  # "hdfs" | "local" | "embedded"
    artifact_type: str  # "jar" | "hql" | "py" | "sh" | "xml" | "sql"
    verified_exists: Optional[bool] = None

    def to_dict(self) -> dict:
        d = {
            "path": self.path,
            "location_type": self.location_type,
            "artifact_type": self.artifact_type,
        }
        if self.verified_exists is not None:
            d["verified_exists"] = self.verified_exists
        return d


@dataclass
class WorkloadInventoryItem:
    """Represents a single workload in the inventory."""
    workload_id: str
    workload_name: str
    workload_type: WorkloadType
    user: str
    queue: str
    entry_point: Optional[str] = None
    code_artifacts: List[CodeArtifact] = field(default_factory=list)
    dependencies: List[CodeArtifact] = field(default_factory=list)
    oozie_workflow_id: Optional[str] = None
    oozie_workflow_name: Optional[str] = None
    oozie_app_path: Optional[str] = None
    yarn_app_id: Optional[str] = None
    source: str = ""
    tags: List[str] = field(default_factory=list)

    # Additional metadata from profiler
    final_status: Optional[str] = None
    started_time: Optional[int] = None
    finished_time: Optional[int] = None
    elapsed_time: Optional[int] = None
    memory_seconds: Optional[int] = None
    vcore_seconds: Optional[int] = None
    diagnostics: Optional[str] = None

    # Impala-specific
    database: Optional[str] = None
    query_type: Optional[str] = None
    rows_produced: Optional[int] = None
    duration_millis: Optional[int] = None

    def to_dict(self) -> dict:
        d = {
            "workload_id": self.workload_id,
            "workload_name": self.workload_name,
            "workload_type": self.workload_type.value,
            "user": self.user,
            "queue": self.queue,
            "source": self.source,
            "tags": self.tags,
        }
        if self.entry_point:
            d["entry_point"] = self.entry_point
        if self.code_artifacts:
            d["code_artifacts"] = [a.to_dict() for a in self.code_artifacts]
        if self.dependencies:
            d["dependencies"] = [a.to_dict() for a in self.dependencies]
        if self.oozie_workflow_id:
            d["oozie_workflow_id"] = self.oozie_workflow_id
        if self.oozie_workflow_name:
            d["oozie_workflow_name"] = self.oozie_workflow_name
        if self.oozie_app_path:
            d["oozie_app_path"] = self.oozie_app_path
        if self.yarn_app_id:
            d["yarn_app_id"] = self.yarn_app_id
        if self.final_status:
            d["final_status"] = self.final_status
        if self.started_time is not None:
            d["started_time"] = self.started_time
        if self.finished_time is not None:
            d["finished_time"] = self.finished_time
        if self.elapsed_time is not None:
            d["elapsed_time"] = self.elapsed_time
        if self.memory_seconds is not None:
            d["memory_seconds"] = self.memory_seconds
        if self.vcore_seconds is not None:
            d["vcore_seconds"] = self.vcore_seconds
        if self.database:
            d["database"] = self.database
        if self.query_type:
            d["query_type"] = self.query_type
        if self.rows_produced is not None:
            d["rows_produced"] = self.rows_produced
        if self.duration_millis is not None:
            d["duration_millis"] = self.duration_millis
        return d
