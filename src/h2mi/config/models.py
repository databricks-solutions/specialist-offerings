"""Configuration model (design §6). Two YAML layers validated with pydantic: env.yaml and one file per table.
Secrets appear only as references (vault:/aws-sm:/env:), never as values (SEC-02)."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

GIB = 1024 ** 3
Mode = Literal["auto", "iceberg-append", "iceberg-upsert", "iceberg-mor", "hive-partition"]
Engine = Literal["onprem-push", "cloud-landing"]
CharPolicy = Literal["preserve_padded", "trim"]
Tier = Literal["standard", "financial"]
Executor = Literal["auto", "databricks", "onprem"]
_SECRET_SCHEMES = ("vault:", "aws-sm:", "env:", "dbx-profile:")


class Databricks(BaseModel):
    host: str
    warehouse_id: str
    uc_catalog: str
    region: str


class Principal(BaseModel):
    client_id: str
    client_secret: str = Field(description="a secret REFERENCE, e.g. vault:<path>#secret; never the value")

    @field_validator("client_secret")
    @classmethod
    def _reference_only(cls, v: str) -> str:
        if not v.startswith(_SECRET_SCHEMES):
            raise ValueError(f"client_secret must be a reference starting with one of {_SECRET_SCHEMES} (SEC-02)")
        return v


class Principals(BaseModel):
    ddl_admin: Principal
    sync: dict[str, Principal]                   # per domain (D6)


class Source(BaseModel):
    hms_uri: str
    hive_metastore_version: str = "3.1.3"


class State(BaseModel):
    hms_database: str = "h2mi_ctl"
    hdfs_root: str
    uc_mirror_schema: str = "h2mi_ctl"


class Naming(BaseModel):
    uc_schema: str = "{domain}"
    uc_table: str = "{hive_table}"


class RetentionClass(BaseModel):
    log_retention: str = "interval 30 days"
    deleted_file_retention: str = "interval 7 days"


class Defaults(BaseModel):
    engine: Engine = "onprem-push"
    format_version: int = 3
    char_policy: CharPolicy = "preserve_padded"
    ns_policy: str = "hold"
    validation_tier: Tier = "standard"
    batch_target_bytes: int = 256 * GIB
    late_data_lookback_partitions: int = 7
    retention_class: str = "standard"
    merge_executor: Executor = "auto"
    onprem_merge_budget_bytes: int = 50 * GIB
    l2_mode: Literal["scoped_async", "sync", "off"] = "scoped_async"
    l2_sample_partitions_per_cycle: int = 2
    replace_density_threshold: float = 0.25


class EnvConfig(BaseModel):
    env: str
    databricks: Databricks
    principals: Principals
    source: Source
    state: State
    naming: Naming = Naming()
    defaults: Defaults = Defaults()
    retention_classes: dict[str, RetentionClass] = {"standard": RetentionClass()}

    @model_validator(mode="after")
    def _retention_known(self) -> EnvConfig:
        if self.defaults.retention_class not in self.retention_classes:
            raise ValueError(f"defaults.retention_class {self.defaults.retention_class!r} is not in retention_classes")
        return self


class Layout(BaseModel):
    cluster_by: list[str] | None = None
    partitioned_by: list[str] | None = None

    @model_validator(mode="after")
    def _exactly_one(self) -> Layout:
        if bool(self.cluster_by) == bool(self.partitioned_by):
            raise ValueError("layout: exactly one of cluster_by | partitioned_by (DDL-15; they cannot be combined on MI)")
        return self


class ColumnDecision(BaseModel):
    redesign: str | None = None                  # e.g. bigint_seconds for INTERVAL (DT-04)
    ns_policy: str | None = None                 # accept_truncation | hold (DT-05)


class TableConfig(BaseModel):
    source: str                                  # hive_db.table
    domain: str
    target: str | None = None                    # <schema>.<table>; else naming rules
    mode: Mode = "auto"
    keys: list[str] = []
    layout: Layout
    locality_column: str | None = None           # INC-17
    engine: Engine | None = None
    format_version: int | None = None
    char_policy: CharPolicy | None = None
    column_decisions: dict[str, ColumnDecision] = {}
    validation_tier: Tier | None = None
    merge_executor: Executor | None = None
    propagate_partition_drops: bool = False

    @model_validator(mode="after")
    def _layout_rules(self) -> TableConfig:
        if self.layout.cluster_by:
            if not self.keys:
                raise ValueError("cluster_by requires keys: a clustered target can only be restated by MERGE (DDL-15)")
            if self.locality_column and self.layout.cluster_by[0] != self.locality_column:
                raise ValueError("cluster_by must lead with locality_column (INC-17)")
            if self.mode == "hive-partition":
                raise ValueError("hive-partition mode needs a PARTITIONED BY target (DDL-15)")
        if "." not in self.source:
            raise ValueError("source must be <hive_db>.<table>")
        return self

    @property
    def hive_db(self) -> str:
        return self.source.split(".", 1)[0]

    @property
    def hive_table(self) -> str:
        return self.source.split(".", 1)[1]
