"""Load env.yaml + a table YAML, apply precedence (table > env defaults > built-in), compute config_hash."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import ValidationError

from ..util.errors import ConfigError
from .models import EnvConfig, RetentionClass, TableConfig


def _load_yaml(path: str | Path) -> dict:
    p = Path(path)
    if not p.is_file():
        raise ConfigError(f"config file not found: {p}")
    data = yaml.safe_load(p.read_text()) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{p}: top level must be a mapping")
    return data


def load_env(path: str | Path) -> EnvConfig:
    try:
        return EnvConfig.model_validate(_load_yaml(path))
    except ValidationError as e:
        raise ConfigError(f"{path}: {e}") from None


def load_table(path: str | Path) -> TableConfig:
    try:
        return TableConfig.model_validate(_load_yaml(path))
    except ValidationError as e:
        raise ConfigError(f"{path}: {e}") from None


@dataclass(frozen=True)
class EffectiveTable:
    table: TableConfig
    target_fq: str
    engine: str
    format_version: int
    char_policy: str
    validation_tier: str
    merge_executor: str
    batch_target_bytes: int
    onprem_merge_budget_bytes: int
    retention: RetentionClass
    config_hash: str
    findings: tuple[str, ...]


def target_fq(env: EnvConfig, t: TableConfig) -> str:
    if t.target:
        schema, _, table = t.target.rpartition(".")
        schema = schema or env.naming.uc_schema.format(domain=t.domain, hive_db=t.hive_db)
    else:
        schema = env.naming.uc_schema.format(domain=t.domain, hive_db=t.hive_db)
        table = env.naming.uc_table.format(hive_table=t.hive_table, domain=t.domain)
    return f"{env.databricks.uc_catalog}.{schema}.{table}".lower()      # DDL-11 normalization


def config_hash(env: EnvConfig, t: TableConfig) -> str:
    doc = {"env": env.model_dump(mode="json"), "table": t.model_dump(mode="json")}
    return hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()[:16]


def effective(env: EnvConfig, t: TableConfig) -> EffectiveTable:
    d = env.defaults
    findings = []
    if t.layout.cluster_by and not t.locality_column:
        findings.append("no locality_column on a clustered target: expect near-full scans per MERGE / scoped L2 (R13)")
    if t.layout.cluster_by and len(t.layout.cluster_by) == len(t.keys) and set(t.layout.cluster_by) == set(t.keys):
        findings.append("cluster_by is the key alone: per-file key bounds overlap every batch, nothing prunes (DDL-15)")
    return EffectiveTable(
        table=t, target_fq=target_fq(env, t),
        engine=t.engine or d.engine, format_version=t.format_version or d.format_version,
        char_policy=t.char_policy or d.char_policy, validation_tier=t.validation_tier or d.validation_tier,
        merge_executor=t.merge_executor or d.merge_executor, batch_target_bytes=d.batch_target_bytes,
        onprem_merge_budget_bytes=d.onprem_merge_budget_bytes,
        retention=env.retention_classes[d.retention_class], config_hash=config_hash(env, t),
        findings=tuple(findings))
