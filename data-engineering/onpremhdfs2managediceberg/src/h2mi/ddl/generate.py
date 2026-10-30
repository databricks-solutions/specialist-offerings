"""DDL generation (design §8.3). Targets are created only through the Statement API, never IRC createTable (D9).

Layout rules (DDL-15): exactly one of CLUSTER BY / PARTITIONED BY. Keyed MERGE targets are clustered and get
merge-on-read write modes (external Spark then writes deletion vectors instead of rewriting files, lab 2026-10-22).
Keyless and hive-partition targets are identity-partitioned: that is the only layout `overwritePartitions()` works on.
Staging tables are partitioned by `_h2mi_batch_id` so clearing a batch is a metadata-only partition drop.
"""
from __future__ import annotations

from ..config.models import RetentionClass
from ..scanner.typemap import ColumnPlan, quote
from ..util.errors import ConfigError

DENYLIST = {"schema.name-mapping.default", "delta.enableChangeDataFeed", "gc.enabled",
            "history.expire.max-snapshot-age-ms"}                                              # DDL-06
MOR_PROPS = {"write.merge.mode": "merge-on-read", "write.update.mode": "merge-on-read",
             "write.delete.mode": "merge-on-read"}
STAGING_PREFIX = "__h2mi_stg_"
STAGING_COLS = [("_h2mi_change", "STRING"), ("_h2mi_batch_id", "STRING")]


def check_denylist(props: dict[str, str]) -> None:
    bad = sorted(DENYLIST & set(props))
    if bad:
        raise ConfigError(f"prohibited or locked table properties (DDL-06): {bad}")


def _esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "\\'")


def _props(props: dict[str, str]) -> str:
    return ", ".join(f"'{k}'='{v}'" for k, v in sorted(props.items()))


def _columns(cols: list[ColumnPlan], extra: list[tuple[str, str]] = ()) -> str:
    bad = [c.name for c in cols if c.target_type is None]
    if bad:
        raise ConfigError(f"columns without a target type (REDESIGN / BLOCK): {bad}")
    lines = [f"{quote(c.name)} {c.target_type}" + (f" COMMENT '{_esc(c.comment)}'" if c.comment else "") for c in cols]
    lines += [f"{quote(n)} {t}" for n, t in extra]
    return ",\n  ".join(lines)


def create_table_ddl(fq: str, cols: list[ColumnPlan], *, cluster_by: list[str] | None = None,
                     partitioned_by: list[str] | None = None, format_version: int = 3,
                     retention: RetentionClass | None = None, comment: str | None = None,
                     extra_props: dict[str, str] | None = None) -> str:
    if bool(cluster_by) == bool(partitioned_by):
        raise ConfigError("layout must be exactly one of cluster_by | partitioned_by (DDL-15)")
    props = {"format-version": str(format_version)}
    if cluster_by:
        props.update(MOR_PROPS)
    if retention:
        props["delta.logRetentionDuration"] = retention.log_retention
        props["delta.deletedFileRetentionDuration"] = retention.deleted_file_retention
    props.update(extra_props or {})
    check_denylist(props)
    ddl = f"CREATE TABLE IF NOT EXISTS {fq} (\n  {_columns(cols)}\n) USING ICEBERG"
    if cluster_by:
        ddl += f"\nCLUSTER BY ({', '.join(quote(c) for c in cluster_by)})"
    else:
        ddl += f"\nPARTITIONED BY ({', '.join(quote(c) for c in partitioned_by)})"
    if comment:
        ddl += f"\nCOMMENT '{_esc(comment)}'"
    return ddl + f"\nTBLPROPERTIES ({_props(props)})"


def staging_name(fq_target: str) -> str:
    head, _, table = fq_target.rpartition(".")
    return f"{head}.{STAGING_PREFIX}{table}" if head else f"{STAGING_PREFIX}{table}"


def staging_table_ddl(fq_target: str, cols: list[ColumnPlan], format_version: int = 3) -> str:
    """Per-table staging table for the MERGE modes (§8.3 step 4, INC-08): same columns + change kind + batch id."""
    return (f"CREATE TABLE IF NOT EXISTS {staging_name(fq_target)} (\n  {_columns(cols, STAGING_COLS)}\n) USING ICEBERG"
            f"\nPARTITIONED BY ({quote('_h2mi_batch_id')})"
            f"\nTBLPROPERTIES ({_props({'format-version': str(format_version)})})")


def grants(fq_target: str, sync_sp: str, readers: list[str] = ()) -> list[str]:
    """DDL-08: the sync SP gets USE CATALOG / USE SCHEMA / SELECT / MODIFY / EXTERNAL USE SCHEMA, readers SELECT."""
    cat, sch, _ = fq_target.split(".")
    out = [f"GRANT USE CATALOG ON CATALOG {cat} TO `{sync_sp}`",
           f"GRANT USE SCHEMA, EXTERNAL USE SCHEMA ON SCHEMA {cat}.{sch} TO `{sync_sp}`",
           f"GRANT SELECT, MODIFY ON TABLE {fq_target} TO `{sync_sp}`",
           f"GRANT SELECT, MODIFY ON TABLE {staging_name(fq_target)} TO `{sync_sp}`"]
    out += [f"GRANT SELECT ON TABLE {fq_target} TO `{r}`" for r in readers]
    return out
