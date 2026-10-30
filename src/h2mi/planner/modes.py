"""Sync-mode resolution per cycle (design §8.4 decision table, INC-01, INC-02).

The mode is re-resolved for every cycle from the actual snapshot range. A configured mode is a ceiling, never a bypass
of the guards: asking for `iceberg-append` on a range with overwrites is BLOCKED, asking for `iceberg-mor` on an
append-only range runs the cheaper append path.
"""
from __future__ import annotations

from ..util.errors import BlockedError

APPEND_OPS = {"append", "replace"}
_RANK = {"iceberg-append": 0, "iceberg-upsert": 1, "iceberg-mor": 2}


def resolve_mode(source_kind: str, operations: list[str], has_live_delete_files: bool, keys: list[str],
                 configured: str = "auto") -> str:
    if source_kind == "hive":
        if configured not in ("auto", "hive-partition"):
            raise BlockedError(f"Hive sources use hive-partition, not {configured}")
        return "hive-partition"
    if configured == "hive-partition":
        raise BlockedError("hive-partition is only for Hive sources")
    if all(op in APPEND_OPS for op in operations):
        resolved = "iceberg-append"      # delete files only apply to files written before them (lab 2026-10-22)
    elif not has_live_delete_files and keys:
        resolved = "iceberg-upsert"
    else:
        resolved = "iceberg-mor"         # changelog rejects live delete files in every Iceberg release (INC-04)
    if configured != "auto" and _RANK[configured] < _RANK[resolved]:
        raise BlockedError(f"configured mode {configured} cannot handle this range (needs {resolved}): "
                           f"operations={sorted(set(operations))} live_delete_files={has_live_delete_files} "
                           f"keys={'yes' if keys else 'no'}")
    return resolved


def choose_method(keys: list[str], cluster_by: list[str] | None, changed_rows: int, rows_in_touched: int,
                  replace_density_threshold: float = 0.25) -> str:
    """For a range with row changes: `partition_replace` or `merge` (§8.6 rule). Keyless must replace, clustered must
    merge; keyed+partitioned picks replace when the changes are dense."""
    if not keys:
        if cluster_by:
            raise BlockedError("keyless source on a clustered target: needs keys or a PARTITIONED BY target (DDL-15)")
        return "partition_replace"
    if cluster_by:
        return "merge"
    density = changed_rows / rows_in_touched if rows_in_touched else 1.0
    return "partition_replace" if density >= replace_density_threshold else "merge"
