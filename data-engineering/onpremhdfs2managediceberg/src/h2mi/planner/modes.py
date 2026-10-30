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


GIB = 1024 ** 3
METHODS = ("append_files", "merge", "row_value_replace", "hash_diff_replace", "partition_replace")


def choose_method(keys: list[str], cluster_by: list[str] | None, changed_rows: int, rows_in_touched: int,
                  replace_density_threshold: float = 0.25, *, source_kind: str = "iceberg",
                  files_added_only: bool = False, partition_bytes: int | None = None,
                  small_partition_bytes: int = 1 * GIB) -> str:
    """Apply method for a range / partition with row changes (§8.6 ladder, INC-18).

    Partition replace is *chosen*, never defaulted: replacing GBs because two rows changed is the expensive case.
      keyed    -> merge (always on clustered targets; on partitioned targets only when the change is sparse)
      keyless  -> Hive, only new files in the partition  -> append_files (file-delta, new rows only)
                  Iceberg, sparse                        -> row_value_replace (exact rows from the local snapshot-diff)
                  Hive rewritten, sparse                 -> hash_diff_replace (bucketed digests, then row-value replace)
      dense change (>= threshold) or a small partition   -> partition_replace
    On a CLUSTER BY target, partition_replace means the Databricks-side REPLACE WHERE (lab 2026-10-30), never an
    on-prem filter-overwrite; the executor routing (planner.executor) decides where it runs.
    """
    density = changed_rows / rows_in_touched if rows_in_touched else 1.0
    dense = density >= replace_density_threshold
    small = partition_bytes is not None and partition_bytes < small_partition_bytes
    if keys:
        if cluster_by:
            return "merge"
        return "partition_replace" if dense or small else "merge"
    if source_kind == "hive" and files_added_only:
        return "append_files"
    if dense or small:
        return "partition_replace"
    return "row_value_replace" if source_kind == "iceberg" else "hash_diff_replace"
