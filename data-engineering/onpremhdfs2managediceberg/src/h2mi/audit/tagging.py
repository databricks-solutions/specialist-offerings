"""Snapshot-summary tags on every append / overwrite / staging commit (AUD-03). MERGE commits carry none (lab), so they
are attributed through their staging commit plus the cycle record."""
from __future__ import annotations

TAG_KEYS = ("run_id", "batch_id", "cycle_id", "src_watermark", "onprem_principal", "version")


def commit_tags(**values: str) -> dict[str, str]:
    """{'snapshot-property.h2mi.<key>': value} for DataFrameWriterV2 .options(**commit_tags(...))."""
    unknown = set(values) - set(TAG_KEYS)
    if unknown:
        raise ValueError(f"unknown tag keys {sorted(unknown)}; allowed {TAG_KEYS}")
    return {f"snapshot-property.h2mi.{k}": str(v) for k, v in values.items() if v is not None}
