"""MERGE executor routing and the download budget (design §8.4, D11, INC-16, NFR-11).

An on-prem MERGE over IRC must read the target's candidate files down the DX link. Before each MERGE batch the planner
estimates that download from metadata only (`<tgt>.files` bounds vs the batch's key / locality range) and routes the
MERGE to Databricks unless the estimate is under budget.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class DownloadEstimate:
    candidate_files: int
    candidate_bytes: int
    expected_download_bytes: int


def estimate_download(files: Iterable[tuple[int, object, object]], lo, hi, key_fraction: float = 1.0) -> DownloadEstimate:
    """`files` = (file_size_in_bytes, lower_bound, upper_bound) of the routing column per target data file; bounds may be
    None (unknown -> candidate). `key_fraction` = share of a file's bytes an on-prem MoR MERGE has to read (key columns
    + positions), 1.0 for copy-on-write targets."""
    n = total = 0
    for size, lower, upper in files:
        if lower is not None and upper is not None and (upper < lo or lower > hi):
            continue
        n, total = n + 1, total + int(size)
    return DownloadEstimate(n, total, int(total * key_fraction))


def choose_executor(est: DownloadEstimate, configured: str = "auto", budget_bytes: int = 50 * 1024 ** 3) -> str:
    """`databricks` unless the caller allows on-prem AND the estimate is under budget."""
    if configured == "databricks":
        return "databricks"
    return "onprem" if est.expected_download_bytes <= budget_bytes else "databricks"
