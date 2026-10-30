import pytest

from h2mi.planner.batches import cap_batch_bytes, pack
from h2mi.planner.executor import DownloadEstimate, choose_executor, estimate_download
from h2mi.planner.modes import choose_method, resolve_mode
from h2mi.util.errors import BlockedError

GIB = 1024 ** 3


def test_mode_table():
    assert resolve_mode("hive", [], False, []) == "hive-partition"
    assert resolve_mode("iceberg", ["append", "replace"], True, ["id"]) == "iceberg-append"   # MoR, insert-only range
    assert resolve_mode("iceberg", ["append", "overwrite"], False, ["id"]) == "iceberg-upsert"
    assert resolve_mode("iceberg", ["append", "overwrite"], True, ["id"]) == "iceberg-mor"
    assert resolve_mode("iceberg", ["delete"], False, []) == "iceberg-mor"                    # keyless


def test_configured_mode_is_a_ceiling():
    assert resolve_mode("iceberg", ["append"], False, ["id"], configured="iceberg-mor") == "iceberg-append"
    with pytest.raises(BlockedError, match="needs iceberg-upsert"):
        resolve_mode("iceberg", ["append", "overwrite"], False, ["id"], configured="iceberg-append")
    with pytest.raises(BlockedError):
        resolve_mode("iceberg", ["append"], False, [], configured="hive-partition")


def test_choose_method_keyed():
    assert choose_method(["k"], ["dt", "k"], 90, 100) == "merge"            # clustered: always merge
    assert choose_method(["k"], None, 30, 100) == "partition_replace"       # dense -> replace
    assert choose_method(["k"], None, 1, 100) == "merge"                    # sparse -> merge
    assert choose_method(["k"], None, 1, 100, partition_bytes=10 * 1024 ** 2) == "partition_replace"   # tiny partition


def test_choose_method_keyless_ladder():
    big = 50 * GIB
    # 2 changed rows in a 50 GiB partition: never a whole-partition rewrite
    assert choose_method([], None, 2, 10_000_000, source_kind="iceberg", partition_bytes=big) == "row_value_replace"
    assert choose_method([], None, 2, 10_000_000, source_kind="hive", partition_bytes=big) == "hash_diff_replace"
    assert choose_method([], None, 2, 10_000_000, source_kind="hive", files_added_only=True,
                         partition_bytes=big) == "append_files"
    # dense change or a small partition: replace is the cheap and simple choice
    assert choose_method([], None, 5_000_000, 10_000_000, source_kind="hive", partition_bytes=big) == "partition_replace"
    assert choose_method([], None, 2, 10_000_000, source_kind="hive", partition_bytes=200 * 1024 ** 2) == "partition_replace"
    # clustered keyless target: the replace is the Databricks REPLACE WHERE, no longer blocked
    assert choose_method([], ["k"], 9, 10, source_kind="hive") == "partition_replace"


def test_pack_and_cap():
    batches = pack([("p1", 3), ("p2", 5), ("p3", 4), ("p4", 9), ("p5", 1)], target_bytes=8)
    assert all(sum({"p1": 3, "p2": 5, "p3": 4, "p4": 9, "p5": 1}[p] for p in b) <= 9 for b in batches)
    assert ["p4"] in batches and len(batches) == 3
    assert pack([], 8) == []
    assert cap_batch_bytes(256 * GIB, None) == 256 * GIB
    assert cap_batch_bytes(256 * GIB, 100 * 1024 ** 2) == 100 * 1024 ** 2 * 40 * 60   # 100 MiB/s x 40 min


def test_download_estimate_and_routing():
    files = [(10 * GIB, "2026-01-01", "2026-01-31"), (10 * GIB, "2026-02-01", "2026-02-28"),
             (10 * GIB, None, None), (10 * GIB, "2026-03-01", "2026-03-31")]
    est = estimate_download(files, "2026-02-10", "2026-02-20", key_fraction=0.1)
    assert (est.candidate_files, est.candidate_bytes, est.expected_download_bytes) == (2, 20 * GIB, 2 * GIB)
    assert choose_executor(est, "auto", budget_bytes=50 * GIB) == "onprem"
    assert choose_executor(est, "auto", budget_bytes=1 * GIB) == "databricks"
    assert choose_executor(est, "databricks", budget_bytes=50 * GIB) == "databricks"
    assert choose_executor(DownloadEstimate(0, 0, 0), "onprem") == "onprem"
