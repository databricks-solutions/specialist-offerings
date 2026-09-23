"""Bronze ingestion. Auto Loader -> bronze_policies / bronze_claims.

Bronze gets a minimal tag-derived expectation set (just PK not_null) to
demonstrate that the pattern works at the raw layer too.
"""

from pyspark import pipelines as dp

from _rules_helper import get_expectations
import _config as cfg


def _make_bronze(name: str, source_dir: str, expectations: dict):
    schema_loc = f"{cfg.VOLUME_BASE}/_schemas/{name}"
    src_path = f"{cfg.VOLUME_BASE}/{source_dir}/"

    if expectations:
        @dp.table(name=name)
        @dp.expect_all_or_drop(expectations)
        def _t():
            return (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", schema_loc)
                .option("cloudFiles.inferColumnTypes", "true")
                .load(src_path)
            )
    else:
        @dp.table(name=name)
        def _t():
            return (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", schema_loc)
                .option("cloudFiles.inferColumnTypes", "true")
                .load(src_path)
            )

    return _t


bp_exp = get_expectations("bronze_policies")
_make_bronze("bronze_policies", "policies", bp_exp)

bc_exp = get_expectations("bronze_claims")
_make_bronze("bronze_claims", "claims", bc_exp)
