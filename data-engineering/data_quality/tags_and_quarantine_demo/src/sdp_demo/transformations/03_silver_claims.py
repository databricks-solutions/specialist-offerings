"""Silver layer for claims: same pattern as silver_policies."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from _rules_helper import get_expectations, failed_expectations_col
import _config as cfg

CLAIM_COLS = [
    "claim_id", "policy_id", "claim_date", "claim_amount",
    "peril_type", "description", "status",
]

expectations = get_expectations("silver_claims")
failed_col = failed_expectations_col(expectations)


def _read_bronze():
    return (
        spark.readStream
        .table(f"{cfg.CATALOG}.{cfg.SCHEMA}.bronze_claims")
        .select(*CLAIM_COLS)
    )


@dp.table(name="silver_claims")
@dp.expect_all_or_drop(expectations)
def silver_claims():
    return _read_bronze()


@dp.table(
    name="silver_claims_quarantine",
    comment="Rows that violated >=1 tag-derived expectation. "
            "failed_expectations lists the rule names that failed.",
)
def silver_claims_quarantine():
    return (
        _read_bronze()
        .withColumn("failed_expectations", failed_col)
        .where(F.size("failed_expectations") > 0)
        .withColumn("violation_count", F.size("failed_expectations"))
    )
