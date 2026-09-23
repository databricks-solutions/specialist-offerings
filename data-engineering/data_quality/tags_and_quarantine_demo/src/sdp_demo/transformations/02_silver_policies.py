"""Silver layer for policies: clean stream (drops bad rows) +
parallel quarantine stream annotated with the rule(s) each row violated."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from _rules_helper import get_expectations, failed_expectations_col
import _config as cfg

POL_COLS = [
    "policy_id", "holder_name", "holder_email", "property_address",
    "property_zip", "coverage_type", "premium_amount",
    "effective_date", "expiration_date", "status",
]

expectations = get_expectations("silver_policies")
failed_col = failed_expectations_col(expectations)


def _read_bronze():
    return (
        spark.readStream
        .table(f"{cfg.CATALOG}.{cfg.SCHEMA}.bronze_policies")
        .select(*POL_COLS)
    )


@dp.table(name="silver_policies")
@dp.expect_all_or_drop(expectations)
def silver_policies():
    return _read_bronze()


@dp.table(
    name="silver_policies_quarantine",
    comment="Rows that violated >=1 tag-derived expectation. "
            "failed_expectations lists the rule names that failed.",
)
def silver_policies_quarantine():
    return (
        _read_bronze()
        .withColumn("failed_expectations", failed_col)
        .where(F.size("failed_expectations") > 0)
        .withColumn("violation_count", F.size("failed_expectations"))
    )
