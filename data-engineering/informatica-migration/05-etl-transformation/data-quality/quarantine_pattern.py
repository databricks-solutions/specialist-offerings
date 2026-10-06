"""
Quarantine Pattern: Separate Valid and Invalid Records
======================================================

WHAT THIS DOES:
- Routes valid records to main Silver table
- Captures invalid records in quarantine table for review
- Enables data stewards to investigate and fix bad data

REPLACES IN INFORMATICA:
- Router transformation with reject port
- Error handling with reject files
- Bad record tables for DQ review

WHY USE THIS PATTERN:
- Don't lose data - bad records are captured, not discarded
- Enable root cause analysis of data quality issues
- Allow data stewards to fix and reprocess bad records
- Audit trail for compliance

PATTERN FLOW:
    Bronze ─┬─> Silver (valid)
            └─> Quarantine (invalid)

QUARANTINE TABLE USAGE:
1. Data steward reviews quarantine table daily
2. Investigates root cause (source system, mapping error, etc.)
3. Fixes data in source or applies corrections
4. Moves corrected records back to Bronze for reprocessing
"""

import dlt
from pyspark.sql.functions import current_timestamp, lit

# =============================================================================
# Quality rules as constants for consistency
# =============================================================================
VALID_TRANSACTION_RULES = """
    transaction_id IS NOT NULL
    AND amount BETWEEN 0.01 AND 999999.99
    AND transaction_date >= '2020-01-01'
    AND transaction_date <= CURRENT_DATE
    AND status IN ('COMPLETED', 'PENDING', 'FAILED', 'REVERSED')
"""

INVALID_TRANSACTION_RULES = f"NOT ({VALID_TRANSACTION_RULES})"


# =============================================================================
# Valid records → Silver table
# =============================================================================
@dlt.table(
    name="silver_transactions_valid",
    comment="Validated transactions that passed all quality checks"
)
@dlt.expect_or_drop("valid_transaction", VALID_TRANSACTION_RULES)
def silver_transactions_valid():
    return (
        dlt.read_stream("bronze_transactions")
        .select("*")
    )


# =============================================================================
# Invalid records → Quarantine table for review
# =============================================================================
@dlt.table(
    name="silver_transactions_quarantine",
    comment="Invalid transactions requiring data steward review"
)
def silver_transactions_quarantine():
    """
    Captures records that fail validation rules.
    Includes metadata for investigation.
    """
    return (
        dlt.read_stream("bronze_transactions")
        .where(INVALID_TRANSACTION_RULES)
        .withColumn("quarantine_timestamp", current_timestamp())
        .withColumn("quarantine_reason", lit("Failed validation rules"))
    )


# =============================================================================
# Alternative: More detailed quarantine with specific failure reasons
# =============================================================================
@dlt.table(name="silver_orders_quarantine_detailed")
def silver_orders_quarantine_detailed():
    """
    Quarantine with specific failure reasons for easier debugging.
    """
    from pyspark.sql.functions import when, array, array_compact

    df = dlt.read_stream("bronze_orders")

    # Check each rule and build array of failure reasons
    df_with_reasons = df.withColumn("failure_reasons",
        array_compact(array(
            when(col("order_id").isNull(), lit("Missing order_id")),
            when(col("amount") <= 0, lit("Invalid amount: <= 0")),
            when(col("amount") > 999999, lit("Invalid amount: > 999999")),
            when(col("order_date") < "2020-01-01", lit("Date too old")),
            when(col("status").isNull(), lit("Missing status")),
            when(col("customer_id").isNull(), lit("Missing customer_id"))
        ))
    )

    # Filter to only invalid records (has at least one failure reason)
    return (
        df_with_reasons
        .where("SIZE(failure_reasons) > 0")
        .withColumn("quarantine_timestamp", current_timestamp())
    )
