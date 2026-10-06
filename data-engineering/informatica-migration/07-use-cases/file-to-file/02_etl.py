"""
USE CASE 1: File → ETL → File (Legacy Integration)
==================================================
STEP 2: ETL TRANSFORMATION (DLT Pipeline)

SCENARIO:
Transform Bronze mainframe transactions to Silver (validated) and Gold (export-ready).

INFORMATICA PATTERN (OLD):
- Expression transformation for business logic
- Filter transformation for validation
- Aggregator for any grouping needed

DATABRICKS PATTERN (NEW):
- DLT with expectations for validation
- Silver table for cleaned data
- Gold table ready for export

THIS FILE: Phase 2 - DLT Pipeline Definition
Deploy this as a DLT pipeline in Databricks.
"""

import dlt
from pyspark.sql.functions import col, date_format, when, lit, current_timestamp

# =============================================================================
# SILVER: Validated and Cleaned Transactions
# =============================================================================
@dlt.table(
    name="silver_finance_transactions",
    comment="Validated mainframe transactions for finance"
)
@dlt.expect_or_drop("valid_txn_id", "txn_id IS NOT NULL AND LENGTH(txn_id) > 0")
@dlt.expect_or_drop("valid_amount", "amount > 0 AND amount < 10000000")
@dlt.expect_or_drop("valid_date", "txn_date >= '2020-01-01' AND txn_date <= CURRENT_DATE")
@dlt.expect_or_drop("valid_account", "account_id IS NOT NULL")
@dlt.expect("valid_cost_center", "cost_center IS NOT NULL")  # Warn only
def silver_finance_transactions():
    """
    Transforms Bronze mainframe data:
    - Validates required fields
    - Formats dates for SAP compatibility
    - Standardizes amount precision
    """
    return (
        dlt.read_stream("bronze_mainframe_transactions")
        .select(
            col("txn_id"),
            col("amount").cast("decimal(15,2)"),
            col("txn_date"),
            # SAP expects YYYY-MM-DD format
            date_format(col("txn_date"), "yyyy-MM-dd").alias("txn_date_str"),
            col("account_id"),
            col("cost_center"),
            # Add processing timestamp
            current_timestamp().alias("processed_timestamp")
        )
    )


# =============================================================================
# GOLD: Export-Ready Data for SAP
# =============================================================================
@dlt.table(
    name="gold_finance_export",
    comment="Finance data ready for SAP upload"
)
def gold_finance_export():
    """
    Prepares final export format:
    - Selects only columns needed by SAP
    - Formats all fields as strings for CSV compatibility
    - Orders columns per SAP specification
    """
    return (
        dlt.read("silver_finance_transactions")
        .select(
            # SAP column order
            col("txn_id").alias("TRANSACTION_ID"),
            col("amount").cast("string").alias("AMOUNT"),
            col("txn_date_str").alias("POSTING_DATE"),
            col("account_id").alias("GL_ACCOUNT"),
            col("cost_center").alias("COST_CENTER")
        )
    )


# =============================================================================
# Optional: Quarantine Invalid Records
# =============================================================================
@dlt.table(
    name="silver_finance_quarantine",
    comment="Invalid transactions requiring review"
)
def silver_finance_quarantine():
    """
    Capture invalid records for data quality review.
    """
    return (
        dlt.read_stream("bronze_mainframe_transactions")
        .where("""
            txn_id IS NULL
            OR LENGTH(txn_id) = 0
            OR amount <= 0
            OR amount >= 10000000
            OR txn_date < '2020-01-01'
            OR txn_date > CURRENT_DATE
            OR account_id IS NULL
        """)
        .withColumn("quarantine_timestamp", current_timestamp())
        .withColumn("quarantine_reason", lit("Failed validation rules"))
    )
