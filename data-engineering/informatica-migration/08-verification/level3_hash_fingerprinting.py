"""
VERIFICATION LEVEL 3: Hash-Based Fingerprinting
===============================================

WHAT THIS DOES:
- Generates cryptographic hash of each row's business columns
- Compares hashes between legacy and Databricks
- Detects even minor data differences (single character changes)

EFFORT: High (requires careful column mapping, more compute)
CONFIDENCE: Excellent (catches any data difference)

WHEN TO USE:
- Final certification before production cutover
- Critical tables requiring byte-for-byte validation
- When Levels 1 & 2 pass but stakeholders need extra confidence

IMPORTANT CONSIDERATIONS:
- Column order must match between systems
- Handle NULLs consistently (COALESCE to empty string)
- Normalize data types (trim strings, format numbers)
- Exclude audit/timestamp columns that differ by design
- Memory-intensive for large tables - sample if needed
"""

from pyspark.sql.functions import (
    md5, concat_ws, col, coalesce, lit, trim,
    round as spark_round, date_format, when
)
from pyspark.sql import DataFrame
from typing import List, Dict, Tuple

# =============================================================================
# Configuration
# =============================================================================

HASH_CONFIGS = {
    "sales": {
        "primary_key": ["sale_id"],
        "hash_columns": [
            "sale_id",
            "customer_id",
            "product_id",
            "store_id",
            "quantity",
            "amount",
            "discount",
            "sale_date"
        ],
        "legacy_table": "oracle_catalog.legacy.sales",
        "databricks_table": "gold_fact_sales",
        "date_column": "sale_date"
    },
    "customers": {
        "primary_key": ["customer_id"],
        "hash_columns": [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "address",
            "city",
            "state",
            "country"
        ],
        "legacy_table": "oracle_catalog.legacy.customers",
        "databricks_table": "gold_dim_customer",
        "date_column": None
    }
}


# =============================================================================
# Hash Generation Functions
# =============================================================================

def normalize_column(df: DataFrame, col_name: str) -> DataFrame:
    """
    Normalize column for consistent hashing across systems.
    Handles NULLs, trims strings, formats numbers/dates.
    """
    return df.withColumn(
        col_name,
        coalesce(
            when(col(col_name).cast("string").isNotNull(),
                 trim(col(col_name).cast("string"))),
            lit("")
        )
    )


def add_row_hash(df: DataFrame, hash_columns: List[str], key_columns: List[str]) -> DataFrame:
    """
    Add MD5 hash column based on business columns.
    Returns DataFrame with primary key(s) and row_hash.
    """
    # Normalize all hash columns
    for col_name in hash_columns:
        df = normalize_column(df, col_name)

    # Create hash from concatenated columns
    df = df.withColumn(
        "row_hash",
        md5(concat_ws("|", *[col(c) for c in hash_columns]))
    )

    # Return only key columns and hash
    return df.select(*key_columns, "row_hash")


def get_hashed_data(table: str, config: dict, business_date: str = None) -> DataFrame:
    """Load table and add row hash."""
    df = spark.table(table)

    # Apply date filter if specified
    if business_date and config.get("date_column"):
        df = df.where(f"{config['date_column']} = '{business_date}'")

    return add_row_hash(df, config["hash_columns"], config["primary_key"])


# =============================================================================
# Hash Comparison Functions
# =============================================================================

def compare_hashes(config: dict, business_date: str = None) -> Dict:
    """
    Compare row hashes between legacy and Databricks.
    Returns detailed mismatch information.
    """
    table_name = config.get("name", "unknown")
    print(f"Generating hashes for {table_name}...")

    # Get hashed data from both systems
    legacy_df = get_hashed_data(
        config["legacy_table"], config, business_date
    ).alias("legacy")

    databricks_df = get_hashed_data(
        config["databricks_table"], config, business_date
    ).alias("databricks")

    key_cols = config["primary_key"]

    # Count totals
    legacy_count = legacy_df.count()
    databricks_count = databricks_df.count()
    print(f"  Legacy rows: {legacy_count:,}")
    print(f"  Databricks rows: {databricks_count:,}")

    # Join on primary key to compare hashes
    join_condition = [col(f"legacy.{k}") == col(f"databricks.{k}") for k in key_cols]

    comparison = legacy_df.join(databricks_df, join_condition, "full_outer")

    # Find mismatches
    # 1. Only in legacy (not in Databricks)
    only_legacy = comparison.where(
        col("databricks.row_hash").isNull()
    ).select([col(f"legacy.{k}").alias(k) for k in key_cols])
    only_legacy_count = only_legacy.count()

    # 2. Only in Databricks (not in legacy)
    only_databricks = comparison.where(
        col("legacy.row_hash").isNull()
    ).select([col(f"databricks.{k}").alias(k) for k in key_cols])
    only_databricks_count = only_databricks.count()

    # 3. Hash mismatch (exists in both but different)
    hash_mismatch = comparison.where(
        col("legacy.row_hash").isNotNull() &
        col("databricks.row_hash").isNotNull() &
        (col("legacy.row_hash") != col("databricks.row_hash"))
    ).select([col(f"legacy.{k}").alias(k) for k in key_cols])
    hash_mismatch_count = hash_mismatch.count()

    # Calculate pass/fail
    total_issues = only_legacy_count + only_databricks_count + hash_mismatch_count
    status = "PASS" if total_issues == 0 else "FAIL"

    print(f"  Only in legacy: {only_legacy_count:,}")
    print(f"  Only in Databricks: {only_databricks_count:,}")
    print(f"  Hash mismatches: {hash_mismatch_count:,}")
    print(f"  Status: {status}")

    return {
        "table_name": table_name,
        "business_date": business_date,
        "legacy_count": legacy_count,
        "databricks_count": databricks_count,
        "only_in_legacy": only_legacy_count,
        "only_in_databricks": only_databricks_count,
        "hash_mismatches": hash_mismatch_count,
        "total_issues": total_issues,
        "status": status,
        # Sample mismatched keys for debugging
        "sample_only_legacy": only_legacy.limit(10).collect() if only_legacy_count > 0 else [],
        "sample_only_databricks": only_databricks.limit(10).collect() if only_databricks_count > 0 else [],
        "sample_hash_mismatch": hash_mismatch.limit(10).collect() if hash_mismatch_count > 0 else []
    }


# =============================================================================
# Debug Mismatches
# =============================================================================

def debug_hash_mismatch(config: dict, key_values: dict) -> None:
    """
    Show side-by-side comparison for a specific mismatched record.
    Helps identify which column(s) differ.
    """
    key_cols = config["primary_key"]
    hash_cols = config["hash_columns"]

    # Build WHERE clause from key values
    where_conditions = " AND ".join([f"{k} = '{v}'" for k, v in key_values.items()])

    print(f"\nDebug: Comparing record with {key_values}")
    print("=" * 80)

    # Get legacy record
    legacy = spark.table(config["legacy_table"]).where(where_conditions).select(hash_cols).collect()
    # Get Databricks record
    databricks = spark.table(config["databricks_table"]).where(where_conditions).select(hash_cols).collect()

    if not legacy:
        print("Record NOT FOUND in legacy")
        return
    if not databricks:
        print("Record NOT FOUND in Databricks")
        return

    legacy_row = legacy[0]
    databricks_row = databricks[0]

    # Compare column by column
    print(f"{'Column':<30} {'Legacy':<30} {'Databricks':<30} {'Match'}")
    print("-" * 100)

    for col_name in hash_cols:
        legacy_val = str(legacy_row[col_name]) if legacy_row[col_name] is not None else "NULL"
        db_val = str(databricks_row[col_name]) if databricks_row[col_name] is not None else "NULL"
        match = "✓" if legacy_val == db_val else "✗ DIFF"
        print(f"{col_name:<30} {legacy_val:<30} {db_val:<30} {match}")


# =============================================================================
# Run Hash Validation
# =============================================================================

def run_hash_validation(table_name: str, business_date: str = None):
    """Run hash validation for specified table."""
    if table_name not in HASH_CONFIGS:
        raise ValueError(f"Unknown table: {table_name}. Available: {list(HASH_CONFIGS.keys())}")

    config = HASH_CONFIGS[table_name]
    config["name"] = table_name

    result = compare_hashes(config, business_date)

    if result["status"] == "FAIL" and result["sample_hash_mismatch"]:
        print("\nSample mismatched records (first 10):")
        for row in result["sample_hash_mismatch"][:5]:
            print(f"  {dict(row.asDict())}")

    return result


# =============================================================================
# Example Usage
# =============================================================================

if __name__ == "__main__":
    # Run hash validation for sales on specific date
    result = run_hash_validation("sales", "2024-01-15")

    # If failures, debug specific record
    # if result["sample_hash_mismatch"]:
    #     key = {"sale_id": result["sample_hash_mismatch"][0]["sale_id"]}
    #     debug_hash_mismatch(HASH_CONFIGS["sales"], key)
