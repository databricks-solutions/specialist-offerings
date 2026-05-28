"""
VERIFICATION LEVEL 1: Automated Volumetric Checks
=================================================

WHAT THIS DOES:
- Automates row count comparison between legacy and Databricks
- Logs results to audit table for trend analysis
- Alerts on mismatches

EFFORT: Low (once set up, runs automatically)
CONFIDENCE: Basic (catches major issues)

USE AS:
- Scheduled daily job during migration
- Part of automated testing pipeline
- Quick validation before deeper checks
"""

from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime, timedelta
import uuid

# =============================================================================
# Configuration
# =============================================================================
LEGACY_CONNECTION = {
    "url": "jdbc:oracle:thin:@//oracle-host:1521/PRODDB",
    "user": dbutils.secrets.get("db-scope", "oracle-user"),
    "password": dbutils.secrets.get("db-scope", "oracle-password")
}

TABLE_MAPPINGS = [
    {
        "name": "sales",
        "legacy_table": "LEGACY_SCHEMA.SALES",
        "legacy_date_column": "SALE_DATE",
        "databricks_table": "gold_fact_sales",
        "databricks_date_column": "sale_date",
        "critical": True
    },
    {
        "name": "customers",
        "legacy_table": "LEGACY_SCHEMA.CUSTOMERS",
        "legacy_date_column": "LAST_UPDATED",
        "databricks_table": "gold_dim_customer",
        "databricks_date_column": "last_updated",
        "critical": True
    },
    {
        "name": "products",
        "legacy_table": "LEGACY_SCHEMA.PRODUCTS",
        "legacy_date_column": None,  # No date filter - full table
        "databricks_table": "gold_dim_product",
        "databricks_date_column": None,
        "critical": False
    }
]


# =============================================================================
# Volumetric Comparison Functions
# =============================================================================

def get_legacy_count(table_config: dict, business_date: str = None) -> int:
    """Get row count from legacy Oracle table."""
    table = table_config["legacy_table"]
    date_col = table_config["legacy_date_column"]

    if date_col and business_date:
        query = f"SELECT COUNT(*) as cnt FROM {table} WHERE {date_col} = TO_DATE('{business_date}', 'YYYY-MM-DD')"
    else:
        query = f"SELECT COUNT(*) as cnt FROM {table}"

    df = spark.read \
        .format("jdbc") \
        .option("url", LEGACY_CONNECTION["url"]) \
        .option("user", LEGACY_CONNECTION["user"]) \
        .option("password", LEGACY_CONNECTION["password"]) \
        .option("query", query) \
        .load()

    return df.collect()[0]["cnt"]


def get_databricks_count(table_config: dict, business_date: str = None) -> int:
    """Get row count from Databricks Gold table."""
    table = table_config["databricks_table"]
    date_col = table_config["databricks_date_column"]

    if date_col and business_date:
        query = f"SELECT COUNT(*) as cnt FROM {table} WHERE {date_col} = '{business_date}'"
    else:
        query = f"SELECT COUNT(*) as cnt FROM {table}"

    return spark.sql(query).collect()[0]["cnt"]


def compare_counts(table_config: dict, business_date: str = None) -> dict:
    """Compare row counts between legacy and Databricks."""
    try:
        legacy_count = get_legacy_count(table_config, business_date)
        databricks_count = get_databricks_count(table_config, business_date)

        difference = databricks_count - legacy_count
        pct_difference = (abs(difference) / legacy_count * 100) if legacy_count > 0 else 0

        status = "PASS" if difference == 0 else "FAIL"

        return {
            "table_name": table_config["name"],
            "business_date": business_date,
            "legacy_count": legacy_count,
            "databricks_count": databricks_count,
            "difference": difference,
            "pct_difference": round(pct_difference, 4),
            "status": status,
            "is_critical": table_config["critical"],
            "error_message": None
        }

    except Exception as e:
        return {
            "table_name": table_config["name"],
            "business_date": business_date,
            "legacy_count": None,
            "databricks_count": None,
            "difference": None,
            "pct_difference": None,
            "status": "ERROR",
            "is_critical": table_config["critical"],
            "error_message": str(e)
        }


# =============================================================================
# Run Validation Suite
# =============================================================================

def run_volumetric_validation(business_date: str = None):
    """
    Run volumetric validation for all configured tables.
    Returns summary and saves results to audit table.
    """
    run_id = str(uuid.uuid4())
    run_timestamp = datetime.now()
    results = []

    print(f"Starting volumetric validation - Run ID: {run_id}")
    print(f"Business Date: {business_date or 'Full Table'}")
    print("=" * 60)

    for table_config in TABLE_MAPPINGS:
        print(f"Validating: {table_config['name']}...", end=" ")

        result = compare_counts(table_config, business_date)
        result["run_id"] = run_id
        result["run_timestamp"] = run_timestamp
        result["validation_level"] = "volumetric"

        results.append(result)

        # Print inline result
        if result["status"] == "PASS":
            print(f"PASS ({result['databricks_count']:,} rows)")
        elif result["status"] == "ERROR":
            print(f"ERROR: {result['error_message']}")
        else:
            print(f"FAIL (Legacy: {result['legacy_count']:,}, "
                  f"Databricks: {result['databricks_count']:,}, "
                  f"Diff: {result['difference']:+,})")

    # Save results to audit table
    results_df = spark.createDataFrame(results)
    results_df.write.mode("append").saveAsTable("main.audit.reconciliation_results")

    # Summary
    print("=" * 60)
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    errors = sum(1 for r in results if r["status"] == "ERROR")

    print(f"SUMMARY: {passed} passed, {failed} failed, {errors} errors")

    # Alert on critical failures
    critical_failures = [r for r in results
                        if r["status"] in ("FAIL", "ERROR") and r["is_critical"]]

    if critical_failures:
        print("\n⚠️  CRITICAL FAILURES DETECTED:")
        for f in critical_failures:
            print(f"  - {f['table_name']}: {f['status']}")
        # send_alert(f"Volumetric validation failed: {len(critical_failures)} critical tables", critical_failures)
        raise Exception(f"Critical validation failures: {[f['table_name'] for f in critical_failures]}")

    return run_id


# =============================================================================
# Example Usage
# =============================================================================

if __name__ == "__main__":
    # Validate specific date
    # run_volumetric_validation("2024-01-15")

    # Validate yesterday
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    run_volumetric_validation(yesterday)

    # Full table validation (no date filter)
    # run_volumetric_validation(None)
