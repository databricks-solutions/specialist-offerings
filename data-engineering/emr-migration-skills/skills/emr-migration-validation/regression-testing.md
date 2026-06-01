# Regression Testing Strategy for EMR to Databricks Migration

## Overview

Regression testing ensures that migrated workloads produce identical results on Databricks as they did on EMR. This goes beyond data validation (row counts, checksums) to cover business logic correctness, edge cases, schema evolution, and downstream consumer compatibility.

## Test Strategy Framework

### 1. Define Test Cases

Each migrated workload needs a set of test cases with:
- **Test name**: Descriptive identifier
- **Input data**: Specific dataset or data conditions
- **Expected output**: Known-good result from EMR
- **Comparison logic**: How to determine pass/fail
- **Tolerance**: Acceptable differences (e.g., floating point precision)

### Test Case Template

```python
test_cases = [
    {
        "name": "daily_aggregation_standard",
        "description": "Standard daily aggregation with typical data volume",
        "input_table": "catalog.schema.raw_events",
        "input_filter": "dt = '2024-01-15'",
        "expected_output_table": "catalog.schema.emr_baseline_daily_agg_20240115",
        "actual_output_table": "catalog.schema.dbx_daily_agg_20240115",
        "comparison": "full_match",
        "key_columns": ["user_id", "dt"],
        "compare_columns": ["event_count", "total_amount"],
        "tolerance": {"total_amount": 0.01},  # Float tolerance
    },
    {
        "name": "daily_aggregation_empty",
        "description": "Edge case: no data for the given date",
        "input_table": "catalog.schema.raw_events",
        "input_filter": "dt = '2099-01-01'",
        "expected_row_count": 0,
        "comparison": "row_count",
    },
    {
        "name": "daily_aggregation_nulls",
        "description": "Edge case: null values in key columns",
        "input_table": "catalog.schema.raw_events_with_nulls",
        "expected_output_table": "catalog.schema.emr_baseline_nulls",
        "actual_output_table": "catalog.schema.dbx_nulls",
        "comparison": "full_match",
        "key_columns": ["user_id", "dt"],
        "compare_columns": ["event_count", "total_amount"],
    },
]
```

### 2. Automated Comparison Pipeline

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Optional
import json

class MigrationRegressionTester:
    """Automated regression testing for EMR to Databricks migration."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.results = []

    def compare_dataframes(
        self,
        expected: DataFrame,
        actual: DataFrame,
        key_columns: List[str],
        compare_columns: List[str],
        tolerance: Optional[Dict[str, float]] = None,
    ) -> Dict:
        """Compare two DataFrames and return detailed differences."""

        result = {
            "row_count_match": expected.count() == actual.count(),
            "expected_count": expected.count(),
            "actual_count": actual.count(),
            "schema_match": True,
            "mismatched_rows": 0,
            "missing_in_actual": 0,
            "extra_in_actual": 0,
            "column_mismatches": {},
        }

        # Schema comparison
        for col in compare_columns:
            if col not in [f.name for f in actual.schema.fields]:
                result["schema_match"] = False
                result["column_mismatches"][col] = "missing in actual"

        if not result["schema_match"]:
            return result

        # Join on key columns
        joined = expected.alias("exp").join(
            actual.alias("act"),
            on=key_columns,
            how="full_outer",
        )

        # Find missing rows
        missing_in_actual = joined.filter(
            F.col(f"act.{compare_columns[0]}").isNull()
            & F.col(f"exp.{compare_columns[0]}").isNotNull()
        )
        result["missing_in_actual"] = missing_in_actual.count()

        extra_in_actual = joined.filter(
            F.col(f"exp.{compare_columns[0]}").isNull()
            & F.col(f"act.{compare_columns[0]}").isNotNull()
        )
        result["extra_in_actual"] = extra_in_actual.count()

        # Compare values for matching rows
        matched = joined.filter(
            F.col(f"exp.{compare_columns[0]}").isNotNull()
            & F.col(f"act.{compare_columns[0]}").isNotNull()
        )

        for col in compare_columns:
            tol = (tolerance or {}).get(col, 0)
            if tol > 0:
                mismatch_condition = F.abs(
                    F.col(f"exp.{col}") - F.col(f"act.{col}")
                ) > tol
            else:
                mismatch_condition = F.col(f"exp.{col}") != F.col(f"act.{col}")

            # Handle nulls
            null_mismatch = (
                F.col(f"exp.{col}").isNull() != F.col(f"act.{col}").isNull()
            )

            mismatches = matched.filter(mismatch_condition | null_mismatch).count()
            if mismatches > 0:
                result["column_mismatches"][col] = mismatches
                result["mismatched_rows"] += mismatches

        return result

    def run_test_case(self, test_case: Dict) -> Dict:
        """Run a single test case and return results."""
        name = test_case["name"]
        print(f"Running test: {name}")

        try:
            if test_case["comparison"] == "row_count":
                actual = self.spark.table(test_case.get("actual_output_table", ""))
                if "input_filter" in test_case:
                    actual = actual.filter(test_case["input_filter"])
                actual_count = actual.count()
                passed = actual_count == test_case["expected_row_count"]
                return {
                    "name": name,
                    "passed": passed,
                    "details": {
                        "expected_count": test_case["expected_row_count"],
                        "actual_count": actual_count,
                    },
                }

            elif test_case["comparison"] == "full_match":
                expected = self.spark.table(test_case["expected_output_table"])
                actual = self.spark.table(test_case["actual_output_table"])

                comparison = self.compare_dataframes(
                    expected=expected,
                    actual=actual,
                    key_columns=test_case["key_columns"],
                    compare_columns=test_case["compare_columns"],
                    tolerance=test_case.get("tolerance"),
                )

                passed = (
                    comparison["row_count_match"]
                    and comparison["schema_match"]
                    and comparison["mismatched_rows"] == 0
                    and comparison["missing_in_actual"] == 0
                    and comparison["extra_in_actual"] == 0
                )

                return {
                    "name": name,
                    "passed": passed,
                    "details": comparison,
                }

        except Exception as e:
            return {
                "name": name,
                "passed": False,
                "details": {"error": str(e)},
            }

    def run_all_tests(self, test_cases: List[Dict]) -> Dict:
        """Run all test cases and return summary."""
        results = []
        for tc in test_cases:
            result = self.run_test_case(tc)
            results.append(result)
            status = "PASS" if result["passed"] else "FAIL"
            print(f"  {status}: {result['name']}")

        passed = sum(1 for r in results if r["passed"])
        failed = sum(1 for r in results if not r["passed"])

        summary = {
            "total": len(results),
            "passed": passed,
            "failed": failed,
            "pass_rate": f"{passed/len(results)*100:.1f}%" if results else "N/A",
            "results": results,
        }

        print(f"\nSummary: {passed}/{len(results)} passed ({summary['pass_rate']})")
        return summary
```

### Usage

```python
# In a Databricks notebook
tester = MigrationRegressionTester(spark)
summary = tester.run_all_tests(test_cases)

# Save results for tracking
spark.createDataFrame([summary]).write.mode("append").saveAsTable(
    "catalog.schema.migration_test_results"
)
```

## Edge Cases to Test

### 1. Empty Data

```python
{
    "name": "empty_input",
    "description": "No input data — should produce empty output",
    "input_filter": "1 = 0",  # No rows
    "expected_row_count": 0,
    "comparison": "row_count",
}
```

### 2. Schema Evolution

```python
{
    "name": "schema_evolution_new_column",
    "description": "Input has a new column not in original schema",
    "input_table": "catalog.schema.events_with_new_column",
    "expected_behavior": "ignore_new_column",  # Or fail, depending on requirements
}
```

Test that the job handles:
- New columns added to input (should be ignored or handled gracefully)
- Columns removed from input (should fail clearly or use defaults)
- Column type changes (string to int, etc.)

### 3. Late-Arriving Data

```python
{
    "name": "late_data",
    "description": "Data arrives after the processing window",
    "setup": "Insert rows with timestamp before processing window after job runs",
    "expected_behavior": "Next run picks up late data",
}
```

### 4. Null and Special Value Handling

```python
null_test_cases = [
    {"name": "null_key_column", "description": "NULL in join key"},
    {"name": "null_value_column", "description": "NULL in aggregation column"},
    {"name": "empty_string", "description": "Empty string vs NULL"},
    {"name": "special_chars", "description": "Unicode, newlines, tabs in string columns"},
    {"name": "max_values", "description": "INT_MAX, LONG_MAX, very large decimals"},
    {"name": "min_values", "description": "Negative numbers, zero, very small decimals"},
    {"name": "nan_infinity", "description": "NaN and Infinity in float/double columns"},
]
```

### 5. Timezone Handling

```python
{
    "name": "timezone_handling",
    "description": "Timestamps across timezone boundaries",
    "notes": [
        "EMR default timezone may differ from Databricks",
        "Check spark.sql.session.timeZone on both platforms",
        "Test DST boundaries (March/November transitions)",
        "Test UTC vs local time conversions",
    ],
}
```

**Common issue**: EMR clusters may default to UTC while Databricks uses the configured timezone. Verify:

```python
# Check timezone setting
print(spark.conf.get("spark.sql.session.timeZone"))

# Set explicitly if needed
spark.conf.set("spark.sql.session.timeZone", "UTC")
```

### 6. Duplicate Data

```python
{
    "name": "duplicate_input_rows",
    "description": "Duplicate rows in input data",
    "expected_behavior": "Same dedup logic as EMR",
}
```

### 7. Concurrent Writes

```python
{
    "name": "concurrent_write",
    "description": "Two jobs writing to the same table simultaneously",
    "notes": "Delta Lake handles this with ACID; Parquet on EMR may not",
}
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Migration Regression Tests
on:
  push:
    branches: [main]
  schedule:
    - cron: '0 8 * * *'  # Daily at 8 AM

jobs:
  regression-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Run regression tests
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          pip install databricks-sdk
          databricks jobs run-now --job-id ${{ vars.REGRESSION_TEST_JOB_ID }} --timeout 3600

      - name: Check results
        run: |
          # Query test results from system tables
          databricks sql execute --query "
            SELECT passed, failed, pass_rate
            FROM catalog.schema.migration_test_results
            ORDER BY run_timestamp DESC LIMIT 1
          "
```

### Databricks Workflow for Regression Tests

```yaml
resources:
  jobs:
    migration_regression_tests:
      name: "Migration Regression Tests"
      schedule:
        quartz_cron_expression: "0 0 8 * * ?"
        timezone_id: "UTC"

      tasks:
        - task_key: run_emr_baseline
          description: "Run EMR workload and capture baseline (if still available)"
          spark_python_task:
            python_file: /Workspace/Repos/migration/run_emr_baseline.py

        - task_key: run_databricks_workload
          depends_on:
            - task_key: run_emr_baseline
          spark_python_task:
            python_file: /Workspace/Repos/migration/run_databricks_workload.py

        - task_key: compare_results
          depends_on:
            - task_key: run_databricks_workload
          spark_python_task:
            python_file: /Workspace/Repos/migration/compare_results.py

        - task_key: report
          depends_on:
            - task_key: compare_results
          run_if: "ALL_DONE"
          spark_python_task:
            python_file: /Workspace/Repos/migration/generate_report.py

      email_notifications:
        on_failure:
          - "migration-team@company.com"
```

## End-to-End Validation with Downstream Consumers

### Step 1: Identify Downstream Consumers

```sql
-- Use Unity Catalog lineage to find downstream tables
SELECT
  target_table_full_name,
  target_type
FROM system.access.table_lineage
WHERE source_table_full_name = 'catalog.schema.migrated_table'
ORDER BY target_table_full_name;
```

### Step 2: Shadow Mode Testing

Run both EMR and Databricks in parallel, writing to separate output locations. Downstream consumers read from EMR while you compare outputs.

```
                    ┌─────────────┐
                    │ Input Data  │
                    └──────┬──────┘
                    ┌──────┴──────┐
              ┌─────┤  Duplicated ├─────┐
              │     └─────────────┘     │
        ┌─────┴─────┐            ┌─────┴─────┐
        │ EMR Job   │            │ DBX Job   │
        └─────┬─────┘            └─────┬─────┘
        ┌─────┴─────┐            ┌─────┴─────┐
        │ EMR Output│            │ DBX Output│
        │ (primary) │            │ (shadow)  │
        └─────┬─────┘            └─────┬─────┘
              │                        │
        ┌─────┴──────┐          ┌─────┴──────┐
        │ Downstream │          │ Compare    │
        │ Consumers  │          │ Script     │
        └────────────┘          └────────────┘
```

### Step 3: Cutover Validation

Before switching downstream consumers to Databricks output:

```python
# Final validation checklist
final_checks = {
    "data_matches": "3 consecutive runs with identical output",
    "performance_acceptable": "Within 2x of EMR baseline",
    "downstream_tested": "All downstream consumers validated with DBX output",
    "error_handling": "Bad records handled identically",
    "monitoring_ready": "Alerts and dashboards configured",
    "rollback_plan": "Can switch back to EMR within 1 hour",
}
```

## Test Results Dashboard

Create a SQL dashboard to track regression test results over time:

```sql
-- Test pass rate over time
SELECT
  DATE(run_timestamp) AS run_date,
  SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS tests_passed,
  SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS tests_failed,
  ROUND(
    SUM(CASE WHEN passed THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
  ) AS pass_rate_pct
FROM catalog.schema.migration_test_results_detail
GROUP BY DATE(run_timestamp)
ORDER BY run_date DESC;

-- Failed tests detail
SELECT
  test_name,
  run_timestamp,
  error_details,
  expected_value,
  actual_value
FROM catalog.schema.migration_test_results_detail
WHERE NOT passed
  AND run_timestamp > CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY run_timestamp DESC;
```
