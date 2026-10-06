# Level 2: Automated Aggregate Comparison with Tolerance

def compare_aggregates(legacy_df, databricks_df, tolerance=0.01):
    """
    Compare aggregate metrics between legacy and Databricks with tolerance
    tolerance: percentage difference allowed (0.01 = 1%)
    """
    results = []

    for col in ["total_sales", "avg_sales", "min_sales"]:
        legacy_val = legacy_df[col]
        databricks_val = databricks_df[col]
        diff = abs(legacy_val - databricks_val)
        pct_diff = (diff / legacy_val * 100) if legacy_val != 0 else 0

        status = "PASS" if pct_diff <= tolerance else "FAIL"

        results.append({
            "metric": col,
            "legacy_value": legacy_val,
            "databricks_value": databricks_val,
            "difference": diff,
            "pct_difference": pct_diff,
            "status": status
        })

    return results
