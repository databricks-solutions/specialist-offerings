-- Reconciliation Audit Table
-- Stores all validation results for trend analysis

CREATE TABLE IF NOT EXISTS main.audit.reconciliation_results (
  run_id STRING,
  run_timestamp TIMESTAMP,
  table_name STRING,
  business_date DATE,
  validation_level STRING,  -- 'volumetric', 'aggregate', 'hash'
  metric_name STRING,
  legacy_value DOUBLE,
  databricks_value DOUBLE,
  difference DOUBLE,
  pct_difference DOUBLE,
  status STRING,  -- 'PASS', 'FAIL', 'WARNING'
  details STRING
);

-- Query validation trends over time
SELECT
  business_date,
  table_name,
  validation_level,
  COUNT(*) as total_checks,
  SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
  SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed,
  AVG(pct_difference) as avg_pct_diff
FROM main.audit.reconciliation_results
WHERE run_timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY business_date, table_name, validation_level
ORDER BY business_date DESC, table_name;
