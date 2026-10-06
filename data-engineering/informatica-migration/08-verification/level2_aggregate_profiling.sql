-- Level 2: Aggregate Profiling
-- Compare key business metrics (SUM, AVG, MIN, MAX)
-- Effort: Medium | Confidence: Good

-- Legacy System
SELECT
  BUSINESS_DATE,
  COUNT(*) as row_count,
  SUM(SALES_AMOUNT) as total_sales,
  AVG(SALES_AMOUNT) as avg_sales,
  MAX(TRANSACTION_ID) as max_txn_id,
  MIN(SALES_AMOUNT) as min_sales
FROM LEGACY_SALES
WHERE BUSINESS_DATE = '2024-01-15'
GROUP BY BUSINESS_DATE;

-- Databricks System
SELECT
  sale_date,
  COUNT(*) as row_count,
  SUM(amount) as total_sales,
  AVG(amount) as avg_sales,
  MAX(sale_id) as max_txn_id,
  MIN(amount) as min_sales
FROM gold_daily_sales
WHERE sale_date = '2024-01-15'
GROUP BY sale_date;

-- Compare with tolerance for floating-point precision
-- e.g., ROUND(legacy_total, 2) = ROUND(databricks_total, 2)
