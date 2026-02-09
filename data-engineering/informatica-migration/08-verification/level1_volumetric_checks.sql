-- =============================================================================
-- VERIFICATION LEVEL 1: Volumetric Checks (Row Counts)
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Compares row counts between legacy and Databricks systems
-- - Simplest validation - catches major data loss issues
-- - Should be first check during migration
--
-- EFFORT: Low
-- CONFIDENCE: Basic (catches major issues, not subtle data problems)
--
-- WHEN TO USE:
-- - Initial validation after each migration batch
-- - Daily reconciliation during parallel run
-- - Quick smoke test before deeper validation
--
-- LIMITATIONS:
-- - Won't detect incorrect values (same count, wrong data)
-- - Won't detect duplicate handling differences
-- - Need aggregate checks (Level 2) for business metrics
-- =============================================================================


-- =============================================================================
-- Step 1: Query Legacy System (Oracle/SQL Server via Informatica)
-- =============================================================================
-- Run this on legacy system or via Lakehouse Federation

-- Daily sales count
SELECT
  DATE(sale_date) as business_date,
  COUNT(*) as row_count
FROM LEGACY_SALES_TABLE
WHERE sale_date >= '2024-01-01'
GROUP BY DATE(sale_date)
ORDER BY business_date;

-- Expected output example:
-- | business_date | row_count |
-- |---------------|-----------|
-- | 2024-01-15    | 1,234,567 |
-- | 2024-01-16    | 1,198,432 |


-- =============================================================================
-- Step 2: Query Databricks Gold Table
-- =============================================================================
SELECT
  sale_date as business_date,
  COUNT(*) as row_count
FROM gold_fact_sales
WHERE sale_date >= '2024-01-01'
GROUP BY sale_date
ORDER BY business_date;


-- =============================================================================
-- Step 3: Compare Side-by-Side (Using Federation)
-- =============================================================================
-- If Lakehouse Federation is configured, compare in single query

WITH legacy_counts AS (
  SELECT
    DATE(sale_date) as business_date,
    COUNT(*) as legacy_count
  FROM oracle_catalog.legacy_schema.sales
  WHERE sale_date >= '2024-01-01'
  GROUP BY DATE(sale_date)
),
databricks_counts AS (
  SELECT
    sale_date as business_date,
    COUNT(*) as databricks_count
  FROM gold_fact_sales
  WHERE sale_date >= '2024-01-01'
  GROUP BY sale_date
)
SELECT
  COALESCE(l.business_date, d.business_date) as business_date,
  COALESCE(l.legacy_count, 0) as legacy_count,
  COALESCE(d.databricks_count, 0) as databricks_count,
  COALESCE(d.databricks_count, 0) - COALESCE(l.legacy_count, 0) as difference,
  CASE
    WHEN l.legacy_count = d.databricks_count THEN 'PASS'
    WHEN l.legacy_count IS NULL THEN 'ONLY_IN_DATABRICKS'
    WHEN d.databricks_count IS NULL THEN 'ONLY_IN_LEGACY'
    ELSE 'MISMATCH'
  END as status
FROM legacy_counts l
FULL OUTER JOIN databricks_counts d
  ON l.business_date = d.business_date
ORDER BY business_date;


-- =============================================================================
-- Step 4: Summary Validation Report
-- =============================================================================
WITH comparison AS (
  -- (use query from Step 3)
  SELECT
    COALESCE(l.business_date, d.business_date) as business_date,
    COALESCE(l.legacy_count, 0) as legacy_count,
    COALESCE(d.databricks_count, 0) as databricks_count,
    ABS(COALESCE(d.databricks_count, 0) - COALESCE(l.legacy_count, 0)) as abs_difference
  FROM legacy_counts l
  FULL OUTER JOIN databricks_counts d ON l.business_date = d.business_date
)
SELECT
  COUNT(*) as total_dates_checked,
  SUM(CASE WHEN abs_difference = 0 THEN 1 ELSE 0 END) as dates_matching,
  SUM(CASE WHEN abs_difference > 0 THEN 1 ELSE 0 END) as dates_with_difference,
  SUM(legacy_count) as total_legacy_rows,
  SUM(databricks_count) as total_databricks_rows,
  SUM(databricks_count) - SUM(legacy_count) as total_difference,
  CASE
    WHEN SUM(legacy_count) = SUM(databricks_count) THEN 'PASS'
    ELSE 'FAIL'
  END as overall_status
FROM comparison;
