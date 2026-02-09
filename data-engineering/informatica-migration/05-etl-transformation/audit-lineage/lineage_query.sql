-- =============================================================================
-- Unity Catalog Lineage: Automatic Data Flow Tracking
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Provides automatic lineage tracking without any custom code
-- - Shows data flow between tables at table and column level
-- - Tracks transformations across workspaces and catalogs
--
-- REPLACES IN INFORMATICA:
-- - Repository queries for metadata
-- - Manual lineage documentation
-- - Impact analysis reports
--
-- LINEAGE CAPABILITIES:
-- - Table-level: Which tables feed into which tables
-- - Column-level: Which columns derive from which source columns
-- - Cross-workspace: Lineage spans multiple Databricks workspaces
-- - Historical: View lineage at different points in time
--
-- ACCESS METHODS:
-- 1. UI: Catalog Explorer → Table → Lineage tab
-- 2. API: REST API for programmatic access
-- 3. SQL: System tables (limited, expanding)
--
-- LINEAGE IS AUTOMATICALLY CAPTURED FOR:
-- - DLT pipelines
-- - Spark SQL queries
-- - Notebooks (when writing to tables)
-- - JDBC/ODBC queries
-- =============================================================================

-- =============================================================================
-- Query Lineage via System Tables (API conceptual example)
-- Note: Exact schema may vary by Databricks version
-- =============================================================================

-- Upstream lineage: What feeds into this table?
SELECT
  source_table_full_name,
  source_column_name,
  target_column_name,
  transformation_type,
  last_updated
FROM system.access.column_lineage
WHERE target_table_full_name = 'main.gold.customer_summary'
ORDER BY source_table_full_name, source_column_name;


-- Downstream lineage: What does this table feed?
SELECT
  target_table_full_name,
  target_column_name,
  source_column_name,
  last_updated
FROM system.access.column_lineage
WHERE source_table_full_name = 'main.silver.customers'
ORDER BY target_table_full_name;


-- =============================================================================
-- Impact Analysis: What would be affected if source changes?
-- =============================================================================

-- Tables affected by changes to silver_customers
WITH RECURSIVE downstream AS (
  -- Base: direct dependencies
  SELECT DISTINCT target_table_full_name, 1 as level
  FROM system.access.table_lineage
  WHERE source_table_full_name = 'main.silver.customers'

  UNION ALL

  -- Recursive: indirect dependencies
  SELECT l.target_table_full_name, d.level + 1
  FROM system.access.table_lineage l
  JOIN downstream d ON l.source_table_full_name = d.target_table_full_name
  WHERE d.level < 5  -- Limit recursion depth
)
SELECT DISTINCT target_table_full_name, MIN(level) as dependency_level
FROM downstream
GROUP BY target_table_full_name
ORDER BY dependency_level, target_table_full_name;


-- =============================================================================
-- Data Freshness: When was each table last updated?
-- =============================================================================
SELECT
  table_catalog,
  table_schema,
  table_name,
  last_altered,
  DATEDIFF(CURRENT_TIMESTAMP, last_altered) as days_since_update
FROM system.information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold')
ORDER BY last_altered DESC;
