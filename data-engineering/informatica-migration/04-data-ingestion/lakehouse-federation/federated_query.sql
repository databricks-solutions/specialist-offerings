-- =============================================================================
-- Lakehouse Federation: Query External Databases In-Place
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Creates virtual connection to external database (Oracle, SQL Server, etc.)
-- - Queries external data without copying it into Databricks
-- - Pushes filters and predicates to source for performance
--
-- REPLACES IN INFORMATICA:
-- - Direct relational connections (with limitations on data movement)
-- - Nothing equivalent - Informatica always moves data
--
-- WHEN TO USE:
-- - POC/Validation: Test migration without moving data first
-- - Hybrid architectures: Keep some data on-prem while processing in cloud
-- - Real-time lookups: Query operational systems for latest data
-- - Cost optimization: Avoid duplicating rarely-accessed data
-- - Migration validation: Compare old vs new systems side-by-side
--
-- WHEN NOT TO USE:
-- - Large data scans or complex aggregations (move to lakehouse instead)
-- - High-frequency queries (consider Lakebase for low-latency)
-- - If same data queried repeatedly (ingest to Bronze)
--
-- PERFORMANCE TIPS:
-- - Push filters to source (WHERE clauses are pushed down)
-- - Select only needed columns (avoid SELECT *)
-- - Best for small result sets with selective filters
-- =============================================================================

-- Step 1: Create connection to external Oracle database
CREATE CONNECTION oracle_prod
  TYPE oracle
  OPTIONS (
    host 'oracle.company.com',
    port '1521',
    database 'PRODDB',
    user secret('db-scope', 'oracle-user'),
    password secret('db-scope', 'oracle-password')
  );

-- Step 2: Create foreign catalog (makes Oracle schemas visible)
CREATE FOREIGN CATALOG oracle_catalog
  USING CONNECTION oracle_prod;

-- Step 3: Query external data without moving it
-- Filters and aggregations are pushed to Oracle when possible
SELECT
  c.customer_id,
  c.customer_name,
  c.credit_limit,
  COUNT(o.order_id) as order_count,
  SUM(o.order_amount) as total_spent
FROM oracle_catalog.prod_schema.customers c
JOIN oracle_catalog.prod_schema.orders o
  ON c.customer_id = o.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL 30 DAYS  -- Pushed to Oracle
GROUP BY c.customer_id, c.customer_name, c.credit_limit
HAVING SUM(o.order_amount) > 1000;
