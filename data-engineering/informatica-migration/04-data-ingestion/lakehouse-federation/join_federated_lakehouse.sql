-- =============================================================================
-- Join Federated (External) and Lakehouse (Delta) Data
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Combines real-time external data with processed lakehouse data
-- - Useful for enrichment, validation, and hybrid queries
--
-- USE CASES:
-- 1. Migration Validation: Compare legacy Oracle data with new Gold tables
-- 2. Real-time Enrichment: Join static dimensions with live operational data
-- 3. Hybrid Reporting: Combine cloud analytics with on-prem master data
--
-- PERFORMANCE NOTES:
-- - Keep federated side small (filtered) for best performance
-- - Large scans on federated tables will be slow
-- - Consider materializing frequently-joined federated data
-- =============================================================================

-- Example 1: Migration Validation
-- Compare customer data between legacy Oracle and new Gold table
SELECT
  'LEGACY' as source,
  fed.customer_id,
  fed.customer_name,
  fed.total_orders as legacy_orders,
  NULL as gold_orders
FROM oracle_catalog.prod_schema.customer_summary fed
WHERE fed.customer_id NOT IN (SELECT customer_id FROM gold_customer_analytics)

UNION ALL

SELECT
  'GOLD' as source,
  gold.customer_id,
  gold.customer_name,
  NULL as legacy_orders,
  gold.total_orders as gold_orders
FROM gold_customer_analytics gold
WHERE gold.customer_id NOT IN (SELECT customer_id FROM oracle_catalog.prod_schema.customer_summary);


-- Example 2: Enrich Gold data with real-time credit limits from Oracle
SELECT
  gold.customer_id,
  gold.customer_name,
  gold.total_purchases,
  gold.last_purchase_date,
  fed.credit_limit,           -- Real-time from Oracle
  fed.credit_score,           -- Real-time from Oracle
  CASE
    WHEN gold.total_purchases > fed.credit_limit THEN 'OVER_LIMIT'
    ELSE 'OK'
  END as credit_status
FROM gold_customer_analytics gold
JOIN oracle_catalog.prod_schema.customers fed
  ON fed.customer_id = gold.customer_id
WHERE gold.customer_status = 'ACTIVE';
