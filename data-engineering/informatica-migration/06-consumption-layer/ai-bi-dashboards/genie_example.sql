-- =============================================================================
-- AI/BI Dashboards & Genie: Natural Language Analytics
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Enables business users to query data using natural language
-- - Auto-generates SQL from plain English questions
-- - Creates self-service dashboards without SQL knowledge
--
-- REPLACES IN INFORMATICA:
-- - Nothing (Informatica has no built-in BI)
-- - Eliminates need for separate tools like Tableau, Power BI for simple queries
--
-- HOW IT WORKS:
-- 1. User asks question in natural language
-- 2. Genie understands table schemas and relationships
-- 3. Generates optimized SQL query
-- 4. Returns results with visualizations
--
-- SETUP REQUIREMENTS:
-- - Tables must be in Unity Catalog
-- - Add table/column descriptions for better Genie understanding
-- - Grant appropriate permissions via Unity Catalog
--
-- BEST PRACTICES FOR GENIE:
-- - Add COMMENT to tables and columns (helps Genie understand semantics)
-- - Use business-friendly column names (customer_name vs cust_nm)
-- - Create Gold tables with pre-joined, denormalized data
-- - Document common business terms in table descriptions
-- =============================================================================

-- =============================================================================
-- Example: User asks Genie a question
-- =============================================================================

-- USER QUESTION: "Show me top 10 customers by revenue this quarter"

-- GENIE GENERATES:
SELECT
  customer_name,
  SUM(order_amount) as total_revenue
FROM gold_customer_orders
WHERE order_date >= DATE_TRUNC('quarter', CURRENT_DATE)
GROUP BY customer_name
ORDER BY total_revenue DESC
LIMIT 10;


-- USER QUESTION: "What was our sales trend by month last year?"

-- GENIE GENERATES:
SELECT
  DATE_TRUNC('month', sale_date) as month,
  SUM(net_amount) as total_sales,
  COUNT(DISTINCT customer_id) as unique_customers,
  COUNT(sale_id) as transaction_count
FROM gold_fact_sales
WHERE sale_date >= DATE_ADD(CURRENT_DATE, -365)
GROUP BY DATE_TRUNC('month', sale_date)
ORDER BY month;


-- USER QUESTION: "Which products are underperforming vs last quarter?"

-- GENIE GENERATES:
WITH current_quarter AS (
  SELECT product_id, SUM(quantity) as current_qty
  FROM gold_fact_sales
  WHERE sale_date >= DATE_TRUNC('quarter', CURRENT_DATE)
  GROUP BY product_id
),
previous_quarter AS (
  SELECT product_id, SUM(quantity) as previous_qty
  FROM gold_fact_sales
  WHERE sale_date >= DATE_ADD(DATE_TRUNC('quarter', CURRENT_DATE), -90)
    AND sale_date < DATE_TRUNC('quarter', CURRENT_DATE)
  GROUP BY product_id
)
SELECT
  p.product_name,
  p.category,
  COALESCE(c.current_qty, 0) as current_quarter_sales,
  COALESCE(q.previous_qty, 0) as previous_quarter_sales,
  COALESCE(c.current_qty, 0) - COALESCE(q.previous_qty, 0) as change,
  ROUND((COALESCE(c.current_qty, 0) - COALESCE(q.previous_qty, 0)) * 100.0
        / NULLIF(q.previous_qty, 0), 1) as pct_change
FROM gold_dim_product p
LEFT JOIN current_quarter c ON p.product_id = c.product_id
LEFT JOIN previous_quarter q ON p.product_id = q.product_id
WHERE COALESCE(c.current_qty, 0) < COALESCE(q.previous_qty, 0) * 0.8  -- 20% decline
ORDER BY change ASC
LIMIT 20;


-- =============================================================================
-- Table Setup: Add descriptions for better Genie understanding
-- =============================================================================

-- Add table description
COMMENT ON TABLE gold_fact_sales IS
'Sales transactions fact table containing all completed orders.
Key metrics: net_amount (after discounts), quantity, gross_margin.
Use sale_date for time-based analysis.
Joins to: gold_dim_customer, gold_dim_product, gold_dim_store.';

-- Add column descriptions
ALTER TABLE gold_fact_sales ALTER COLUMN net_amount
COMMENT 'Final sale amount after applying discounts. Use for revenue reporting.';

ALTER TABLE gold_fact_sales ALTER COLUMN customer_tier
COMMENT 'Customer value tier: VIP (>$10k lifetime), Premium (>$5k), Regular, New';

ALTER TABLE gold_dim_customer ALTER COLUMN activity_status
COMMENT 'Customer engagement status: Active (ordered in 30 days), Recent (90 days), Lapsed (365 days), Dormant (>365 days)';
