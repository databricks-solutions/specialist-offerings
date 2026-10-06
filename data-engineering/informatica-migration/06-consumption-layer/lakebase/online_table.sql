-- =============================================================================
-- Lakebase (Online Tables): Low-Latency Application Serving
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Syncs Delta Lake tables to optimized serving layer
-- - Provides sub-millisecond point lookups via REST/JDBC
-- - Automatically refreshes from source Delta tables
--
-- REPLACES IN INFORMATICA:
-- - Loading data to Oracle/SQL Server for application serving
-- - Reverse ETL to operational databases
-- - Custom sync jobs to application databases
--
-- USE CASES:
-- - Real-time recommendation engines
-- - Fraud detection lookups
-- - Customer profile serving for web/mobile apps
-- - Inventory availability checks
-- - Feature serving for ML models
--
-- PERFORMANCE CHARACTERISTICS:
-- | Metric       | Lakebase                | SQL Warehouse           |
-- |--------------|-------------------------|-------------------------|
-- | Latency      | < 10ms point lookups    | 100ms - seconds         |
-- | Throughput   | 10,000+ QPS             | 100s QPS                |
-- | Best For     | Key-based lookups       | Analytical queries      |
-- | Consistency  | Eventual (configurable) | Strong                  |
--
-- LIMITATIONS:
-- - Optimized for point lookups (by primary key)
-- - Not for complex analytical queries
-- - Size limits apply (check documentation)
-- =============================================================================

-- =============================================================================
-- Step 1: Create Online Table from Gold Delta Table
-- =============================================================================

-- Create the base table (if not exists)
CREATE TABLE IF NOT EXISTS main.gold.dim_customer (
  customer_id BIGINT,
  customer_name STRING,
  email STRING,
  customer_tier STRING,
  lifetime_value DECIMAL(15,2),
  last_order_date DATE,
  PRIMARY KEY (customer_id)  -- Required for Online Tables
);

-- Enable Online Table sync
ALTER TABLE main.gold.dim_customer
SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',          -- Required for sync
  'pipelines.onlineTableEnabled' = 'true',        -- Enable Online Table
  'pipelines.onlineTableRefreshInterval' = '5 minutes'  -- Sync frequency
);


-- =============================================================================
-- Step 2: Alternative - Create dedicated Online Table with subset of columns
-- =============================================================================

-- Create Online Table with only columns needed for app serving
CREATE OR REPLACE TABLE main.online.customer_profile (
  customer_id BIGINT PRIMARY KEY,
  customer_name STRING,
  customer_tier STRING,
  lifetime_value DECIMAL(15,2),
  credit_limit DECIMAL(15,2),
  is_active BOOLEAN
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'pipelines.onlineTableEnabled' = 'true',
  'pipelines.onlineTableRefreshInterval' = '1 minute'  -- Near real-time
);

-- Populate from Gold
INSERT OVERWRITE main.online.customer_profile
SELECT
  customer_id,
  full_name as customer_name,
  customer_tier,
  lifetime_value,
  credit_limit,
  activity_status IN ('Active', 'Recent') as is_active
FROM main.gold.dim_customer;


-- =============================================================================
-- Step 3: Create Online Table for Inventory Lookups
-- =============================================================================

CREATE OR REPLACE TABLE main.online.inventory_availability (
  product_id BIGINT PRIMARY KEY,
  warehouse_id BIGINT,
  product_name STRING,
  available_quantity INT,
  last_updated TIMESTAMP
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'pipelines.onlineTableEnabled' = 'true',
  'pipelines.onlineTableRefreshInterval' = '30 seconds'  -- Frequent refresh for inventory
);


-- =============================================================================
-- Query Online Tables (same SQL, optimized serving)
-- =============================================================================

-- Point lookup by primary key (sub-10ms latency)
SELECT * FROM main.online.customer_profile WHERE customer_id = 12345;

-- Batch lookup (still fast for small batches)
SELECT * FROM main.online.customer_profile
WHERE customer_id IN (12345, 12346, 12347, 12348);
