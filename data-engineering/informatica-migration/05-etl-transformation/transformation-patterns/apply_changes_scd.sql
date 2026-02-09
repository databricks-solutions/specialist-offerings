-- =============================================================================
-- DLT APPLY CHANGES: Declarative CDC Processing
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Processes CDC stream and maintains target table state
-- - Handles INSERT/UPDATE/DELETE automatically
-- - Much simpler than manual MERGE for most use cases
--
-- REPLACES IN INFORMATICA:
-- - Update Strategy transformation
-- - SCD Wizard (for Type 1)
-- - Complex CDC mapping logic
--
-- APPLY CHANGES vs MANUAL MERGE:
-- | Aspect          | APPLY CHANGES           | Manual MERGE           |
-- |-----------------|-------------------------|------------------------|
-- | Complexity      | Simple, declarative     | More code              |
-- | Flexibility     | Limited options         | Full control           |
-- | SCD Support     | Type 1 & 2 built-in     | Custom implementation  |
-- | Streaming       | Native support          | Requires foreachBatch  |
-- | Best For        | Standard CDC patterns   | Complex business logic |
--
-- SCD TYPE SUPPORT:
-- - SCD Type 1: Default - overwrites with latest values
-- - SCD Type 2: Use STORED AS SCD TYPE 2 for history tracking
--
-- SYNTAX:
--   APPLY CHANGES INTO target
--   FROM source
--   KEYS (key_columns)
--   [APPLY AS DELETE WHEN condition]
--   [IGNORE NULL UPDATES]
--   SEQUENCE BY ordering_column
--   [COLUMNS column_list | COLUMNS * EXCEPT (excluded_columns)]
--   [STORED AS SCD TYPE 2]
-- =============================================================================


-- =============================================================================
-- Pattern 1: SCD Type 1 (Current State Only)
-- Most common pattern - just maintain latest version
-- =============================================================================
CREATE OR REFRESH STREAMING LIVE TABLE silver_inventory;

APPLY CHANGES INTO LIVE.silver_inventory
FROM STREAM(bronze_inventory_cdc)
KEYS (inv_id)                                    -- Primary key
APPLY AS DELETE WHEN _change_type = 'DELETE'    -- Handle deletes
SEQUENCE BY _commit_timestamp                    -- Order changes correctly
COLUMNS * EXCEPT (_change_type, _commit_timestamp, _sequence_number);
-- Exclude CDC metadata columns from target


-- =============================================================================
-- Pattern 2: SCD Type 2 (Full History)
-- Track all historical changes with effective dates
-- =============================================================================
CREATE OR REFRESH STREAMING LIVE TABLE silver_customer_history;

APPLY CHANGES INTO LIVE.silver_customer_history
FROM STREAM(bronze_customer_cdc)
KEYS (customer_id)
APPLY AS DELETE WHEN _change_type = 'DELETE'
SEQUENCE BY _commit_timestamp
COLUMNS * EXCEPT (_change_type, _commit_timestamp, _sequence_number)
STORED AS SCD TYPE 2;  -- Enables history tracking

-- SCD Type 2 automatically adds these columns:
-- __START_AT: Effective start timestamp
-- __END_AT: Effective end timestamp (NULL for current)


-- =============================================================================
-- Pattern 3: Ignore NULL Updates
-- Don't overwrite with NULLs (common requirement)
-- =============================================================================
CREATE OR REFRESH STREAMING LIVE TABLE silver_product;

APPLY CHANGES INTO LIVE.silver_product
FROM STREAM(bronze_product_cdc)
KEYS (product_id)
APPLY AS DELETE WHEN _change_type = 'DELETE'
IGNORE NULL UPDATES    -- Don't overwrite existing values with NULL
SEQUENCE BY _commit_timestamp
COLUMNS * EXCEPT (_change_type, _commit_timestamp);


-- =============================================================================
-- Pattern 4: Specific Column Selection
-- Only update certain columns, exclude others
-- =============================================================================
CREATE OR REFRESH STREAMING LIVE TABLE silver_customer_contact;

APPLY CHANGES INTO LIVE.silver_customer_contact
FROM STREAM(bronze_customer_cdc)
KEYS (customer_id)
SEQUENCE BY _commit_timestamp
COLUMNS (
  customer_id,
  email,
  phone,
  address,
  city,
  state,
  zip_code
  -- Intentionally excluding: credit_limit, internal_notes, etc.
);


-- =============================================================================
-- Pattern 5: Conditional Delete (Soft Delete via CDC)
-- =============================================================================
CREATE OR REFRESH STREAMING LIVE TABLE silver_orders;

APPLY CHANGES INTO LIVE.silver_orders
FROM STREAM(bronze_orders_cdc)
KEYS (order_id)
-- Delete when change_type is DELETE or status is CANCELLED
APPLY AS DELETE WHEN _change_type = 'DELETE' OR status = 'CANCELLED'
SEQUENCE BY _commit_timestamp
COLUMNS * EXCEPT (_change_type, _commit_timestamp);
