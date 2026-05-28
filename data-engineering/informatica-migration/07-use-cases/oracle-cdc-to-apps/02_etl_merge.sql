-- =============================================================================
-- USE CASE 3: Oracle CDC → Application Serving
-- =============================================================================
-- STEP 2: DLT PIPELINE WITH CDC PROCESSING
--
-- SCENARIO:
-- Process CDC data from Lakeflow Connect and maintain current inventory state.
-- Applications query Lakebase for real-time inventory lookups.
--
-- OLD PATTERN:
--   PowerExchange CDC → Update Strategy transformation → SQL Server
--   (Complex mapping with DD_INSERT/DD_UPDATE/DD_DELETE flags)
--
-- NEW PATTERN:
--   Bronze CDC → APPLY CHANGES → Silver → Gold → Lakebase
--   (Declarative CDC processing, automatic INSERT/UPDATE/DELETE handling)
--
-- THIS FILE: Deploy as DLT Pipeline
-- =============================================================================


-- =============================================================================
-- SILVER: Current Inventory State (SCD Type 1)
-- =============================================================================
-- APPLY CHANGES automatically handles INSERT/UPDATE/DELETE from CDC stream

CREATE OR REFRESH STREAMING LIVE TABLE silver_inventory;

APPLY CHANGES INTO LIVE.silver_inventory
FROM STREAM(bronze_inventory_stock_cdc)
KEYS (product_id, warehouse_id)                  -- Composite primary key
APPLY AS DELETE WHEN _change_type = 'DELETE'    -- Handle hard deletes
IGNORE NULL UPDATES                              -- Don't overwrite with NULLs
SEQUENCE BY _commit_timestamp                    -- Order by source commit time
COLUMNS * EXCEPT (_change_type, _commit_timestamp, _sequence_number);


-- =============================================================================
-- SILVER: Inventory Transactions (Append-Only Fact)
-- =============================================================================
CREATE OR REFRESH STREAMING LIVE TABLE silver_inventory_transactions (
  CONSTRAINT valid_txn EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity EXPECT (quantity != 0) ON VIOLATION DROP ROW
)
COMMENT "Inventory movement transactions"
AS SELECT
  transaction_id,
  product_id,
  warehouse_id,
  transaction_type,  -- 'RECEIPT', 'SHIPMENT', 'ADJUSTMENT', 'TRANSFER'
  quantity,
  unit_cost,
  quantity * unit_cost as total_value,
  reference_number,
  _commit_timestamp as transaction_timestamp,
  current_timestamp() as processed_timestamp
FROM STREAM(bronze_inventory_transactions_cdc)
WHERE _change_type IN ('INSERT', 'UPDATE');  -- Ignore deletes for fact table


-- =============================================================================
-- SILVER: Product Dimension
-- =============================================================================
CREATE OR REFRESH LIVE TABLE silver_dim_product
COMMENT "Product master data"
AS SELECT
  product_id,
  product_name,
  product_category,
  product_subcategory,
  unit_of_measure,
  unit_cost,
  unit_price,
  is_active,
  created_date,
  last_updated
FROM bronze_inventory_products;


-- =============================================================================
-- SILVER: Warehouse Dimension
-- =============================================================================
CREATE OR REFRESH LIVE TABLE silver_dim_warehouse
COMMENT "Warehouse master data"
AS SELECT
  warehouse_id,
  warehouse_name,
  warehouse_code,
  address,
  city,
  state,
  region,
  warehouse_type,  -- 'DISTRIBUTION', 'RETAIL', 'RETURNS'
  is_active
FROM bronze_inventory_warehouses;


-- =============================================================================
-- GOLD: Current Inventory with Enrichment (for Application Serving)
-- =============================================================================
CREATE OR REFRESH LIVE TABLE gold_inventory_current
COMMENT "Current inventory levels with product and warehouse details - feeds Lakebase"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
  -- Keys
  i.product_id,
  i.warehouse_id,

  -- Product info
  p.product_name,
  p.product_category,
  p.unit_of_measure,

  -- Warehouse info
  w.warehouse_name,
  w.warehouse_code,
  w.region as warehouse_region,

  -- Inventory metrics
  i.quantity_on_hand,
  i.quantity_reserved,
  i.quantity_on_hand - i.quantity_reserved as available_quantity,
  i.quantity_on_order,
  i.reorder_point,
  i.last_count_date,

  -- Calculated fields for apps
  CASE
    WHEN i.quantity_on_hand - i.quantity_reserved <= 0 THEN 'OUT_OF_STOCK'
    WHEN i.quantity_on_hand - i.quantity_reserved <= i.reorder_point THEN 'LOW_STOCK'
    ELSE 'IN_STOCK'
  END as stock_status,

  -- Audit
  i.last_updated,
  current_timestamp() as etl_timestamp

FROM LIVE.silver_inventory i
JOIN LIVE.silver_dim_product p ON i.product_id = p.product_id
JOIN LIVE.silver_dim_warehouse w ON i.warehouse_id = w.warehouse_id
WHERE i.quantity_on_hand > 0 OR i.quantity_on_order > 0;  -- Exclude zero inventory
