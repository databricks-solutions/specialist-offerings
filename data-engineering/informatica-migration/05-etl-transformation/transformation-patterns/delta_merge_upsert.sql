-- =============================================================================
-- Delta MERGE: Upsert (INSERT/UPDATE/DELETE) from CDC
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Applies changes from CDC source to target Delta table
-- - Handles INSERT, UPDATE, DELETE in single atomic operation
-- - ACID compliant - all or nothing
--
-- REPLACES IN INFORMATICA:
-- - Update Strategy Transformation (DD_INSERT, DD_UPDATE, DD_DELETE)
-- - Target table with Update/Insert mode
-- - Separate sessions for inserts vs updates
--
-- MERGE SYNTAX:
--   MERGE INTO target
--   USING source
--   ON join_condition
--   WHEN MATCHED AND condition THEN UPDATE/DELETE
--   WHEN NOT MATCHED AND condition THEN INSERT
--
-- CDC _change_type VALUES:
-- - 'INSERT': New record in source
-- - 'UPDATE': Modified record
-- - 'DELETE': Removed from source
--
-- MERGE BEST PRACTICES:
-- - Always filter source to only unprocessed changes
-- - Use _commit_timestamp for ordering
-- - Handle DELETE carefully (soft delete vs hard delete)
-- - Consider SCD Type 2 if history is needed
--
-- ALTERNATIVE: For simpler cases, use DLT APPLY CHANGES (see apply_changes_scd.sql)
-- =============================================================================

-- =============================================================================
-- Pattern 1: Full MERGE with INSERT/UPDATE/DELETE
-- =============================================================================
MERGE INTO silver_customer target
USING (
  -- Only process new changes since last run
  SELECT *
  FROM bronze_customer_cdc
  WHERE _commit_timestamp > (
    SELECT COALESCE(MAX(last_updated), '1900-01-01')
    FROM silver_customer
  )
  -- Handle multiple changes to same record - keep latest
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY _commit_timestamp DESC
  ) = 1
) source
ON target.customer_id = source.customer_id

-- Update existing records
WHEN MATCHED AND source._change_type = 'UPDATE' THEN
  UPDATE SET
    target.first_name = source.first_name,
    target.last_name = source.last_name,
    target.email = source.email,
    target.address = source.address,
    target.phone = source.phone,
    target.last_updated = source._commit_timestamp

-- Delete removed records (hard delete)
WHEN MATCHED AND source._change_type = 'DELETE' THEN
  DELETE

-- Insert new records
WHEN NOT MATCHED AND source._change_type IN ('INSERT', 'UPDATE') THEN
  INSERT (
    customer_id,
    first_name,
    last_name,
    email,
    address,
    phone,
    created_date,
    last_updated
  )
  VALUES (
    source.customer_id,
    source.first_name,
    source.last_name,
    source.email,
    source.address,
    source.phone,
    source._commit_timestamp,
    source._commit_timestamp
  );


-- =============================================================================
-- Pattern 2: Soft Delete (Mark as Inactive Instead of Removing)
-- =============================================================================
MERGE INTO silver_product target
USING bronze_product_cdc source
ON target.product_id = source.product_id

WHEN MATCHED AND source._change_type = 'UPDATE' THEN
  UPDATE SET
    target.product_name = source.product_name,
    target.price = source.price,
    target.category = source.category,
    target.last_updated = source._commit_timestamp

-- Soft delete: set is_active = FALSE instead of removing row
WHEN MATCHED AND source._change_type = 'DELETE' THEN
  UPDATE SET
    target.is_active = FALSE,
    target.deleted_date = source._commit_timestamp,
    target.last_updated = source._commit_timestamp

WHEN NOT MATCHED THEN
  INSERT (product_id, product_name, price, category, is_active, created_date, last_updated)
  VALUES (source.product_id, source.product_name, source.price, source.category,
          TRUE, source._commit_timestamp, source._commit_timestamp);


-- =============================================================================
-- Pattern 3: Insert-Only MERGE (Append New, Skip Existing)
-- For incremental loads without updates
-- =============================================================================
MERGE INTO silver_events target
USING bronze_events_new source
ON target.event_id = source.event_id

WHEN NOT MATCHED THEN
  INSERT *;  -- Insert all columns from source
