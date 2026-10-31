# Databricks notebook source
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- ABAC PoC: Simple RLS and Column Masking Functions
# MAGIC -- =====================================================================
# MAGIC -- This script creates 2 basic functions per schema:
# MAGIC -- 1. filter_all - RLS function returning BOOLEAN
# MAGIC -- 2. mask_ct - Column masking function returning masked string
# MAGIC -- =====================================================================
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- AF Dataspace Functions
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION 
# MAGIC aa_catalog.af_schema.filter_all() 
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN FALSE;
# MAGIC
# MAGIC -- CREATE OR REPLACE  FUNCTION 
# MAGIC -- aa_catalog.af_schema.mask_col(col STRING) 
# MAGIC -- RETURN '***-**-****';
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION aa_catalog.af_schema.mask_col(col VARIANT)
# MAGIC   RETURNS VARIANT
# MAGIC   COMMENT 'Generic masking UDF - returns static masked value matching the input data type'
# MAGIC   RETURN
# MAGIC     CASE
# MAGIC       WHEN schema_of_variant(col) = 'STRING'
# MAGIC         THEN CAST('***-**-****' AS VARIANT)
# MAGIC       WHEN schema_of_variant(col) IN ('BIGINT', 'INT', 'SMALLINT', 'TINYINT')
# MAGIC         THEN CAST(0 AS VARIANT)
# MAGIC       WHEN schema_of_variant(col) LIKE 'DECIMAL%'
# MAGIC         THEN CAST(0.00 AS VARIANT)
# MAGIC       WHEN schema_of_variant(col) IN ('DOUBLE', 'FLOAT')
# MAGIC         THEN CAST(0.0 AS VARIANT)
# MAGIC       WHEN schema_of_variant(col) = 'BOOLEAN'
# MAGIC         THEN CAST(FALSE AS VARIANT)
# MAGIC       WHEN schema_of_variant(col) = 'DATE'
# MAGIC         THEN CAST(DATE'1900-01-01' AS VARIANT)
# MAGIC       WHEN schema_of_variant(col) IN ('TIMESTAMP', 'TIMESTAMP_NTZ')
# MAGIC         THEN CAST(TIMESTAMP'1900-01-01 00:00:00' AS VARIANT)
# MAGIC       ELSE CAST('***' AS VARIANT)
# MAGIC     END;

# COMMAND ----------

# MAGIC %sql
# MAGIC   -- For STRING columns
# MAGIC   -- CREATE OR REPLACE FUNCTION
# MAGIC   -- aa_catalog.af_schema.mask_col(col STRING)
# MAGIC   -- RETURN CAST('***-**-****' as STRING);
# MAGIC
# MAGIC   -- -- For DOUBLE columns
# MAGIC   -- CREATE OR REPLACE FUNCTION
# MAGIC   -- aa_catalog.af_schema.mask_col(col DOUBLE)
# MAGIC   -- RETURN -999.99;
# MAGIC
# MAGIC   -- -- For INT columns
# MAGIC   -- CREATE OR REPLACE FUNCTION
# MAGIC   -- aa_catalog.af_schema.mask_col(col INT)
# MAGIC   -- RETURN -999;
# MAGIC
# MAGIC   -- -- For BIGINT columns
# MAGIC   -- CREATE OR REPLACE FUNCTION
# MAGIC   -- aa_catalog.af_schema.mask_col(col BIGINT)
# MAGIC   -- RETURN CAST(-999 AS BIGINT);
# MAGIC
# MAGIC   -- -- For DATE columns
# MAGIC   -- CREATE OR REPLACE FUNCTION
# MAGIC   -- aa_catalog.af_schema.mask_col(col DATE)
# MAGIC   -- RETURN CAST('1900-01-01' AS DATE);
# MAGIC
# MAGIC   -- -- For TIMESTAMP columns
# MAGIC   CREATE OR REPLACE FUNCTION
# MAGIC   aa_catalog.af_schema.mask_col(col TIMESTAMP)
# MAGIC   RETURN CAST('1900-01-01 00:00:00' AS TIMESTAMP);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- BB Dataspace Functions  
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE  FUNCTION aa_catalog.bb_schema.filter_all(col STRING) 
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN col == FALSE;
# MAGIC
# MAGIC CREATE OR REPLACE  FUNCTION aa_catalog.bb_schema.mask_col(ct STRING) 
# MAGIC RETURN '***-**-****';
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- Verification
# MAGIC -- =====================================================================
# MAGIC
# MAGIC -- -- Test AF functions
# MAGIC -- SELECT main.af_dataspace.filter_all('TRUE') as af_filter_test;
# MAGIC -- SELECT main.af_dataspace.filter_all('FALSE') as af_filter_test2;  
# MAGIC -- SELECT main.af_dataspace.mask_ct('sensitive_data') as af_mask_test;
# MAGIC
# MAGIC -- -- Test BB functions
# MAGIC -- SELECT main.bb_dataspace.filter_all('TRUE') as bb_filter_test;
# MAGIC -- SELECT main.bb_dataspace.filter_all('FALSE') as bb_filter_test2;
# MAGIC -- SELECT main.bb_dataspace.mask_ct('sensitive_data') as bb_mask_test;
# MAGIC
# MAGIC -- -- List functions
# MAGIC -- SHOW FUNCTIONS IN main.af_dataspace;
# MAGIC -- SHOW FUNCTIONS IN main.bb_dataspace; 

# COMMAND ----------

