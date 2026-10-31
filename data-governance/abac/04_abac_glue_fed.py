# Databricks notebook source
# MAGIC %sql
# MAGIC select * from aa_fed_glue.markets_db.quotes_pqt;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_fed_glue.markets_db.trades_pqt;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row Level Filter

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE POLICY glue_fed_af_1_table_access_policy
# MAGIC ON CATALOG aa_fed_glue
# MAGIC COMMENT 'hide tables when does not satisfy table tag condition'
# MAGIC ROW FILTER main.af_schema.filter_all
# MAGIC TO abac_group_af1
# MAGIC FOR TABLES
# MAGIC WHEN NOT((hasTagValue("abac_sublob","AF") OR hasTagValue("abac_sublob","CB") OR hasTagValue("abac_sublob","CCB")) AND hasTagValue("abac_data_use_category","false"))
# MAGIC MATCH COLUMNS (1==1) AS sec
# MAGIC USING COLUMNS(sec);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_fed_glue.markets_db.quotes_pqt;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_fed_glue.markets_db.trades_pqt;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE POLICY glue_fed_af_1_col_access_policy
# MAGIC ON CATALOG aa_fed_glue
# MAGIC COMMENT 'hide columns when does not satisfy column tag condition'
# MAGIC COLUMN MASK main.af_schema.mask_ct
# MAGIC TO abac_group_af1
# MAGIC FOR TABLES
# MAGIC WHEN ((hasTagValue("abac_sublob","AF") OR hasTagValue("abac_sublob","CB") OR hasTagValue("abac_sublob","CCB")) AND hasTagValue("abac_data_use_category","false"))
# MAGIC MATCH COLUMNS NOT(hasTagValue("abac_protection_group","INTL") AND hasTagValue("abac_sensitive","false") AND hasTagValue("abac_pci","false")) AS sec
# MAGIC ON COLUMN sec;

# COMMAND ----------

# MAGIC %md
# MAGIC #TEST

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_glue_cat.sample_db.sample_customer_parquet limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE POLICY glue_fed_af_1_col_access_policy
# MAGIC ON CATALOG aa_glue_cat
# MAGIC COMMENT 'hide columns when does not satisfy column tag condition'
# MAGIC COLUMN MASK aa_catalog.af_schema.mask_col
# MAGIC TO abac_group_af1
# MAGIC FOR TABLES
# MAGIC WHEN ((hasTagValue("abac_sublob","AF")))
# MAGIC MATCH COLUMNS NOT(hasTagValue("abac_sensitive","false")) AS sec
# MAGIC ON COLUMN sec;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_glue_cat.sample_db.sample_customer_parquet limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_glue_cat.sample_db.sample_products;

# COMMAND ----------

# MAGIC %sql
# MAGIC   CREATE OR REPLACE FUNCTION aa_catalog.af_schema.mask_product_name(product_name STRING)
# MAGIC   RETURN CASE
# MAGIC     WHEN is_account_group_member('abac_group_af1') THEN '****'
# MAGIC     ELSE product_name
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC   ALTER TABLE aa_glue_cat.sample_db.sample_products
# MAGIC   ALTER COLUMN product_name SET MASK aa_catalog.af_schema.mask_product_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_glue_cat.sample_db.sample_products;

# COMMAND ----------

  display(spark.read.parquet('s3://<your-bucket>/path/to/emp_dbx/'))

# COMMAND ----------

df = spark.read.table("aa_glue_cat.aa_glue.employees_2")

# COMMAND ----------

display(df)

# COMMAND ----------

