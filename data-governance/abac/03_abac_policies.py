# Databricks notebook source
# MAGIC %md
# MAGIC ## Add "user" to group: abac_group_af1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Table Tags Policy

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE POLICY af_1_table_access_policy
# MAGIC
# MAGIC ON CATALOG aa_catalog
# MAGIC
# MAGIC COMMENT 'hide tables when does not satisfy table tag condition'
# MAGIC
# MAGIC ROW FILTER aa_catalog.af_schema.filter_all
# MAGIC TO abac_group_af1
# MAGIC
# MAGIC FOR TABLES
# MAGIC WHEN NOT((hasTagValue("abac_sublob","AF") 
# MAGIC OR hasTagValue("abac_sublob","CB") 
# MAGIC OR hasTagValue("abac_sublob","CCB")) 
# MAGIC AND hasTagValue("abac_data_use_category","false"));
# MAGIC
# MAGIC -- MATCH COLUMNS (1==1) AS sec
# MAGIC -- USING COLUMNS(sec);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Current Table Tags Applied
# MAGIC #### Should be able to access only 2 tables in the entire catalog that satisfy condition:
# MAGIC abac_sublob = ("AF","CB","CCB") AND abac_data_use_category = "false"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.af_schema.accounts;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.af_schema.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.af_schema.transactions;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Column Tags Policy

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE POLICY af_1_col_access_policy
# MAGIC
# MAGIC ON CATALOG aa_catalog
# MAGIC COMMENT 'hide columns when does not satisfy column tag condition'
# MAGIC
# MAGIC COLUMN MASK aa_catalog.af_schema.mask_col
# MAGIC TO abac_group_af1
# MAGIC
# MAGIC FOR TABLES
# MAGIC WHEN ((hasTagValue("abac_sublob","AF") 
# MAGIC OR hasTagValue("abac_sublob","CB") 
# MAGIC OR hasTagValue("abac_sublob","CCB")) 
# MAGIC AND hasTagValue("abac_data_use_category","false"))
# MAGIC
# MAGIC MATCH COLUMNS 
# MAGIC NOT(hasTagValue("abac_protection_group","INTL") 
# MAGIC AND hasTagValue("abac_sensitive","false") 
# MAGIC AND hasTagValue("abac_pci","false")) AS sec
# MAGIC ON COLUMN sec;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.af_schema.accounts;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.af_schema.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.af_schema.transactions;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove "user" from group: abac_group_af1
# MAGIC ## Add "user" to group: abac_group_af2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE POLICY af_2_table_access_policy
# MAGIC ON CATALOG aa_catalog
# MAGIC COMMENT 'hide tables when does not satisfy table tag condition'
# MAGIC ROW FILTER aa_catalog.af_schema.filter_all
# MAGIC TO abac_group_af2
# MAGIC FOR TABLES
# MAGIC WHEN NOT((hasTagValue("abac_sublob","AF") OR hasTagValue("abac_sublob","CB") OR hasTagValue("abac_sublob","CCB")) AND hasTagValue("abac_data_use_category","false"))
# MAGIC MATCH COLUMNS (1==1) AS sec
# MAGIC USING COLUMNS(sec);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE POLICY af_2_col_access_policy
# MAGIC ON CATALOG aa_catalog
# MAGIC COMMENT 'hide columns when does not satisfy column tag condition'
# MAGIC COLUMN MASK aa_catalog.af_schema.mask_col
# MAGIC TO abac_group_af2
# MAGIC FOR TABLES
# MAGIC WHEN ((hasTagValue("abac_sublob","AF") OR hasTagValue("abac_sublob","CB") OR hasTagValue("abac_sublob","CCB")) AND hasTagValue("abac_data_use_category","false"))
# MAGIC MATCH COLUMNS NOT((hasTagValue("abac_protection_group","INTL") OR hasTagValue("abac_protection_group","CNFD")) AND hasTagValue("abac_sensitive","false") AND hasTagValue("abac_pci","false")) AS sec
# MAGIC ON COLUMN sec;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.af_schema.accounts;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.af_schema.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.af_schema.transactions;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove "user" from abac_group_af2
# MAGIC ## Add "user" to abac_group_bb1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE POLICY bb_1_table_access_policy
# MAGIC ON CATALOG aa_catalog
# MAGIC COMMENT 'hide tables when does not satisfy table tag condition'
# MAGIC ROW FILTER aa_catalog.bb_schema.filter_all
# MAGIC TO abac_group_bb1
# MAGIC FOR TABLES
# MAGIC WHEN NOT((hasTagValue("abac_sublob","CB") OR hasTagValue("abac_sublob","CCB") OR hasTagValue("abac_sublob","BB")) AND hasTagValue("abac_data_use_category","false"))
# MAGIC MATCH COLUMNS (1==1) AS sec
# MAGIC USING COLUMNS(sec);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE POLICY bb_1_col_access_policy
# MAGIC ON CATALOG aa_catalog
# MAGIC COMMENT 'hide columns when does not satisfy column tag condition'
# MAGIC COLUMN MASK aa_catalog.bb_schema.mask_col
# MAGIC TO abac_group_bb1
# MAGIC FOR TABLES
# MAGIC WHEN ((hasTagValue("abac_sublob","CB") OR hasTagValue("abac_sublob","CCB") OR hasTagValue("abac_sublob","BB")) AND hasTagValue("abac_data_use_category","false"))
# MAGIC MATCH COLUMNS NOT((hasTagValue("abac_protection_group","INTL") OR hasTagValue("abac_protection_group","CNFD") OR hasTagValue("abac_protection_group","HICNFD") OR hasTagValue("abac_protection_group","HIGHPI") OR hasTagValue("abac_protection_group","PIDRID") OR hasTagValue("abac_protection_group","CNFDPI") OR hasTagValue("abac_protection_group","GOVTID") OR hasTagValue("abac_protection_group","RISKSC")) AND hasTagValue("abac_sensitive","false") AND hasTagValue("abac_pci","false")) AS sec
# MAGIC ON COLUMN sec;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### access af_schema tables ?

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.af_schema.accounts;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.af_schema.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.af_schema.transactions;

# COMMAND ----------

# MAGIC %md
# MAGIC ### access bb_schema tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.bb_schema.commercial_accounts;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.bb_schema.corporate_clients;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aa_catalog.bb_schema.corporate_transactions;

# COMMAND ----------

