"""
USE CASE 2: File → ETL → Power BI (No Oracle!)
==============================================
STEP 1: INGESTION

SCENARIO:
Daily sales CSV files land in cloud storage. Previously, these were loaded
to Oracle using Informatica, then Power BI queried Oracle. Now, we ingest
directly to Bronze and Power BI connects to Gold tables via SQL Warehouse.

OLD PATTERN:
  CSV → Informatica → Oracle → Power BI
  (ETL and BI compete for Oracle resources)

NEW PATTERN:
  CSV → Auto Loader → Bronze → DLT → Gold → SQL Warehouse → Power BI
  (Isolated compute, no Oracle needed)

BENEFITS:
- No Oracle license needed for BI serving
- Isolated compute for BI queries (no ETL contention)
- Real-time data (Power BI sees data as soon as it lands in Gold)
- Auto-scaling for varying BI workload

THIS FILE: Phase 1 - Auto Loader ingestion
"""

from pyspark.sql.functions import col, current_timestamp

# =============================================================================
# Auto Loader Configuration
# =============================================================================
LANDING_PATH = "/mnt/landing/sales/*.csv"
SCHEMA_PATH = "/mnt/schemas/sales"
CHECKPOINT_PATH = "/mnt/checkpoints/bronze_sales"

# =============================================================================
# Streaming Ingestion with Auto Loader
# =============================================================================
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", SCHEMA_PATH) \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("header", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .load(LANDING_PATH)

# Add ingestion metadata
df_with_metadata = df \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_file", col("_metadata.file_path"))

# Write to Bronze
df_with_metadata.writeStream \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(availableNow=True)  # Process available files, then stop
    .table("bronze_sales")

print("Ingestion complete: CSV files loaded to bronze_sales")
print("Next: Deploy 02_etl.sql as DLT pipeline")
print("Then: Connect Power BI to SQL Warehouse → gold_daily_sales")
