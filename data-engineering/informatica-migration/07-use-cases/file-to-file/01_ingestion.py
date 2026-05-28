"""
USE CASE 1: File → ETL → File (Legacy Integration)
==================================================
STEP 1: INGESTION

SCENARIO:
A mainframe system generates fixed-width file `trans_dump.dat` containing
daily transaction data. The file needs to be parsed, validated, transformed,
and output as a single CSV file `finance_upload.csv` for an SAP system.

INFORMATICA PATTERN (OLD):
- Flat File Source with fixed-width definition
- Expression transformation for date conversion
- Filter for validation
- Flat File Target with merge type = sequential

DATABRICKS PATTERN (NEW):
Phase 1: Auto Loader ingests fixed-width file → Bronze
Phase 2: DLT transforms and validates → Silver → Gold
Phase 3: Export Gold to single CSV for SAP

THIS FILE: Phase 1 - Ingestion
"""

from pyspark.sql.functions import col, substring, to_date, trim, when, lit

# =============================================================================
# Fixed-Width File Layout (from mainframe documentation)
# =============================================================================
# Position  Length  Field           Type
# 1-10      10      txn_id          String
# 11-20     10      amount          Numeric (2 decimal places implied)
# 21-28     8       txn_date        Date (YYYYMMDD)
# 29-38     10      account_id      String
# 39-48     10      cost_center     String
# =============================================================================

# Read fixed-width file as raw text using Auto Loader
df_raw = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "text") \
    .option("cloudFiles.schemaLocation", "/mnt/schemas/mainframe_trans") \
    .load("/mnt/landing/mainframe/trans_dump*.dat")

# Parse fixed-width fields based on layout
df_parsed = df_raw.select(
    # Transaction ID: positions 1-10
    trim(substring(col("value"), 1, 10)).alias("txn_id"),

    # Amount: positions 11-20 (implied 2 decimal places)
    (substring(col("value"), 11, 10).cast("double") / 100).alias("amount"),

    # Date: positions 21-28 (YYYYMMDD format)
    to_date(substring(col("value"), 21, 8), "yyyyMMdd").alias("txn_date"),

    # Account ID: positions 29-38
    trim(substring(col("value"), 29, 10)).alias("account_id"),

    # Cost Center: positions 39-48
    trim(substring(col("value"), 39, 10)).alias("cost_center"),

    # Add ingestion metadata
    col("_metadata.file_path").alias("source_file"),
    col("_metadata.file_modification_time").alias("file_timestamp")
)

# Filter out header/trailer records if present
df_filtered = df_parsed.where(
    "txn_id IS NOT NULL AND txn_id != '' AND txn_id NOT LIKE 'HDR%' AND txn_id NOT LIKE 'TRL%'"
)

# Write to Bronze table
df_filtered.writeStream \
    .option("checkpointLocation", "/mnt/checkpoints/bronze_mainframe_trans") \
    .trigger(availableNow=True) \
    .table("bronze_mainframe_transactions")

print("Step 1 Complete: Fixed-width file ingested to Bronze table")
print("Next: Run 02_etl.py to transform data")
