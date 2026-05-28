"""
USE CASE 1: File → ETL → File (Legacy Integration)
==================================================
STEP 3: EXPORT TO CSV FOR SAP

SCENARIO:
Export Gold table to single CSV file for SAP upload.

INFORMATICA PATTERN (OLD):
- Flat File Target with Merge Type = Sequential
- Session config for single output file
- Post-session command to rename file

DATABRICKS PATTERN (NEW):
- Read Gold table
- Coalesce to single partition
- Write CSV with SAP-compatible format
- Rename to final filename

IMPORTANT NOTES:
- coalesce(1) forces single file but is memory-intensive
- Only use for small datasets (< 2GB recommended)
- For larger files, consider chunking or different delivery method

THIS FILE: Phase 3 - Export to CSV
Run as a scheduled Databricks Job after DLT pipeline completes.
"""

from datetime import datetime
import os

# =============================================================================
# Configuration
# =============================================================================
EXPORT_PATH = "/mnt/export/sap"
TEMP_PATH = f"{EXPORT_PATH}/temp"
FINAL_FILENAME = f"finance_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# =============================================================================
# Read Gold Table
# =============================================================================
df_export = spark.table("gold_finance_export")

# Get record count for logging
record_count = df_export.count()
print(f"Exporting {record_count} records to CSV")

# =============================================================================
# Validate Data Before Export
# =============================================================================
if record_count == 0:
    print("WARNING: No records to export. Skipping file creation.")
    dbutils.notebook.exit("NO_DATA")

# Size check - warn if potentially too large
estimated_size_mb = record_count * 100 / 1024 / 1024  # Rough estimate
if estimated_size_mb > 1000:
    print(f"WARNING: Estimated file size {estimated_size_mb:.0f}MB may cause memory issues")
    print("Consider chunking export for large files")

# =============================================================================
# Export to Single CSV
# =============================================================================
print(f"Writing to temporary location: {TEMP_PATH}")

# Coalesce to single file (memory-intensive for large data)
df_export \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("nullValue", "") \
    .csv(TEMP_PATH)

# =============================================================================
# Rename Part File to Final Name
# =============================================================================
# Find the part file (Spark creates part-00000-xxx.csv)
files = dbutils.fs.ls(TEMP_PATH)
part_files = [f for f in files if f.name.startswith("part-") and f.name.endswith(".csv")]

if not part_files:
    raise Exception("No CSV part file found in output directory")

part_file = part_files[0]
final_path = f"{EXPORT_PATH}/{FINAL_FILENAME}"

print(f"Renaming {part_file.name} to {FINAL_FILENAME}")

# Move and rename
dbutils.fs.mv(part_file.path, final_path)

# Clean up temp directory
dbutils.fs.rm(TEMP_PATH, recurse=True)

# =============================================================================
# Verify Output
# =============================================================================
final_file_info = dbutils.fs.ls(EXPORT_PATH)
exported_file = [f for f in final_file_info if f.name == FINAL_FILENAME][0]

print("=" * 60)
print("EXPORT COMPLETE")
print("=" * 60)
print(f"File: {final_path}")
print(f"Size: {exported_file.size / 1024 / 1024:.2f} MB")
print(f"Records: {record_count}")
print("=" * 60)

# =============================================================================
# Optional: Archive source file after successful export
# =============================================================================
# source_files = dbutils.fs.ls("/mnt/landing/mainframe/")
# for f in source_files:
#     if f.name.startswith("trans_dump"):
#         archive_path = f"/mnt/archive/mainframe/{f.name}"
#         dbutils.fs.mv(f.path, archive_path)
#         print(f"Archived: {f.name}")
