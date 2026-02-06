"""
Auto Loader: Fixed-Width File Parsing
=====================================

WHAT THIS DOES:
- Reads fixed-width/positional files (common from mainframes, legacy systems)
- Parses fields using substring based on column positions
- Handles type conversions (string → date, double, etc.)

REPLACES IN INFORMATICA:
- Flat File Source with fixed-width column definitions
- Expression transformation for type conversions
- Normalizer for complex parsing

WHEN TO USE:
- Mainframe extracts (COBOL copybook layouts)
- Legacy system dumps with positional fields
- Bank/insurance industry transaction files

FILE FORMAT EXAMPLE:
    Position 1-10:  txn_id (string)
    Position 11-20: amount (numeric, 2 decimal places)
    Position 21-28: txn_date (YYYYMMDD format)

IMPORTANT:
- substring() is 1-based (first character is position 1)
- Define your field positions based on source documentation
- Handle NULL/empty values with coalesce() or when() expressions
"""

from pyspark.sql.functions import col, substring, to_date, trim, when

# Read fixed-width file as raw text lines
df_raw = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "text") \
    .option("cloudFiles.schemaLocation", "/mnt/schemas/transactions") \
    .load("/mnt/landing/trans_dump*.dat")

# Parse fields using substring with defined byte positions
# Adjust positions based on your actual file layout
df_parsed = df_raw.select(
    trim(substring(col("value"), 1, 10)).alias("txn_id"),           # Chars 1-10
    substring(col("value"), 11, 10).cast("double").alias("amount"), # Chars 11-20
    to_date(substring(col("value"), 21, 8), "yyyyMMdd").alias("txn_date")  # Chars 21-28
)

# Optional: Add data quality filter
df_clean = df_parsed.where("txn_id IS NOT NULL AND txn_id != ''")

df_clean.writeStream \
    .option("checkpointLocation", "/mnt/checkpoints/bronze_txn") \
    .table("bronze_transactions")
