"""
JDBC: Incremental Load with High-Watermark
==========================================

WHAT THIS DOES:
- Loads only new/changed records since last run
- Uses timestamp or sequence column as watermark
- Appends new records to Bronze table

REPLACES IN INFORMATICA:
- Session with incremental extraction
- Mapping variable for last run timestamp
- Lookup to find max processed value

WHEN TO USE:
- Daily/hourly incremental loads
- Source has reliable timestamp or sequence column
- Don't need to capture DELETEs (use CDC for that)

WATERMARK COLUMN OPTIONS:
- LAST_UPDATED: Timestamp when row was modified
- CREATED_DATE: Timestamp when row was created (inserts only)
- SEQUENCE_ID: Auto-increment ID (inserts only, not updates)

IMPORTANT:
- This pattern does NOT capture DELETEs - use CDC for that
- Ensure watermark column is indexed on source for performance
- Handle late-arriving data with buffer (e.g., -1 hour)
"""

from pyspark.sql.functions import max as spark_max

# Get high-watermark: max timestamp from last load
max_timestamp = spark.sql("""
    SELECT COALESCE(MAX(last_updated), '1900-01-01') as max_ts
    FROM bronze_customer
""").collect()[0]["max_ts"]

print(f"Loading records updated after: {max_timestamp}")

# Extract only records newer than watermark
df_incremental = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("query", f"""
        SELECT * FROM CUSTOMER
        WHERE LAST_UPDATED > TIMESTAMP '{max_timestamp}'
    """) \
    .option("numPartitions", "10") \
    .load()

record_count = df_incremental.count()
print(f"Found {record_count} new/updated records")

# Append new records to Bronze
if record_count > 0:
    df_incremental.write \
        .mode("append") \
        .saveAsTable("bronze_customer")
    print(f"Loaded {record_count} records to bronze_customer")
else:
    print("No new records to load")

# --- Alternative: With buffer for late-arriving data ---
# query = f"""
#     SELECT * FROM CUSTOMER
#     WHERE LAST_UPDATED > TIMESTAMP '{max_timestamp}' - INTERVAL '1' HOUR
# """
