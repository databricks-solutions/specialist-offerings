"""
Auto Loader: Incremental CSV Ingestion to Bronze Layer
=======================================================

WHAT THIS DOES:
- Incrementally processes new CSV files as they arrive in cloud storage
- Automatically infers and evolves schema as source files change
- Provides exactly-once processing guarantees via checkpointing

REPLACES IN INFORMATICA:
- File Watcher + Source Definition + Session Scheduling
- Manual schema management in Designer
- Complex workflow triggers for new file detection

WHEN TO USE:
- Landing zone receives CSV/JSON/Parquet files from upstream systems
- Need automatic schema detection and evolution
- Want incremental processing without tracking processed files manually

KEY OPTIONS:
- cloudFiles.format: csv, json, parquet, avro, text, binary
- cloudFiles.schemaLocation: Where to persist inferred schema (required for production)
- cloudFiles.inferColumnTypes: Auto-detect data types (true/false)
- trigger(availableNow=True): Process all available files then stop (micro-batch)
- trigger(processingTime='5 minutes'): Continuous streaming with interval

BEST PRACTICES:
- Always set schemaLocation for production workloads
- Use checkpointLocation on reliable storage (not local)
- Enable inferColumnTypes for automatic type detection
- Use availableNow=True for scheduled batch jobs
"""

# Stream new CSV files from landing zone
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/mnt/schemas/sales") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("header", "true") \
    .load("/mnt/landing/sales/*.csv")

# Write to Bronze table with checkpoint for exactly-once semantics
df.writeStream \
    .option("checkpointLocation", "/mnt/checkpoints/bronze_sales") \
    .trigger(availableNow=True)  # Process available files, then stop
    .table("bronze_sales")
