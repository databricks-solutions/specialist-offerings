# Incremental Import Patterns: Sqoop → Databricks

## Sqoop Incremental: Append Mode

```bash
# Sqoop (append — new rows only)
sqoop import \
  --connect jdbc:mysql://host:3306/mydb \
  --table orders \
  --incremental append \
  --check-column order_id \
  --last-value 10000 \
  --target-dir /data/raw/orders \
  --append
```

### Databricks: MERGE INTO (Recommended)

```python
# Read new data from source
new_data = (spark.read.format("jdbc")
    .option("url", "jdbc:mysql://host:3306/mydb")
    .option("user", dbutils.secrets.get("scope", "db-user"))
    .option("password", dbutils.secrets.get("scope", "db-pass"))
    .option("query", "SELECT * FROM orders WHERE order_id > 10000")
    .load())

# Append to Delta table
new_data.write.mode("append").saveAsTable("main.raw.orders")
# Track last value in a checkpoint table or widget parameter
```

### Databricks: Auto Loader (if source writes files)

```python
# If the RDBMS exports to files, use Auto Loader
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/main/raw/_schemas/orders")
    .load("/Volumes/main/raw/orders_landing/"))

(df.writeStream
    .option("checkpointLocation", "/Volumes/main/raw/_checkpoints/orders")
    .trigger(availableNow=True)
    .toTable("main.raw.orders"))
```

## Sqoop Incremental: Last Modified Mode

```bash
# Sqoop (lastmodified — upsert based on timestamp)
sqoop import \
  --connect jdbc:mysql://host:3306/mydb \
  --table customers \
  --incremental lastmodified \
  --check-column updated_at \
  --last-value '2024-01-01 00:00:00' \
  --merge-key customer_id \
  --target-dir /data/raw/customers
```

### Databricks: Delta MERGE INTO

```python
# Read changed records from source
changes = (spark.read.format("jdbc")
    .option("url", "jdbc:mysql://host:3306/mydb")
    .option("user", dbutils.secrets.get("scope", "db-user"))
    .option("password", dbutils.secrets.get("scope", "db-pass"))
    .option("query",
        "SELECT * FROM customers WHERE updated_at > '2024-01-01 00:00:00'")
    .load())

# Create temp view for MERGE
changes.createOrReplaceTempView("changes")
```

```sql
-- Delta MERGE (upsert)
MERGE INTO main.raw.customers AS target
USING changes AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### Databricks: Full Pattern with Watermarking

```python
# Full incremental pipeline with watermark tracking
from pyspark.sql.functions import max as spark_max

# Get last watermark
try:
    last_watermark = spark.sql(
        "SELECT max(watermark_value) FROM main.meta.watermarks "
        "WHERE table_name = 'customers'"
    ).collect()[0][0]
except:
    last_watermark = "1970-01-01 00:00:00"

# Read incremental changes
changes = (spark.read.format("jdbc")
    .option("url", "jdbc:mysql://host:3306/mydb")
    .option("user", dbutils.secrets.get("scope", "db-user"))
    .option("password", dbutils.secrets.get("scope", "db-pass"))
    .option("query",
        f"SELECT * FROM customers WHERE updated_at > '{last_watermark}'")
    .load())

if changes.count() > 0:
    # MERGE
    changes.createOrReplaceTempView("changes")
    spark.sql("""
        MERGE INTO main.raw.customers AS target
        USING changes AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Update watermark
    new_watermark = changes.agg(spark_max("updated_at")).collect()[0][0]
    spark.sql(f"""
        MERGE INTO main.meta.watermarks AS target
        USING (SELECT 'customers' as table_name, '{new_watermark}' as watermark_value) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET watermark_value = source.watermark_value
        WHEN NOT MATCHED THEN INSERT *
    """)
```

## Scheduling Comparison

| Sqoop Pattern | Databricks Equivalent |
|---|---|
| Cron job running sqoop import | Databricks Job with schedule |
| Oozie coordinator triggering sqoop | Databricks Workflow with cron trigger |
| Daily full refresh | Scheduled job with `.mode("overwrite")` |
| Hourly incremental append | Scheduled job with MERGE INTO + watermark |
| Real-time CDC | Change Data Feed + Structured Streaming |
