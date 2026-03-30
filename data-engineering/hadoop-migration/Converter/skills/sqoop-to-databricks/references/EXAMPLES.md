# Examples: Sqoop → Databricks

## Example 1: Full Table Import

### Before (Sqoop)
```bash
sqoop import \
  --connect jdbc:mysql://mysql-prod:3306/ecommerce \
  --username etl_user \
  --password etl_pass123 \
  --table products \
  --num-mappers 4 \
  --as-parquetfile \
  --target-dir /data/warehouse/products \
  --delete-target-dir
```

### After (Databricks Notebook)
```python
# Setup: Store credentials in a secret scope
# databricks secrets create-scope --scope jdbc-prod
# databricks secrets put --scope jdbc-prod --key mysql-user
# databricks secrets put --scope jdbc-prod --key mysql-pass

df = (spark.read.format("jdbc")
    .option("url", "jdbc:mysql://mysql-prod:3306/ecommerce")
    .option("user", dbutils.secrets.get("jdbc-prod", "mysql-user"))
    .option("password", dbutils.secrets.get("jdbc-prod", "mysql-pass"))
    .option("dbtable", "products")
    .option("numPartitions", 4)
    .load())

df.write.mode("overwrite").saveAsTable("main.warehouse.products")
```

### After (Lakehouse Federation — even simpler)
```sql
-- One-time setup
CREATE CONNECTION mysql_prod TYPE mysql
OPTIONS (
    host = 'mysql-prod',
    port = '3306',
    user = secret('jdbc-prod', 'mysql-user'),
    password = secret('jdbc-prod', 'mysql-pass')
);
CREATE FOREIGN CATALOG ecommerce USING CONNECTION mysql_prod;

-- Then just query or snapshot:
CREATE OR REPLACE TABLE main.warehouse.products
AS SELECT * FROM ecommerce.ecommerce.products;
```

## Example 2: Incremental Import with Merge Key

### Before (Sqoop script run by cron)
```bash
#!/bin/bash
LAST_VALUE=$(cat /opt/sqoop/state/customers_last_ts.txt)

sqoop import \
  --connect jdbc:postgresql://pg-host:5432/crm \
  --username sync_user \
  --password-file hdfs:///secure/pg_password \
  --table customers \
  --incremental lastmodified \
  --check-column updated_at \
  --last-value "$LAST_VALUE" \
  --merge-key customer_id \
  --target-dir /data/raw/customers \
  --num-mappers 8

# Update state file
date +"%Y-%m-%d %H:%M:%S" > /opt/sqoop/state/customers_last_ts.txt
```

### After (Databricks Notebook — scheduled as Job)
```python
from pyspark.sql.functions import max as spark_max

# Read watermark
watermark_df = spark.sql(
    "SELECT watermark_value FROM main.meta.watermarks "
    "WHERE table_name = 'customers'"
)
last_value = watermark_df.collect()[0][0] if watermark_df.count() > 0 else "1970-01-01"

# Read incremental changes
changes = (spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://pg-host:5432/crm")
    .option("user", dbutils.secrets.get("jdbc-prod", "pg-user"))
    .option("password", dbutils.secrets.get("jdbc-prod", "pg-pass"))
    .option("query",
        f"SELECT * FROM customers WHERE updated_at > '{last_value}'")
    .option("numPartitions", 8)
    .option("partitionColumn", "customer_id")
    .option("lowerBound", 1)
    .option("upperBound", 10000000)
    .load())

if changes.count() > 0:
    changes.createOrReplaceTempView("customer_changes")

    spark.sql("""
        MERGE INTO main.raw.customers AS target
        USING customer_changes AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Update watermark
    new_ts = changes.agg(spark_max("updated_at")).collect()[0][0]
    spark.sql(f"""
        MERGE INTO main.meta.watermarks AS t
        USING (SELECT 'customers' AS table_name, '{new_ts}' AS watermark_value) AS s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET watermark_value = s.watermark_value
        WHEN NOT MATCHED THEN INSERT *
    """)
```

## Example 3: Sqoop Export

### Before (Sqoop)
```bash
sqoop export \
  --connect jdbc:mysql://mysql-prod:3306/reporting \
  --username rpt_user \
  --password rpt_pass \
  --table daily_kpis \
  --export-dir /data/processed/daily_kpis \
  --input-fields-terminated-by '\001' \
  --update-key report_date \
  --update-mode allowinsert \
  --num-mappers 4
```

### After (Databricks)
```python
df = spark.table("main.analytics.daily_kpis")

(df.write.format("jdbc")
    .option("url", "jdbc:mysql://mysql-prod:3306/reporting")
    .option("user", dbutils.secrets.get("jdbc-prod", "rpt-user"))
    .option("password", dbutils.secrets.get("jdbc-prod", "rpt-pass"))
    .option("dbtable", "daily_kpis")
    .option("numPartitions", 4)
    .mode("overwrite")  # or "append" based on use case
    .save())
# Note: For update-mode allowinsert, use a staging table + SQL MERGE on the target DB
```
