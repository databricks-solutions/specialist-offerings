# Sqoop → Databricks JDBC Patterns

## Basic Import

```bash
# Sqoop
sqoop import \
  --connect jdbc:mysql://host:3306/mydb \
  --username dbuser \
  --password dbpass \
  --table customers \
  --target-dir /data/raw/customers \
  --num-mappers 4
```

```python
# Databricks — Option 1: Spark JDBC
df = (spark.read.format("jdbc")
    .option("url", "jdbc:mysql://host:3306/mydb")
    .option("user", dbutils.secrets.get("scope", "db-user"))
    .option("password", dbutils.secrets.get("scope", "db-pass"))
    .option("dbtable", "customers")
    .option("numPartitions", 4)
    .load())

df.write.mode("overwrite").saveAsTable("main.raw.customers")
```

```sql
-- Databricks — Option 2: Lakehouse Federation (no code needed)
CREATE CONNECTION mysql_conn TYPE mysql
OPTIONS (
    host = 'host',
    port = '3306',
    user = secret('scope', 'db-user'),
    password = secret('scope', 'db-pass')
);

CREATE FOREIGN CATALOG mysql_catalog USING CONNECTION mysql_conn;

-- Then query directly:
SELECT * FROM mysql_catalog.mydb.customers;
-- Or create a managed copy:
CREATE TABLE main.raw.customers AS SELECT * FROM mysql_catalog.mydb.customers;
```

## Import with Query (Free-Form)

```bash
# Sqoop
sqoop import \
  --connect jdbc:oracle:thin:@host:1521:orcl \
  --username user \
  --password pass \
  --query 'SELECT id, name, amount FROM orders WHERE status = "active" AND $CONDITIONS' \
  --split-by id \
  --target-dir /data/raw/active_orders \
  --num-mappers 8
```

```python
# Databricks
df = (spark.read.format("jdbc")
    .option("url", "jdbc:oracle:thin:@host:1521:orcl")
    .option("user", dbutils.secrets.get("scope", "oracle-user"))
    .option("password", dbutils.secrets.get("scope", "oracle-pass"))
    .option("query", "SELECT id, name, amount FROM orders WHERE status = 'active'")
    .option("partitionColumn", "id")
    .option("lowerBound", 1)
    .option("upperBound", 10000000)
    .option("numPartitions", 8)
    .load())

df.write.mode("overwrite").saveAsTable("main.raw.active_orders")
```

## Import to Hive

```bash
# Sqoop
sqoop import \
  --connect jdbc:mysql://host:3306/mydb \
  --table products \
  --hive-import \
  --hive-database staging \
  --hive-table products \
  --hive-overwrite \
  --num-mappers 4
```

```python
# Databricks
df = (spark.read.format("jdbc")
    .option("url", "jdbc:mysql://host:3306/mydb")
    .option("user", dbutils.secrets.get("scope", "db-user"))
    .option("password", dbutils.secrets.get("scope", "db-pass"))
    .option("dbtable", "products")
    .option("numPartitions", 4)
    .load())

df.write.mode("overwrite").saveAsTable("main.staging.products")
```

## Export

```bash
# Sqoop
sqoop export \
  --connect jdbc:mysql://host:3306/mydb \
  --table customer_scores \
  --export-dir /data/processed/scores \
  --input-fields-terminated-by ',' \
  --num-mappers 4
```

```python
# Databricks
df = spark.table("main.processed.customer_scores")
# Or: df = spark.read.csv("/Volumes/main/processed/scores/")

(df.write.format("jdbc")
    .option("url", "jdbc:mysql://host:3306/mydb")
    .option("user", dbutils.secrets.get("scope", "db-user"))
    .option("password", dbutils.secrets.get("scope", "db-pass"))
    .option("dbtable", "customer_scores")
    .option("numPartitions", 4)
    .mode("overwrite")
    .save())
```

## Parameter Mapping

| Sqoop Flag | Spark JDBC Equivalent |
|---|---|
| `--connect <url>` | `.option("url", "<url>")` |
| `--username <user>` | `.option("user", dbutils.secrets.get(...))` |
| `--password <pass>` | `.option("password", dbutils.secrets.get(...))` |
| `--table <table>` | `.option("dbtable", "<table>")` |
| `--query '<sql> AND $CONDITIONS'` | `.option("query", "<sql>")` |
| `--split-by <col>` | `.option("partitionColumn", "<col>")` |
| `--num-mappers N` | `.option("numPartitions", N)` |
| `--boundary-query` | `.option("lowerBound", ...).option("upperBound", ...)` |
| `--columns <cols>` | Use SELECT in query |
| `--where <clause>` | Use WHERE in query or `.option("query", ...)` |
| `--target-dir <path>` | `.saveAsTable()` or `.write.parquet()` |
| `--hive-import` | `.write.saveAsTable()` |
| `--as-parquetfile` | Delta (default) or `.write.parquet()` |
| `--compress` | Delta compression (automatic) |
| `--fetch-size N` | `.option("fetchsize", N)` |
