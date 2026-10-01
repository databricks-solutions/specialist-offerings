# Common Migration Patterns: Hadoop to Databricks

## HDFS Path Migration

### Path Mapping Rules

| Hadoop Path | Databricks Equivalent | Notes |
|---|---|---|
| `hdfs:///user/hive/warehouse/` | Unity Catalog managed tables | No explicit path needed |
| `hdfs:///data/raw/` | `/Volumes/<catalog>/<schema>/raw/` | UC Volumes for file access |
| `hdfs:///tmp/` | `/Volumes/<catalog>/<schema>/tmp/` | Or DBFS: `dbfs:/tmp/` |
| `hdfs:///user/<name>/` | `/Volumes/<catalog>/<schema>/user_data/` | Per-user data |
| `s3://bucket/path` | `s3://bucket/path` (external location) | Register as UC external location |

### Code Pattern

```python
# Before (Hadoop)
df = spark.read.parquet("hdfs:///data/raw/events/")

# After (Databricks with UC Volumes)
df = spark.read.parquet("/Volumes/main/default/raw/events/")

# After (Databricks with external location)
df = spark.read.parquet("s3://datalake-bucket/raw/events/")
```

## Authentication Migration

### Kerberos to Unity Catalog

| Hadoop Auth | Databricks Auth |
|---|---|
| Kerberos keytab | Service principal / OAuth M2M |
| `hadoop.security.authentication=kerberos` | Workspace-level identity federation |
| HDFS ACLs | UC grants: `GRANT SELECT ON TABLE ...` |
| Sentry policies | UC privileges + row/column filters |

### JDBC Credentials

```python
# Before (Hadoop — password in config)
spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://host:3306/db") \
    .option("user", "myuser") \
    .option("password", "mypass")

# After (Databricks — secret scope)
spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://host:3306/db") \
    .option("user", dbutils.secrets.get("scope", "jdbc-user")) \
    .option("password", dbutils.secrets.get("scope", "jdbc-pass"))

# After (Databricks — Lakehouse Federation, no credentials in code)
df = spark.table("mysql_catalog.mydb.customers")
```

## Configuration Migration

### Spark Configuration

| Hadoop Config | Databricks Equivalent |
|---|---|
| `spark.master` | Not needed (managed by Databricks) |
| `spark.submit.deployMode` | Not needed (always cluster mode) |
| `spark.yarn.queue` | Cluster policies / Serverless |
| `spark.executor.memory` | Cluster auto-scaling handles this |
| `spark.executor.instances` | Auto-scaling; use `spark.databricks.adaptive.*` |
| `spark.hadoop.fs.defaultFS` | Not needed; use UC paths |
| `spark.hive.metastore.uris` | Unity Catalog (automatic) |

### Hive Configuration

| Hadoop Config | Databricks Equivalent |
|---|---|
| `hive.metastore.uris` | Unity Catalog (built-in) |
| `hive.exec.dynamic.partition=true` | Default behavior in Databricks SQL |
| `hive.exec.dynamic.partition.mode=nonstrict` | Default in Databricks |
| `hive.support.concurrency=true` | Delta Lake handles concurrency |
| `hive.txn.manager` | Delta Lake ACID (automatic) |

## File Format Migration

| Hadoop Format | Databricks Recommendation |
|---|---|
| ORC | Delta Lake (convert with `CONVERT TO DELTA`) |
| Parquet | Delta Lake or keep Parquet with UC |
| Avro | Delta Lake (use `spark.read.format("avro")` then write Delta) |
| Text/CSV | Delta Lake or keep as CSV in Volumes |
| SequenceFile | Read with Spark, write as Delta |
| RCFile | Read with Spark, write as Delta |

## Data Type Mapping

| Hive Type | Databricks SQL Type | Notes |
|---|---|---|
| `STRING` | `STRING` | Same |
| `INT` | `INT` | Same |
| `BIGINT` | `BIGINT` | Same |
| `DOUBLE` | `DOUBLE` | Same |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | Same |
| `TIMESTAMP` | `TIMESTAMP` | Same |
| `DATE` | `DATE` | Same |
| `BOOLEAN` | `BOOLEAN` | Same |
| `BINARY` | `BINARY` | Same |
| `ARRAY<T>` | `ARRAY<T>` | Same |
| `MAP<K,V>` | `MAP<K,V>` | Same |
| `STRUCT<...>` | `STRUCT<...>` | Same |
| `UNIONTYPE<...>` | `STRUCT<...>` | Flatten to struct |
