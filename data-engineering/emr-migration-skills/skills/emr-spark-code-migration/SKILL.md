---
name: emr-spark-code-migration
description: "Convert PySpark and Scala Spark code from EMR to Databricks Runtime. Use when: (1) 'convert EMR Spark code', (2) 'EMR PySpark to Databricks', (3) 'Spark API differences EMR vs Databricks', (4) 'library not found on Databricks', (5) 'EMR code not working on Databricks', (6) migrating any Spark application code. Covers API changes, library mappings, and runtime differences."
---

# EMR Spark Code Migration to Databricks

## Overview

Most Apache Spark code is portable between EMR and Databricks because both run standard Spark. However, there are key differences in:

- **S3 access patterns**: EMR uses EMRFS and instance profiles natively; Databricks uses Unity Catalog external locations or instance profiles configured differently.
- **Library versions**: DBR bundles specific versions of Hadoop, Delta Lake, and common ML libraries that may differ from EMR.
- **Platform-specific APIs**: GlueContext, EMRFS consistent view, YARN configs, and bootstrap actions have no direct Databricks equivalents.
- **Pre-initialized sessions**: Databricks notebooks provide a pre-initialized `spark` session; you do not create your own SparkContext.
- **Delta Lake as default**: Databricks uses Delta Lake as its default table format, replacing raw Parquet/ORC workflows.

The goal of migration is not a line-by-line rewrite but adapting the code to leverage Databricks-native capabilities while preserving business logic.

## Critical Rules

1. **Always check runtime compatibility first.** Match your EMR release to the closest DBR LTS version before changing any code. See `runtime-compatibility.md`.
2. **Prefer Delta Lake over other formats.** Replace raw Parquet, ORC, or CSV writes with Delta tables wherever possible. Delta provides ACID transactions, time travel, and Z-ordering.
3. **Use Unity Catalog for data governance.** Replace Hive metastore references with Unity Catalog three-level namespace (`catalog.schema.table`). See the `emr-hive-to-unity-catalog` skill for detailed migration.
4. **Do not hardcode AWS credentials.** Use instance profiles, Unity Catalog external locations, or Databricks secret scopes instead of embedding access keys in Spark configs.
5. **Remove all YARN and Hadoop cluster manager configs.** Databricks manages its own cluster orchestration; YARN settings are ignored or cause errors.
6. **Remove GlueContext dependencies entirely.** Replace with native SparkSession operations. See the GlueContext Removal section below.
7. **Test with the target DBR version locally or in a dev workspace** before deploying to production.

## Quick Reference: Top 20 Common Changes

| # | EMR Pattern | Databricks Pattern |
|---|---|---|
| 1 | `s3://bucket/path` | Works, but prefer `abfss://container@account.dfs.core.windows.net/path` or Unity Catalog managed tables / external locations |
| 2 | `sc = SparkContext(conf)` | Use pre-initialized `spark` session directly; do not create a new SparkContext |
| 3 | `from awsglue.context import GlueContext` | Remove entirely; use `spark` (SparkSession) directly |
| 4 | `GlueContext(sc).create_dynamic_frame.from_catalog(...)` | `spark.table("catalog.schema.table")` or `spark.read.format(...).load(...)` |
| 5 | `spark.hadoop.fs.s3a.access.key` / `secret.key` | Use instance profile, Unity Catalog external locations, or `dbutils.secrets.get()` |
| 6 | `pip install` in EMR bootstrap action | `%pip install` in notebook cell, or cluster-level library, or `requirements.txt` in job config |
| 7 | EMRFS consistent view (`fs.s3.consistent`) | Not needed; Delta Lake provides ACID guarantees |
| 8 | `spark.yarn.queue`, `spark.yarn.executor.memoryOverhead` | Not applicable; remove all `spark.yarn.*` configs |
| 9 | Custom SerDe JARs via `--jars` | Install as cluster library, or upload to UC Volumes and reference in job config |
| 10 | `spark.sql("USE database_name")` (Hive) | `spark.sql("USE catalog.schema")` (Unity Catalog) |
| 11 | EMR Step API (boto3 `add_job_flow_steps`) | Databricks Jobs API or `databricks bundle deploy` |
| 12 | `spark.sparkContext.addPyFile("s3://...")` | `%pip install` or cluster library; for custom modules use Repos or UC Volumes |
| 13 | `--packages org.apache.hadoop:hadoop-aws:3.3.4` | Pre-installed on DBR; do not add explicitly (causes version conflicts) |
| 14 | CloudWatch custom metrics via `CW_NAMESPACE` | Databricks monitoring, Ganglia UI, or custom metrics via Log4j/Prometheus |
| 15 | `spark.read.parquet("s3://...")` then overwrite | `spark.read.table("catalog.schema.table")` with Delta; use `MERGE` for upserts |
| 16 | `--conf spark.serializer=org.apache.spark.serializer.KryoSerializer` | Works the same way; Kryo is supported on DBR |
| 17 | `spark.sql.sources.partitionOverwriteMode=dynamic` | Works the same way on DBR |
| 18 | `sc.textFile("s3://bucket/file.txt")` | `spark.read.text("s3://bucket/file.txt")` or use `dbutils.fs.head()` for quick inspection |
| 19 | Hive UDFs registered via `ADD JAR` | Upload JAR to UC Volumes, then `CREATE FUNCTION ... USING JAR` |
| 20 | EMR Managed Scaling / autoscaling | Databricks autoscaling (min/max workers) or Serverless compute |

## S3 Access Patterns

### Credential-Based Access (EMR)

```python
# EMR: Hardcoded credentials in Spark config (NOT recommended but common)
spark.conf.set("spark.hadoop.fs.s3a.access.key", "AKIA...")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "wJalr...")
df = spark.read.parquet("s3a://my-bucket/data/")
```

### Credential-Based Access (Databricks — Use Secret Scopes)

```python
# Databricks: Use secret scopes instead of hardcoded credentials
access_key = dbutils.secrets.get(scope="aws-secrets", key="access-key")
secret_key = dbutils.secrets.get(scope="aws-secrets", key="secret-key")
spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
df = spark.read.parquet("s3a://my-bucket/data/")
```

### Instance Profile Access (EMR)

```python
# EMR: Instance profile configured at cluster launch; no code changes needed
df = spark.read.parquet("s3://my-bucket/data/")
```

### Instance Profile Access (Databricks)

```python
# Databricks: Instance profile attached to cluster config
# In cluster config UI: AWS > Instance Profile > select the IAM role
# Then code is the same:
df = spark.read.parquet("s3://my-bucket/data/")
```

### Best Practice: Unity Catalog External Locations

```sql
-- Databricks: Register the S3 path as an external location in Unity Catalog
CREATE EXTERNAL LOCATION my_s3_location
URL 's3://my-bucket/data/'
WITH (STORAGE CREDENTIAL my_aws_credential);

-- Then use it as a managed or external table:
CREATE TABLE catalog.schema.my_table
USING DELTA
LOCATION 's3://my-bucket/data/delta_table';
```

```python
# Then simply read via Unity Catalog:
df = spark.table("catalog.schema.my_table")
```

### Cross-Account Access

On EMR, cross-account S3 access typically uses an assumed IAM role via STS. On Databricks, the equivalent is a Unity Catalog storage credential backed by an IAM role with a trust policy for the Databricks-managed role.

## Library Installation

| EMR Method | Databricks Equivalent |
|---|---|
| Bootstrap action (`pip install ...`) | Cluster init script, or cluster library config |
| `--packages` in spark-submit | Cluster Maven library, or `%pip install` for Python |
| `--jars` in spark-submit | Cluster library (JAR), or upload to UC Volumes |
| `--py-files` in spark-submit | `%pip install` from wheel, or Databricks Repos |
| Conda environment via bootstrap | Cluster-level conda environment (DBR ML runtimes) |
| EMR Applications list (Hive, Presto, etc.) | Built into DBR; SQL Warehouse for Presto-like queries |

### Example: Bootstrap Action to Databricks

**EMR bootstrap action:**
```bash
#!/bin/bash
sudo pip3 install pandas==2.0.3 boto3 pyarrow
sudo yum install -y libpq-dev
```

**Databricks equivalent (notebook):**
```python
%pip install pandas==2.0.3 boto3 pyarrow psycopg2-binary
```

**Databricks equivalent (job config / requirements.txt):**
```text
pandas==2.0.3
boto3
pyarrow
psycopg2-binary
```

## GlueContext Removal

AWS Glue ETL jobs use `GlueContext` and `DynamicFrame` which are AWS-proprietary. These must be replaced with native Spark equivalents.

### Before (EMR/Glue):

```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database', 'table_name'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from Glue Data Catalog
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=args['database'],
    table_name=args['table_name']
)

# Apply mapping
mapped = dynamic_frame.apply_mapping([
    ("col1", "string", "column_one", "string"),
    ("col2", "long", "column_two", "int")
])

# Write back
glueContext.write_dynamic_frame.from_options(
    frame=mapped,
    connection_type="s3",
    connection_options={"path": "s3://output-bucket/result/"},
    format="parquet"
)

job.commit()
```

### After (Databricks):

```python
# spark session is pre-initialized in Databricks notebooks
# For jobs, use: from pyspark.sql import SparkSession; spark = SparkSession.builder.getOrCreate()

# Parameters via widgets (notebooks) or job parameters
database = dbutils.widgets.get("database")         # or spark.conf.get("spark.databricks.job.param.database")
table_name = dbutils.widgets.get("table_name")

# Read from Unity Catalog (replaces Glue Data Catalog)
df = spark.table(f"{database}.{table_name}")

# Apply column transformations (replaces apply_mapping)
from pyspark.sql.functions import col
mapped = df.select(
    col("col1").alias("column_one").cast("string"),
    col("col2").alias("column_two").cast("int")
)

# Write as Delta table (replaces raw Parquet to S3)
mapped.write.mode("overwrite").saveAsTable("catalog.schema.result_table")

# Or if you must write to S3:
# mapped.write.mode("overwrite").format("delta").save("s3://output-bucket/result/")
```

### Key DynamicFrame Replacements

| GlueContext / DynamicFrame | Native Spark Equivalent |
|---|---|
| `create_dynamic_frame.from_catalog(db, table)` | `spark.table("catalog.schema.table")` |
| `create_dynamic_frame.from_options(s3, format)` | `spark.read.format(fmt).load(path)` |
| `dynamic_frame.toDF()` | Already a DataFrame |
| `DynamicFrame.fromDF(df, glueContext, name)` | Just use the DataFrame directly |
| `apply_mapping([...])` | `df.select(col("x").alias("y").cast("type"))` |
| `resolveChoice(specs=[...])` | `df.withColumn("col", col("col").cast("target_type"))` |
| `write_dynamic_frame.from_options(...)` | `df.write.format(...).save(...)` or `df.write.saveAsTable(...)` |
| `glueContext.purge_s3_path(...)` | `dbutils.fs.rm(path, recurse=True)` |
| `job.commit()` | Not needed; no equivalent required |

## Common Errors Table

| Error on Databricks | Cause | Solution |
|---|---|---|
| `java.lang.ClassNotFoundException: com.amazonaws.services.glue...` | Glue libraries not available on DBR | Remove all `awsglue` imports; use native Spark |
| `No FileSystem for scheme: s3` | `s3://` URI scheme not configured | Use `s3a://` or configure instance profile; or use UC paths |
| `java.lang.NoSuchMethodError: com.google.common.base.Preconditions...` | Guava version conflict | Remove explicit Guava dependency; use DBR bundled version |
| `org.apache.hadoop.fs.s3a.auth.NoAuthWithAWSException` | No S3 credentials configured | Attach instance profile to cluster or configure UC external location |
| `AnalysisException: Table or view not found` | Hive metastore table not in Unity Catalog | Register table in Unity Catalog or use external location path |
| `Py4JJavaError: ... IllegalArgumentException: Wrong FS: s3a://...` | Mixed S3 URI schemes | Standardize on `s3a://` or `s3://` consistently |
| `ModuleNotFoundError: No module named 'somelib'` | Library not installed on cluster | Use `%pip install somelib` or add to cluster libraries |
| `java.lang.NoClassDefFoundError: org/apache/hadoop/fs/s3a/S3AFileSystem` | hadoop-aws JAR missing or wrong version | Do not add hadoop-aws manually; it is bundled with DBR |
| `WARN: Ignoring spark.yarn.* configuration` | YARN configs have no effect on DBR | Remove all `spark.yarn.*` settings from Spark config |
| `UnsupportedOperationException: Delta requires ...` | Writing to a Delta path without Delta runtime | Ensure you are on DBR (not open-source Spark); Delta is built in |

## Related Skills

- **emr-hive-to-unity-catalog**: Detailed migration of Hive metastore schemas, tables, and permissions to Unity Catalog.
- **emr-config-migration**: Translation of EMR cluster configurations, Spark defaults, and YARN settings to Databricks equivalents.
- **emr-streaming-migration**: Migration of Spark Streaming and Structured Streaming jobs from EMR to Databricks, including Kinesis-to-Event-Hubs conversion.
