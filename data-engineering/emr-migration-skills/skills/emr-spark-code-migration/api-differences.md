# API Differences: EMR vs Databricks Runtime

This document catalogs the API-level differences between running Spark on Amazon EMR and on Databricks Runtime (DBR). It is organized by functional category.

## SparkContext / SparkSession Differences

### EMR: Explicit SparkContext Creation

On EMR, scripts submitted via `spark-submit` or EMR Steps typically create their own SparkContext and SparkSession:

```python
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf() \
    .setAppName("MyEMRJob") \
    .setMaster("yarn") \
    .set("spark.executor.memory", "4g") \
    .set("spark.executor.cores", "2")

sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
```

### Databricks: Pre-Initialized Session

In Databricks notebooks, `spark`, `sc`, `sqlContext`, and `dbutils` are pre-initialized. Creating a new SparkContext will fail because only one can exist per JVM.

```python
# spark is already available — just use it
# sc = spark.sparkContext  (if you need the SparkContext object)

# For Databricks Jobs (non-notebook scripts), use getOrCreate:
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# Do NOT set master("yarn") or master("local") — Databricks manages this
```

### Scala Equivalent

```scala
// EMR
val spark = SparkSession.builder()
  .appName("MyEMRJob")
  .master("yarn")
  .config("spark.executor.memory", "4g")
  .enableHiveSupport()
  .getOrCreate()

// Databricks (notebooks): spark is pre-initialized
// Databricks (jobs):
val spark = SparkSession.builder()
  .appName("MyDBRJob")
  // Do NOT set .master() — Databricks handles it
  .getOrCreate()
```

### Key Differences

| Aspect | EMR | Databricks |
|---|---|---|
| `master()` setting | `"yarn"` or `"local[*]"` | Do not set; managed by platform |
| `enableHiveSupport()` | Needed for Hive metastore | Not needed; Unity Catalog is default |
| Multiple SparkContexts | Not supported (same as DBR) | Not supported |
| `spark-submit` | Full support | Supported for JAR/Python jobs, but notebooks are preferred |
| `SparkContext.stop()` | Common in scripts | Avoid in notebooks; will break the session |

## File System APIs

### EMR: Hadoop FileSystem API

```python
# EMR: Direct Hadoop FS operations
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# Using Hadoop API
hadoop_conf = sc._jsc.hadoopConfiguration()
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
    sc._jvm.java.net.URI("s3://my-bucket"), hadoop_conf
)
status = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path("s3://my-bucket/data/"))
for file_status in status:
    print(file_status.getPath().toString())
```

### Databricks: dbutils.fs

```python
# Databricks: Use dbutils.fs for file operations
files = dbutils.fs.ls("s3://my-bucket/data/")
for f in files:
    print(f.path, f.size)

# Other dbutils.fs operations:
dbutils.fs.head("s3://my-bucket/data/file.txt", 1000)  # Read first 1000 bytes
dbutils.fs.cp("s3://source/path", "s3://dest/path", recurse=True)
dbutils.fs.rm("s3://my-bucket/temp/", recurse=True)
dbutils.fs.mkdirs("s3://my-bucket/new-dir/")
dbutils.fs.put("/tmp/myfile.txt", "file contents", overwrite=True)

# Hadoop FS API also works on Databricks if needed:
hadoop_fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
```

### Key Differences

| Operation | EMR (Hadoop FS) | Databricks (dbutils.fs) |
|---|---|---|
| List files | `fs.listStatus(Path(...))` | `dbutils.fs.ls(path)` |
| Read file head | `fs.open(Path(...)).read(...)` | `dbutils.fs.head(path, maxBytes)` |
| Copy files | `FileUtil.copy(...)` | `dbutils.fs.cp(src, dst)` |
| Delete files | `fs.delete(Path(...), recursive)` | `dbutils.fs.rm(path, recurse)` |
| Create directory | `fs.mkdirs(Path(...))` | `dbutils.fs.mkdirs(path)` |
| File exists | `fs.exists(Path(...))` | Try/except on `dbutils.fs.ls()` |
| Mount storage | N/A (direct S3 access) | `dbutils.fs.mount(source, mountPoint)` (legacy; use UC external locations) |

## S3 Access Patterns

### URI Schemes

| Scheme | EMR Behavior | Databricks Behavior |
|---|---|---|
| `s3://` | EMRFS (recommended on EMR, supports consistent view) | Works if instance profile is configured; maps to `s3a://` internally |
| `s3a://` | Standard Hadoop S3A connector | Fully supported; preferred for direct S3 access |
| `s3n://` | Legacy, deprecated | Not supported; rewrite to `s3a://` |

### EMR EMRFS Configuration (Remove on Databricks)

```python
# EMR-specific EMRFS configs — REMOVE these on Databricks
spark.conf.set("spark.hadoop.fs.s3.consistent", "true")
spark.conf.set("spark.hadoop.fs.s3.consistent.retryPeriodSeconds", "10")
spark.conf.set("spark.hadoop.fs.s3.consistent.retryCount", "5")
spark.conf.set("spark.hadoop.fs.s3.consistent.metadata.tableName", "EmrFSMetadata")
# These configs are ignored on Databricks and can cause warnings
```

### S3A Configuration Differences

```python
# EMR: S3A with endpoint and path-style access
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

# Databricks: Usually not needed — Databricks auto-configures S3A
# Only set endpoint if accessing a non-standard S3-compatible store (e.g., MinIO)
```

### S3 Server-Side Encryption

```python
# EMR
spark.conf.set("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
spark.conf.set("spark.hadoop.fs.s3a.server-side-encryption.key", "arn:aws:kms:...")

# Databricks: Same configs work, but preferred approach is UC external location with encryption config
```

## Hive Integration Differences

### EMR: Direct Hive Metastore

```python
# EMR: enableHiveSupport() connects to the local or remote Hive metastore
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sql("SHOW DATABASES").show()
spark.sql("USE my_database")
df = spark.sql("SELECT * FROM my_table")
```

### Databricks: Unity Catalog

```python
# Databricks: Unity Catalog three-level namespace
spark.sql("SHOW CATALOGS").show()
spark.sql("USE CATALOG my_catalog")
spark.sql("USE SCHEMA my_schema")
df = spark.sql("SELECT * FROM my_catalog.my_schema.my_table")

# Or shorthand:
df = spark.table("my_catalog.my_schema.my_table")
```

### Catalog API Differences

```python
# EMR: List tables in Hive metastore
spark.catalog.listDatabases()
spark.catalog.listTables("my_database")
spark.catalog.tableExists("my_database.my_table")

# Databricks: Same API works, but with Unity Catalog namespace
spark.catalog.listDatabases()  # Lists schemas in current catalog
spark.catalog.listTables("my_schema")
spark.catalog.tableExists("my_catalog.my_schema.my_table")

# Databricks-specific catalog operations:
spark.sql("SHOW CATALOGS")
spark.sql("SHOW SCHEMAS IN my_catalog")
spark.sql("SHOW TABLES IN my_catalog.my_schema")
spark.sql("DESCRIBE TABLE EXTENDED my_catalog.my_schema.my_table")
```

## Serialization (Kryo Configuration)

Kryo serialization works the same on both platforms. The configuration is portable:

```python
# Works on both EMR and Databricks
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryo.registrationRequired", "false")

# Register custom classes (same on both):
spark.conf.set("spark.kryo.classesToRegister",
    "com.mycompany.MyClass1,com.mycompany.MyClass2")
```

### Difference: Default Serializer

| Platform | Default Serializer |
|---|---|
| EMR | Java serialization |
| Databricks | Java serialization (same default, but Photon engine bypasses serialization for many operations) |

Photon, the Databricks native query engine, uses its own columnar format internally. Kryo settings primarily affect RDD operations and shuffle of non-SQL workloads.

## UDF Registration Differences

### Python UDFs

```python
# Works identically on both EMR and Databricks
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def my_udf(value):
    return value.upper()

df = df.withColumn("upper_col", my_udf(df["col"]))

# SQL registration also works the same:
spark.udf.register("my_sql_udf", lambda x: x.upper(), StringType())
spark.sql("SELECT my_sql_udf(col) FROM table")
```

### Databricks-Specific: Pandas UDFs (Arrow-Optimized)

```python
# Works on both, but Databricks has better Arrow optimization
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("string")
def pandas_upper(s: pd.Series) -> pd.Series:
    return s.str.upper()

df = df.withColumn("upper_col", pandas_upper(df["col"]))
```

### Scala UDFs

```scala
// Works the same on both EMR and Databricks
import org.apache.spark.sql.functions.udf

val myUdf = udf((s: String) => s.toUpperCase)
val result = df.withColumn("upper_col", myUdf($"col"))

// SQL registration:
spark.udf.register("my_sql_udf", (s: String) => s.toUpperCase)
```

### Java/Scala UDF JARs

```python
# EMR: Load UDF JAR via spark-submit --jars or sc.addJar
sc.addJar("s3://my-bucket/jars/my-udfs.jar")
spark.sql("CREATE TEMPORARY FUNCTION my_func AS 'com.mycompany.MyUDF'")

# Databricks: Install JAR as cluster library or via UC Volumes
spark.sql("""
    CREATE FUNCTION my_catalog.my_schema.my_func
    AS 'com.mycompany.MyUDF'
    USING JAR '/Volumes/my_catalog/my_schema/jars/my-udfs.jar'
""")
```

## Catalog API Differences

### EMR: Hive Catalog API

```python
# EMR: Standard Spark Catalog API against Hive Metastore
spark.catalog.listDatabases()
spark.catalog.setCurrentDatabase("my_db")
spark.catalog.listTables("my_db")
spark.catalog.listColumns("my_db", "my_table")
spark.catalog.isCached("my_table")
spark.catalog.cacheTable("my_table")
spark.catalog.refreshTable("my_table")
```

### Databricks: Extended Catalog API

```python
# Databricks: Same API plus Unity Catalog SQL commands
spark.catalog.listDatabases()  # Works, lists schemas in current catalog
spark.catalog.listTables("my_schema")

# Unity Catalog specific operations (SQL-based):
spark.sql("SHOW CATALOGS")
spark.sql("SHOW GRANTS ON TABLE my_catalog.my_schema.my_table")
spark.sql("ALTER TABLE my_catalog.my_schema.my_table SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
spark.sql("DESCRIBE HISTORY my_catalog.my_schema.my_table")  # Delta time travel
spark.sql("OPTIMIZE my_catalog.my_schema.my_table ZORDER BY (col1, col2)")
```

## Streaming API Differences

Most Structured Streaming code is portable. Key differences:

```python
# EMR: Read from Kinesis
df = spark.readStream \
    .format("kinesis") \
    .option("streamName", "my-stream") \
    .option("region", "us-east-1") \
    .option("initialPosition", "latest") \
    .load()

# Databricks: Read from Kinesis (same syntax, works on DBR)
# But also consider using Auto Loader for file-based streaming:
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/checkpoints/schema") \
    .load("s3://my-bucket/incoming/")
```

### Checkpoint Location

```python
# EMR: Checkpoint to S3
query = df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "s3://my-bucket/checkpoints/job1") \
    .option("path", "s3://my-bucket/output/") \
    .start()

# Databricks: Prefer Delta format and DBFS or UC Volumes for checkpoints
query = df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/job1") \
    .toTable("catalog.schema.output_table")
```

For detailed streaming migration, see the **emr-streaming-migration** skill.

## SQL Syntax Differences

Standard Spark SQL works identically. Databricks adds additional SQL commands:

```sql
-- Databricks-specific SQL extensions (not available on EMR):
OPTIMIZE table_name [ZORDER BY (col1, col2)];
VACUUM table_name [RETAIN 168 HOURS];
DESCRIBE HISTORY table_name;
RESTORE TABLE table_name TO VERSION AS OF 5;
COPY INTO table_name FROM 's3://path' FILEFORMAT = CSV;
CREATE MATERIALIZED VIEW ...;
CREATE STREAMING TABLE ...;

-- Works on both:
SELECT *, row_number() OVER (PARTITION BY id ORDER BY ts DESC) AS rn FROM table;
MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE ...;
```

## Python Version Differences

| Platform | Python Version | Notes |
|---|---|---|
| EMR 6.15 | Python 3.9 | Can install other versions via bootstrap |
| EMR 7.x | Python 3.9 | Can install other versions via bootstrap |
| DBR 14.3 LTS | Python 3.10 | Fixed per runtime version |
| DBR 15.4 LTS | Python 3.11 | Fixed per runtime version |
| DBR 16.x | Python 3.12 | Fixed per runtime version |

### Impact on Code

- **f-strings with `=`** (e.g., `f"{x=}"`) require Python 3.8+ (safe on both).
- **`match` statements** require Python 3.10+ (safe on DBR 14.3+, not on EMR default Python 3.9).
- **`tomllib`** standard library requires Python 3.11+ (DBR 15.4+ only).
- **Type hint syntax** like `list[str]` (lowercase) requires Python 3.9+ (safe on both).
- **`asyncio.TaskGroup`** requires Python 3.11+ (DBR 15.4+ only).

### Recommendation

If your EMR code uses Python 3.9 features only, it will run without changes on any DBR version. If migrating to DBR 15.4+ you can optionally modernize to use Python 3.11 features.
