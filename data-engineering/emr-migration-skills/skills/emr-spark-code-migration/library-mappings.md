# Library Mappings: EMR to Databricks Runtime

This document maps libraries bundled with Amazon EMR releases to their equivalents on Databricks Runtime (DBR). Use this to identify version conflicts, missing libraries, and pre-installed packages.

## Core Hadoop / Spark Library Versions

### EMR Release → Core Library Versions

| Library | EMR 6.15 | EMR 7.0 | EMR 7.1 | EMR 7.2 |
|---|---|---|---|---|
| Apache Spark | 3.4.1 | 3.5.0 | 3.5.1 | 3.5.3 |
| Hadoop | 3.3.6 | 3.3.6 | 3.3.6 | 3.4.0 |
| Hive | 3.1.3 | 3.1.3 | 3.1.3 | 3.1.3 |
| hadoop-aws | 3.3.6 | 3.3.6 | 3.3.6 | 3.4.0 |
| aws-java-sdk-bundle | 1.12.x | 1.12.x | 1.12.x | 1.12.x |
| Scala | 2.12.17 | 2.12.18 | 2.12.18 | 2.12.18 |
| Java | 8/11 | 8/11/17 | 8/11/17 | 8/11/17 |
| Python | 3.9 | 3.9 | 3.9 | 3.9 |
| Guava | 27.0-jre | 27.0-jre | 27.0-jre | 27.0-jre |
| Jackson | 2.14.x | 2.15.x | 2.15.x | 2.15.x |
| Delta Lake | Not bundled | Not bundled | Not bundled | Not bundled |
| Iceberg | 1.3.x (optional) | 1.4.x (optional) | 1.4.x (optional) | 1.5.x (optional) |
| Hudi | 0.14.x (optional) | 0.14.x (optional) | 0.14.x (optional) | 0.14.x (optional) |

### DBR Version → Core Library Versions

| Library | DBR 14.3 LTS | DBR 15.4 LTS | DBR 16.x |
|---|---|---|---|
| Apache Spark | 3.5.0 | 3.5.0 | 4.0.0 |
| Hadoop | 3.3.6 | 3.3.6 | 3.4.x |
| Hive (metastore client) | 2.3.9 | 2.3.9 | 2.3.9 |
| hadoop-aws | 3.3.6 | 3.3.6 | 3.4.x |
| aws-java-sdk-bundle | 1.12.x | 1.12.x | 1.12.x |
| Scala | 2.12.15 | 2.12.15 | 2.13.x |
| Java | 8/11 | 11/17 | 17 |
| Python | 3.10 | 3.11 | 3.12 |
| Guava | 16.0.1 | 16.0.1 | 27.0+ |
| Jackson | 2.14.x | 2.15.x | 2.16.x |
| Delta Lake | 3.1.0 | 3.2.0 | 4.0.0 |
| Iceberg | Not bundled | Not bundled | Optional |
| Hudi | Not bundled | Not bundled | Not bundled |

## Pre-Installed vs Needs Installation

### Pre-installed on DBR (Do NOT add manually)

These libraries are bundled with DBR. Adding them manually often causes version conflicts:

| Library | Notes |
|---|---|
| `hadoop-aws` | Always bundled; adding a different version causes `NoSuchMethodError` |
| `aws-java-sdk-bundle` | Bundled with hadoop-aws; never add separately |
| `delta-core` / `delta-spark` | Core to DBR; adding it will conflict with the built-in version |
| `guava` | Bundled; version differs from EMR (see Guava Conflicts section) |
| `jackson-core` / `jackson-databind` | Bundled; version conflicts are common if you add your own |
| `commons-lang3` | Bundled |
| `commons-io` | Bundled |
| `snappy-java` | Bundled |
| `zstd-jni` | Bundled |
| `parquet-hadoop` | Bundled |
| `orc-core` | Bundled |
| `protobuf-java` | Bundled |
| `arrow` / `pyarrow` | Bundled |

### Needs Explicit Installation on DBR

These libraries are available on EMR (or commonly used there) but are NOT pre-installed on DBR:

| Library | How to Install on DBR |
|---|---|
| `boto3` / `botocore` | `%pip install boto3` — needed for direct AWS API calls |
| `awswrangler` (AWS SDK for pandas) | `%pip install awswrangler` |
| `psycopg2` | `%pip install psycopg2-binary` |
| `pymysql` | `%pip install pymysql` |
| `requests` | Pre-installed on some DBR versions; `%pip install requests` if missing |
| `sqlalchemy` | `%pip install sqlalchemy` |
| `koalas` | Deprecated; use `pyspark.pandas` (built into Spark 3.2+) |
| `iceberg-spark-runtime` | Install as Maven library on cluster |
| `hudi-spark-bundle` | Install as Maven library on cluster |

## Common Third-Party Python Libraries

### ML / Data Science Libraries

| Library | EMR 6.15 (default) | EMR 7.x (default) | DBR 14.3 LTS | DBR 15.4 LTS | DBR 14.3 ML | DBR 15.4 ML |
|---|---|---|---|---|---|---|
| pandas | 1.5.x | 2.0.x | 1.5.3 | 2.1.x | 2.0.3 | 2.1.x |
| numpy | 1.24.x | 1.26.x | 1.24.x | 1.26.x | 1.24.x | 1.26.x |
| scikit-learn | Not installed | Not installed | Not installed | Not installed | 1.3.x | 1.4.x |
| tensorflow | Not installed | Not installed | Not installed | Not installed | 2.14.x | 2.15.x |
| pytorch | Not installed | Not installed | Not installed | Not installed | 2.0.x | 2.1.x |
| xgboost | Not installed | Not installed | Not installed | Not installed | 1.7.x | 2.0.x |
| matplotlib | Not installed | Not installed | 3.7.x | 3.8.x | 3.7.x | 3.8.x |
| scipy | Not installed | Not installed | 1.11.x | 1.12.x | 1.11.x | 1.12.x |

**Note:** For ML workloads, use the **Databricks Runtime for Machine Learning (ML Runtime)** which pre-installs TensorFlow, PyTorch, scikit-learn, XGBoost, and other ML libraries.

### Data Processing Libraries

| Library | EMR (default) | DBR 14.3 LTS | DBR 15.4 LTS | Install Method |
|---|---|---|---|---|
| pyarrow | 12.x+ | 12.x | 14.x | Pre-installed |
| pyspark | Matches Spark version | Matches Spark version | Matches Spark version | Pre-installed |
| koalas | 1.8.x (EMR 6.x) | N/A (use pyspark.pandas) | N/A | Deprecated |
| petl | Not installed | Not installed | Not installed | `%pip install petl` |
| polars | Not installed | Not installed | Not installed | `%pip install polars` |
| dask | Not installed | Not installed | Not installed | `%pip install dask` |

## Critical Version Conflicts

### Guava Version Conflict

This is one of the most common issues when migrating JARs from EMR to Databricks.

| Platform | Guava Version |
|---|---|
| EMR 6.15 / 7.x | 27.0-jre |
| DBR 14.3 / 15.4 | 16.0.1 |
| DBR 16.x | 27.0+ |

**Symptoms:**
```
java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument(ZLjava/lang/String;J)V
java.lang.NoSuchMethodError: com.google.common.collect.ImmutableMap.toImmutableMap(...)
```

**Solutions:**
1. **Shade Guava** in your JAR using Maven Shade Plugin or SBT Assembly:
   ```xml
   <relocations>
     <relocation>
       <pattern>com.google.common</pattern>
       <shadedPattern>shaded.com.google.common</shadedPattern>
     </relocation>
   </relocations>
   ```
2. **Upgrade to DBR 16.x** which bundles a newer Guava version.
3. **Remove explicit Guava dependency** and rewrite code to use the bundled version's API.

### Jackson Version Alignment

| Platform | Jackson Version |
|---|---|
| EMR 6.15 | 2.14.x |
| EMR 7.x | 2.15.x |
| DBR 14.3 | 2.14.x |
| DBR 15.4 | 2.15.x |

Jackson versions generally align between EMR 7.x and DBR 15.4. Problems arise when:
- A third-party JAR bundles its own Jackson version
- You explicitly add a different Jackson version as a cluster library

**Solution:** Do not add Jackson JARs explicitly. If a library requires a specific Jackson version, shade it.

### hadoop-aws Version Alignment

**Never add hadoop-aws as a cluster library on Databricks.** It is bundled with the runtime and adding a different version causes class-loading conflicts.

**Symptoms:**
```
java.lang.NoSuchMethodError: org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(...)
java.lang.NoClassDefFoundError: org/apache/hadoop/fs/s3a/auth/delegation/...
```

**Solution:** Remove any explicit hadoop-aws or aws-java-sdk dependencies from your project when running on Databricks.

### Hive Metastore Client

| Platform | Hive Metastore Version |
|---|---|
| EMR 6.15 / 7.x | 3.1.3 |
| DBR 14.3 / 15.4 | 2.3.9 (for external metastore compatibility) |

If connecting DBR to an external Hive metastore (e.g., AWS Glue Data Catalog), the client version difference usually does not cause issues because the Thrift protocol is backward-compatible. However, certain Hive 3.x features (ACID tables, materialized views) may not work through the 2.3.9 client.

**Recommendation:** Migrate to Unity Catalog instead of connecting to external Hive metastore.

## EMR-Specific Libraries (No DBR Equivalent)

These libraries exist only on EMR or AWS Glue and must be replaced:

| EMR Library | Purpose | Databricks Replacement |
|---|---|---|
| `awsglue` (GlueContext, DynamicFrame) | ETL framework | Native SparkSession / DataFrame |
| `awsglue.transforms` | Built-in transforms | PySpark DataFrame operations |
| `awsglue.utils.getResolvedOptions` | Argument parsing | `dbutils.widgets` or `argparse` |
| `emrfs-hadoop` | S3 consistent view | Delta Lake (ACID built-in) |
| `aws-sagemaker-spark` | SageMaker integration | MLflow + Databricks Model Serving |
| `hadoop-lzo` | LZO compression | Use Snappy or Zstd (pre-installed) |
| `s3-dist-cp` | Distributed S3 copy | `dbutils.fs.cp()` or `COPY INTO` |

## Maven Coordinates for Common Libraries

When you need to install JARs on Databricks clusters, use these Maven coordinates:

```
# Iceberg (if not using Delta)
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0

# Hudi (if not using Delta)
org.apache.hudi:hudi-spark3.5-bundle_2.12:0.14.1

# Kafka connector (for Structured Streaming)
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

# Kinesis connector
org.apache.spark:spark-streaming-kinesis-asl_2.12:3.5.0

# PostgreSQL JDBC
org.postgresql:postgresql:42.7.1

# MySQL JDBC
com.mysql:mysql-connector-j:8.2.0

# SQL Server JDBC
com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11

# MongoDB Spark Connector
org.mongodb.spark:mongo-spark-connector_2.12:10.2.1

# Elasticsearch Spark Connector
org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0
```

## Version Compatibility Matrix Summary

| Migration Path | Recommended DBR | Key Concern |
|---|---|---|
| EMR 6.15 (Spark 3.4) → DBR | DBR 14.3 LTS (Spark 3.5) | Minor Spark API changes (3.4→3.5); check deprecated APIs |
| EMR 7.0 (Spark 3.5) → DBR | DBR 14.3 LTS or 15.4 LTS | Closest match; focus on library versions |
| EMR 7.1 (Spark 3.5.1) → DBR | DBR 15.4 LTS | Good match; Python version difference (3.9→3.11) |
| EMR 7.2 (Spark 3.5.3) → DBR | DBR 15.4 LTS | Hadoop 3.4 on EMR vs 3.3.6 on DBR; test hadoop-aws features |
| Any EMR → DBR 16.x | DBR 16.x (Spark 4.0) | Major version jump; Scala 2.13, many deprecated APIs removed |
