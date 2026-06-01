# Code Patterns: EMR to Databricks Migration

This document provides before/after code examples for the 10 most common migration patterns.

---

## Pattern 1: S3 Access with Credentials → Unity Catalog External Locations

### EMR (Before)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("S3AccessJob") \
    .master("yarn") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAIOSFODNN7EXAMPLE") \  # gitleaks:allow
    .config("spark.hadoop.fs.s3a.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY") \  # gitleaks:allow
    .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com") \
    .getOrCreate()

# Read data from S3 using hardcoded credentials
df = spark.read.parquet("s3a://my-data-bucket/raw/events/2024/")

# Process and write back to S3
df_cleaned = df.filter(df["status"] == "active").drop("temp_col")
df_cleaned.write.mode("overwrite").parquet("s3a://my-data-bucket/processed/events/")

spark.stop()
```

### Databricks (After)

```python
# No need to create SparkSession — it's pre-initialized
# No need for S3 credentials — Unity Catalog manages access

# Option A: Read from a Unity Catalog managed table (preferred)
df = spark.table("main.events.raw_events")

# Option B: Read from an external location registered in Unity Catalog
df = spark.read.parquet("s3a://my-data-bucket/raw/events/2024/")
# ^ Works if an external location is configured for s3://my-data-bucket/

# Option C: Use secret scope if you truly need direct credential access
access_key = dbutils.secrets.get(scope="aws-creds", key="access-key")
secret_key = dbutils.secrets.get(scope="aws-creds", key="secret-key")
spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
df = spark.read.parquet("s3a://my-data-bucket/raw/events/2024/")

# Process and write as a Delta managed table
df_cleaned = df.filter(df["status"] == "active").drop("temp_col")
df_cleaned.write.mode("overwrite").saveAsTable("main.events.processed_events")
```

**Notes:**
- Unity Catalog external locations let you access S3 paths without embedding credentials in code.
- Managed Delta tables are preferred over writing raw Parquet to S3.
- `spark.stop()` should not be called in Databricks notebooks.

---

## Pattern 2: GlueContext / DynamicFrame → Native SparkSession

### EMR / AWS Glue (Before)

```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_db', 'source_table', 'target_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from Glue Data Catalog
source_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args['source_db'],
    table_name=args['source_table'],
    transformation_ctx="source_dyf"
)

# Resolve choice types (Glue-specific)
resolved = source_dyf.resolveChoice(
    specs=[("price", "cast:double"), ("quantity", "cast:int")]
)

# Apply mapping
mapped = resolved.apply_mapping([
    ("id", "string", "event_id", "string"),
    ("price", "double", "event_price", "double"),
    ("quantity", "int", "event_qty", "int"),
    ("timestamp", "string", "event_ts", "timestamp")
])

# Filter nulls using Glue's Filter transform
filtered = Filter.apply(frame=mapped, f=lambda x: x["event_id"] is not None)

# Write to S3
glueContext.write_dynamic_frame.from_options(
    frame=filtered,
    connection_type="s3",
    connection_options={"path": args['target_path'], "partitionKeys": ["event_ts"]},
    format="parquet"
)

job.commit()
```

### Databricks (After)

```python
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

# Parameters via widgets or job config
source_catalog = dbutils.widgets.get("source_catalog")  # e.g., "main"
source_schema = dbutils.widgets.get("source_schema")     # e.g., "raw"
source_table = dbutils.widgets.get("source_table")       # e.g., "events"

# Read from Unity Catalog
df = spark.table(f"{source_catalog}.{source_schema}.{source_table}")

# Resolve types (replaces resolveChoice)
df = df.withColumn("price", col("price").cast(DoubleType())) \
       .withColumn("quantity", col("quantity").cast(IntegerType()))

# Apply column mapping (replaces apply_mapping)
mapped = df.select(
    col("id").alias("event_id"),
    col("price").alias("event_price"),
    col("quantity").alias("event_qty"),
    col("timestamp").cast(TimestampType()).alias("event_ts")
)

# Filter nulls (replaces Glue Filter transform)
filtered = mapped.filter(col("event_id").isNotNull())

# Write as Delta managed table (replaces write to S3 Parquet)
filtered.write \
    .mode("overwrite") \
    .partitionBy("event_ts") \
    .saveAsTable(f"{source_catalog}.processed.events")
```

**Notes:**
- All `awsglue` imports are removed.
- `DynamicFrame` operations are replaced with standard DataFrame operations.
- `resolveChoice` becomes explicit `.cast()` calls.
- `apply_mapping` becomes `.select()` with `.alias()`.
- `job.init()` / `job.commit()` have no equivalent and are not needed.

---

## Pattern 3: EMR Step Script with argparse → Databricks Notebook with Widgets

### EMR (Before)

```python
"""EMR Step script submitted via boto3 add_job_flow_steps"""
import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--mode", default="overwrite")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName(f"ETL-{args.date}") \
        .master("yarn") \
        .enableHiveSupport() \
        .getOrCreate()

    df = spark.read.parquet(f"{args.input_path}/date={args.date}")
    result = df.groupBy("category").count()
    result.write.mode(args.mode).parquet(args.output_path)

    spark.stop()

if __name__ == "__main__":
    main()
```

**EMR Step submission (boto3):**
```python
emr_client.add_job_flow_steps(
    JobFlowId="j-XXXXXXXXXXXXX",
    Steps=[{
        "Name": "ETL Step",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "s3://my-scripts/etl_job.py",
                "--input-path", "s3://my-data/raw",
                "--output-path", "s3://my-data/processed",
                "--date", "2024-01-15",
                "--mode", "overwrite"
            ]
        }
    }]
)
```

### Databricks (After — Notebook with Widgets)

```python
# Databricks notebook: /Repos/team/etl/etl_job

# Define widgets for parameterization
dbutils.widgets.text("input_table", "main.raw.events")
dbutils.widgets.text("output_table", "main.processed.events_summary")
dbutils.widgets.text("date", "2024-01-15")
dbutils.widgets.dropdown("mode", "overwrite", ["overwrite", "append"])

# Get parameter values
input_table = dbutils.widgets.get("input_table")
output_table = dbutils.widgets.get("output_table")
date_filter = dbutils.widgets.get("date")
write_mode = dbutils.widgets.get("mode")

# spark is pre-initialized
df = spark.table(input_table).filter(f"date = '{date_filter}'")
result = df.groupBy("category").count()
result.write.mode(write_mode).saveAsTable(output_table)
```

**Databricks Job submission (REST API or databricks.yml):**
```yaml
# databricks.yml — Databricks Asset Bundle
resources:
  jobs:
    etl_job:
      name: "ETL Job"
      tasks:
        - task_key: etl_step
          notebook_task:
            notebook_path: /Repos/team/etl/etl_job
            base_parameters:
              input_table: "main.raw.events"
              output_table: "main.processed.events_summary"
              date: "2024-01-15"
              mode: "overwrite"
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            num_workers: 4
            node_type_id: "i3.xlarge"
```

**Notes:**
- `argparse` is replaced by `dbutils.widgets` for notebooks or job parameters in `databricks.yml`.
- `spark-submit` is replaced by the Databricks Jobs API or Asset Bundles.
- `master("yarn")` and `spark.stop()` are removed.
- S3 paths are replaced by Unity Catalog table references.

---

## Pattern 4: EMRFS Consistent View → Delta Lake ACID

### EMR (Before)

```python
# EMR: Using EMRFS consistent view to handle S3 eventual consistency
spark = SparkSession.builder \
    .appName("ConsistentWriteJob") \
    .master("yarn") \
    .config("spark.hadoop.fs.s3.consistent", "true") \
    .config("spark.hadoop.fs.s3.consistent.retryPeriodSeconds", "10") \
    .config("spark.hadoop.fs.s3.consistent.retryCount", "5") \
    .config("spark.hadoop.fs.s3.consistent.metadata.tableName", "EmrFSMetadata") \
    .config("spark.hadoop.fs.s3.consistent.metadata.read.capacity", "600") \
    .config("spark.hadoop.fs.s3.consistent.metadata.write.capacity", "300") \
    .getOrCreate()

# Read-modify-write cycle prone to consistency issues without EMRFS
df = spark.read.parquet("s3://my-bucket/inventory/")
updated = df.filter(df["stock"] > 0)
updated.write.mode("overwrite").parquet("s3://my-bucket/inventory/")

# Partition overwrite to avoid full table overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
daily = spark.read.parquet("s3://my-bucket/events/date=2024-01-15/")
daily_processed = daily.withColumn("processed", lit(True))
daily_processed.write.mode("overwrite").partitionBy("date") \
    .parquet("s3://my-bucket/events/")
```

### Databricks (After)

```python
# No EMRFS configs needed — Delta Lake provides ACID transactions

# Read from Delta table
df = spark.table("main.warehouse.inventory")

# Atomic overwrite with Delta
updated = df.filter(df["stock"] > 0)
updated.write.mode("overwrite").saveAsTable("main.warehouse.inventory")

# Even better: Use MERGE for upserts instead of overwrite
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "main.warehouse.inventory")
source = spark.read.parquet("s3://incoming/new-inventory/")

target.alias("t").merge(
    source.alias("s"),
    "t.product_id = s.product_id"
).whenMatchedUpdate(set={
    "stock": "s.stock",
    "last_updated": "current_timestamp()"
}).whenNotMatchedInsertAll().execute()

# Partition overwrite works natively with Delta
# replaceWhere is more precise than dynamic partition overwrite
daily_processed = spark.read.parquet("s3://my-bucket/events/date=2024-01-15/") \
    .withColumn("processed", lit(True))

daily_processed.write \
    .mode("overwrite") \
    .option("replaceWhere", "date = '2024-01-15'") \
    .saveAsTable("main.events.daily")
```

**Notes:**
- All `spark.hadoop.fs.s3.consistent.*` configs should be removed.
- Delta Lake provides ACID guarantees natively; no metadata table or DynamoDB is needed.
- Use `MERGE` for upserts instead of read-overwrite patterns.
- Use `replaceWhere` for surgical partition overwrites.
- Delta also provides time travel: `spark.read.option("versionAsOf", 5).table("...")`.

---

## Pattern 5: Custom JAR Loading → Cluster Libraries / UC Volumes

### EMR (Before)

```bash
# EMR: spark-submit with custom JARs
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --jars s3://my-jars/custom-udf.jar,s3://my-jars/postgres-jdbc.jar \
    --packages org.apache.spark:spark-avro_2.12:3.5.0 \
    --py-files s3://my-jars/utils.zip \
    s3://my-scripts/main_job.py
```

```python
# Or loading JARs at runtime
sc.addJar("s3://my-jars/custom-udf.jar")
spark.sql("CREATE TEMPORARY FUNCTION clean_text AS 'com.mycompany.CleanTextUDF'")

# Adding Python files at runtime
sc.addPyFile("s3://my-scripts/helpers.py")
from helpers import transform_data
```

### Databricks (After)

**Option A: Cluster Libraries (UI or API)**
```json
// Cluster configuration — libraries section
{
  "libraries": [
    {"jar": "dbfs:/FileStore/jars/custom-udf.jar"},
    {"maven": {"coordinates": "org.postgresql:postgresql:42.7.1"}},
    {"pypi": {"package": "great-expectations==0.18.0"}}
  ]
}
```

**Option B: Unity Catalog Volumes (recommended for governance)**
```python
# Upload JAR to a UC Volume
# dbutils.fs.cp("s3://my-jars/custom-udf.jar", "/Volumes/main/libs/jars/custom-udf.jar")

# Register UDF from UC Volume
spark.sql("""
    CREATE FUNCTION main.udfs.clean_text
    AS 'com.mycompany.CleanTextUDF'
    USING JAR '/Volumes/main/libs/jars/custom-udf.jar'
""")

# Use the UDF
spark.sql("SELECT main.udfs.clean_text(description) FROM main.products.items")
```

**Option C: Job-level library configuration (databricks.yml)**
```yaml
resources:
  jobs:
    my_job:
      tasks:
        - task_key: main_task
          libraries:
            - jar: /Volumes/main/libs/jars/custom-udf.jar
            - maven:
                coordinates: org.postgresql:postgresql:42.7.1
            - pypi:
                package: great-expectations==0.18.0
          spark_python_task:
            python_file: /Repos/team/project/main_job.py
```

**Option D: %pip for Python packages in notebooks**
```python
%pip install great-expectations==0.18.0 boto3 psycopg2-binary
```

**Notes:**
- `sc.addJar()` works on Databricks but cluster libraries are preferred for reliability.
- `sc.addPyFile()` works but `%pip install` or Databricks Repos are cleaner.
- Maven coordinates work the same way (`--packages` → cluster Maven library).
- UC Volumes provide governance over shared JARs (access control, lineage).

---

## Pattern 6: Hive Metastore Queries → Unity Catalog

### EMR (Before)

```python
spark = SparkSession.builder \
    .appName("HiveJob") \
    .master("yarn") \
    .enableHiveSupport() \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .getOrCreate()

# List databases
spark.sql("SHOW DATABASES").show()

# Use a specific database
spark.sql("USE my_database")

# Query table
df = spark.sql("""
    SELECT customer_id, SUM(amount) as total_spend
    FROM my_database.transactions
    WHERE year = 2024
    GROUP BY customer_id
    HAVING total_spend > 1000
""")

# Create a new table in Hive
df.write.mode("overwrite").saveAsTable("my_database.high_value_customers")

# Create an external table
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS my_database.external_events (
        event_id STRING,
        event_type STRING,
        event_ts TIMESTAMP
    )
    STORED AS PARQUET
    LOCATION 's3://my-bucket/events/'
""")

# Partition management
spark.sql("ALTER TABLE my_database.events ADD PARTITION (date='2024-01-15')")
spark.sql("MSCK REPAIR TABLE my_database.events")
```

### Databricks (After)

```python
# No enableHiveSupport() or metastore URI needed — Unity Catalog is default

# List catalogs and schemas
spark.sql("SHOW CATALOGS").show()
spark.sql("SHOW SCHEMAS IN main").show()

# Use a specific catalog and schema
spark.sql("USE CATALOG main")
spark.sql("USE SCHEMA my_schema")

# Query table (three-level namespace)
df = spark.sql("""
    SELECT customer_id, SUM(amount) as total_spend
    FROM main.my_schema.transactions
    WHERE year = 2024
    GROUP BY customer_id
    HAVING total_spend > 1000
""")

# Create a managed Delta table in Unity Catalog
df.write.mode("overwrite").saveAsTable("main.my_schema.high_value_customers")

# Create an external table with Unity Catalog
spark.sql("""
    CREATE TABLE IF NOT EXISTS main.my_schema.external_events (
        event_id STRING,
        event_type STRING,
        event_ts TIMESTAMP
    )
    USING DELTA
    LOCATION 's3://my-bucket/events/'
""")
# Note: The S3 location must be under a registered external location in Unity Catalog

# Partitions are managed automatically by Delta Lake
# No need for MSCK REPAIR TABLE or manual ADD PARTITION
# Delta discovers partitions automatically
```

**Notes:**
- Three-level namespace: `catalog.schema.table` replaces `database.table`.
- Delta Lake manages partitions automatically; `MSCK REPAIR TABLE` is not needed.
- `STORED AS PARQUET` becomes `USING DELTA` (or specify format explicitly).
- External tables must point to paths under registered UC external locations.
- `enableHiveSupport()` is not needed.

---

## Pattern 7: Writing Parquet to S3 → Writing Delta to Managed Tables

### EMR (Before)

```python
# EMR: Raw Parquet workflow with manual schema management
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, year, month, dayofmonth

spark = SparkSession.builder.appName("ParquetWriter").master("yarn").getOrCreate()

# Read source
df = spark.read.json("s3://my-bucket/raw/events/")

# Add metadata columns
df = df.withColumn("processed_at", current_timestamp()) \
       .withColumn("year", year("event_ts")) \
       .withColumn("month", month("event_ts")) \
       .withColumn("day", dayofmonth("event_ts"))

# Write as partitioned Parquet
df.write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .parquet("s3://my-bucket/processed/events/")

# Manually manage schema evolution: add new columns
# If source schema changes, Parquet silently drops new columns or errors
# Must manually merge schemas:
df_old = spark.read.option("mergeSchema", "true").parquet("s3://my-bucket/processed/events/")

# No easy way to:
# - Update existing records
# - Roll back a bad write
# - Query historical versions
# - Compact small files
```

### Databricks (After)

```python
from pyspark.sql.functions import current_timestamp

# Read source
df = spark.read.format("json").load("s3://my-bucket/raw/events/")
# Or better: use Auto Loader for incremental ingestion
# df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("s3://...")

# Add metadata
df = df.withColumn("processed_at", current_timestamp())

# Write as Delta managed table (auto-partitioned, ACID, schema evolution)
df.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("main.events.processed_events")

# Delta provides automatically:
# - ACID transactions (no partial writes)
# - Schema evolution with mergeSchema
# - Time travel: spark.table("...").option("versionAsOf", 5)
# - OPTIMIZE for file compaction: spark.sql("OPTIMIZE main.events.processed_events")
# - Z-ORDER for query acceleration: spark.sql("OPTIMIZE ... ZORDER BY (event_type)")
# - VACUUM for cleanup: spark.sql("VACUUM main.events.processed_events RETAIN 168 HOURS")

# Update existing records (impossible with raw Parquet)
from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, "main.events.processed_events")
dt.update(
    condition="event_id = 'evt_123'",
    set={"status": "'corrected'"}
)

# Roll back a bad write
spark.sql("RESTORE TABLE main.events.processed_events TO VERSION AS OF 5")
```

**Notes:**
- Delta Lake eliminates the need for manual partition management.
- Schema evolution is handled with `mergeSchema` option.
- Time travel, updates, deletes, and merges are native to Delta.
- `OPTIMIZE` and `VACUUM` replace manual file compaction scripts.
- Prefer managed tables over writing to explicit S3 paths.

---

## Pattern 8: spark-submit with --jars → Databricks Job with Libraries

### EMR (Before)

```bash
# EMR: spark-submit command in an EMR Step
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 8g \
    --executor-cores 4 \
    --num-executors 10 \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=20 \
    --jars s3://my-jars/custom-udf-1.0.jar,s3://my-jars/postgres-42.6.0.jar \
    --packages org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
    --py-files s3://my-code/utils.zip \
    s3://my-code/main_etl.py \
    --input-path s3://raw-data/ \
    --output-path s3://processed-data/ \
    --date 2024-01-15
```

### Databricks (After — databricks.yml)

```yaml
# databricks.yml — Databricks Asset Bundle
bundle:
  name: etl_pipeline

resources:
  jobs:
    main_etl:
      name: "Main ETL Pipeline"
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"  # Daily at 6 AM
        timezone_id: "America/New_York"
      tasks:
        - task_key: etl_task
          spark_python_task:
            python_file: ./src/main_etl.py
            parameters:
              - "--input-table"
              - "main.raw.events"
              - "--output-table"
              - "main.processed.events"
              - "--date"
              - "{{job.parameters.date}}"
          libraries:
            - jar: /Volumes/main/libs/jars/custom-udf-1.0.jar
            - maven:
                coordinates: "org.postgresql:postgresql:42.7.1"
            # spark-avro is pre-installed on DBR
            # delta-spark is pre-installed on DBR
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "i3.xlarge"
            autoscale:
              min_workers: 5
              max_workers: 20
            spark_conf:
              spark.sql.shuffle.partitions: "200"
              spark.serializer: "org.apache.spark.serializer.KryoSerializer"
              # dynamic allocation is managed by Databricks autoscaling
            # driver and executor memory managed by node_type_id selection

      parameters:
        - name: date
          default: "2024-01-15"
```

**Notes:**
- `--master yarn` and `--deploy-mode cluster` are not needed.
- Driver/executor memory is determined by the node type, not Spark configs.
- Dynamic allocation is replaced by Databricks cluster autoscaling (min/max workers).
- `--packages` for Delta and Avro are not needed (pre-installed on DBR).
- `--py-files` is replaced by Databricks Repos or `%pip install`.
- `--jars` are replaced by job-level `libraries` or cluster libraries.

---

## Pattern 9: YARN Queue Isolation → Cluster Access Control

### EMR (Before)

```python
# EMR: YARN queue-based resource isolation
spark = SparkSession.builder \
    .appName("TeamAJob") \
    .master("yarn") \
    .config("spark.yarn.queue", "team_a") \
    .config("spark.yarn.executor.memoryOverhead", "1024") \
    .config("spark.yarn.am.memory", "2g") \
    .config("spark.yarn.am.cores", "2") \
    .config("spark.yarn.maxAppAttempts", "2") \
    .config("spark.yarn.submit.waitAppCompletion", "true") \
    .config("spark.yarn.tags", "etl,team_a,production") \
    .getOrCreate()
```

**EMR YARN Capacity Scheduler (capacity-scheduler.xml):**
```xml
<property>
    <name>yarn.scheduler.capacity.root.queues</name>
    <value>team_a,team_b,default</value>
</property>
<property>
    <name>yarn.scheduler.capacity.root.team_a.capacity</name>
    <value>40</value>
</property>
<property>
    <name>yarn.scheduler.capacity.root.team_b.capacity</name>
    <value>40</value>
</property>
<property>
    <name>yarn.scheduler.capacity.root.default.capacity</name>
    <value>20</value>
</property>
```

### Databricks (After)

```python
# No YARN configs needed — resource isolation is handled by cluster policies and access control
# spark session is pre-initialized

# All spark.yarn.* configs should be REMOVED
# Resource allocation is handled by:
# 1. Cluster size (node type + number of workers)
# 2. Cluster policies (enforce resource limits per team)
# 3. Cluster access control (who can use which cluster)
# 4. SQL Warehouses for SQL workloads (with query queuing)
```

**Databricks Cluster Policy (JSON):**
```json
{
  "cluster_name": {
    "type": "fixed",
    "value": "team-a-cluster"
  },
  "spark_version": {
    "type": "regex",
    "pattern": "15\\.4\\.x-scala.*"
  },
  "node_type_id": {
    "type": "allowlist",
    "values": ["i3.xlarge", "i3.2xlarge"]
  },
  "num_workers": {
    "type": "range",
    "minValue": 2,
    "maxValue": 20
  },
  "autoscale.min_workers": {
    "type": "range",
    "minValue": 2,
    "maxValue": 5
  },
  "autoscale.max_workers": {
    "type": "range",
    "minValue": 5,
    "maxValue": 20
  },
  "custom_tags.team": {
    "type": "fixed",
    "value": "team_a"
  }
}
```

**Notes:**
- YARN queues are replaced by separate clusters or cluster policies per team.
- Resource limits are enforced through cluster policies (max nodes, node types, auto-termination).
- Cost isolation is achieved through tagging (`custom_tags.team`) and cluster-level cost attribution.
- For shared compute, SQL Warehouses provide automatic query queuing and resource management.
- `spark.yarn.*` configs are all ignored on Databricks and should be removed.

---

## Pattern 10: CloudWatch Metrics → Databricks Monitoring

### EMR (Before)

```python
# EMR: Custom CloudWatch metrics via Spark listener
import boto3
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MetricsJob") \
    .master("yarn") \
    .config("spark.metrics.conf.*.sink.cloudwatch.class",
            "com.amazonaws.emr.metrics.CloudWatchSink") \
    .config("spark.metrics.conf.*.sink.cloudwatch.namespace", "EMR/Spark") \
    .config("spark.metrics.conf.*.sink.cloudwatch.period", "60") \
    .config("spark.metrics.conf.*.sink.cloudwatch.unit", "SECONDS") \
    .getOrCreate()

# Manual CloudWatch metric publishing
cw_client = boto3.client("cloudwatch", region_name="us-east-1")

def publish_metric(name, value, unit="Count"):
    cw_client.put_metric_data(
        Namespace="MyApp/ETL",
        MetricData=[{
            "MetricName": name,
            "Value": value,
            "Unit": unit,
            "Dimensions": [
                {"Name": "Environment", "Value": "production"},
                {"Name": "JobName", "Value": "daily_etl"}
            ]
        }]
    )

# After processing
df = spark.read.parquet("s3://my-bucket/raw/")
processed = df.filter(df["valid"] == True)
record_count = processed.count()

publish_metric("RecordsProcessed", record_count)
publish_metric("ProcessingTimeMs", processing_time, "Milliseconds")

processed.write.mode("overwrite").parquet("s3://my-bucket/processed/")
```

### Databricks (After)

```python
# Databricks provides built-in monitoring — no custom sink configuration needed
# Ganglia metrics, Spark UI, and query history are available by default

# For custom application metrics, use Spark's built-in accumulators
# or push to your observability platform

from pyspark.sql.functions import col

# Process data
df = spark.table("main.raw.events")
processed = df.filter(col("valid") == True)
record_count = processed.count()

# Option A: Log metrics for Databricks job monitoring
print(f"METRIC: records_processed={record_count}")
# Databricks captures stdout/stderr in job run output

# Option B: Use Spark accumulators for distributed counting
records_acc = spark.sparkContext.accumulator(0)
errors_acc = spark.sparkContext.accumulator(0)

def process_row(row):
    if row["valid"]:
        records_acc.add(1)
    else:
        errors_acc.add(1)

df.foreach(process_row)
print(f"Records: {records_acc.value}, Errors: {errors_acc.value}")

# Option C: Write metrics to a Delta table for dashboarding
from pyspark.sql import Row
from datetime import datetime

metrics = spark.createDataFrame([
    Row(
        job_name="daily_etl",
        metric_name="records_processed",
        metric_value=float(record_count),
        timestamp=datetime.now()
    )
])
metrics.write.mode("append").saveAsTable("main.monitoring.job_metrics")

# Option D: Push to external observability (Datadog, Prometheus, etc.)
# %pip install datadog
# from datadog import statsd
# statsd.gauge("etl.records_processed", record_count, tags=["env:prod"])

# Write results
processed.write.mode("overwrite").saveAsTable("main.processed.events")
```

**Notes:**
- CloudWatch sink configs are removed; Databricks has built-in monitoring.
- Spark UI is available in the Databricks UI for each cluster and job run.
- Ganglia metrics provide cluster-level CPU, memory, and network monitoring.
- For custom metrics, write to a Delta table and build dashboards in Databricks SQL.
- For integration with external monitoring (Datadog, Prometheus, Splunk), use their respective client libraries.
- Databricks also supports structured logging via Log4j configuration on clusters.
