# Examples: Spark to Databricks Migration

## Example 1: PySpark Batch ETL

### Before (OSS Spark)
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when

spark = SparkSession.builder \
    .master("yarn") \
    .appName("DailySalesETL") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hiveserver:9083") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.instances", "10") \
    .enableHiveSupport() \
    .getOrCreate()

# Read from HDFS
raw_sales = spark.read.parquet("hdfs:///data/raw/sales/2024/01/")
customers = spark.table("default.dim_customers")

# Transform
enriched = raw_sales.join(customers, "customer_id") \
    .withColumn("revenue_category",
        when(col("amount") > 1000, "high")
        .when(col("amount") > 100, "medium")
        .otherwise("low"))

# Write to Hive table
enriched.write.mode("overwrite") \
    .partitionBy("revenue_category") \
    .saveAsTable("analytics.enriched_sales")

# Write summary to HDFS
summary = enriched.groupBy("revenue_category") \
    .agg(sum("amount").alias("total"), count("*").alias("count"))
summary.write.mode("overwrite") \
    .parquet("hdfs:///data/processed/sales_summary/")

spark.stop()
```

### After (Databricks Notebook)
```python
from pyspark.sql.functions import col, sum, count, when

# spark is pre-initialized in Databricks notebooks
# Unity Catalog is the default metastore

# Read from UC Volumes (or directly from cloud storage)
raw_sales = spark.read.parquet("/Volumes/main/raw/sales/2024/01/")
customers = spark.table("main.default.dim_customers")

# Transform (unchanged)
enriched = raw_sales.join(customers, "customer_id") \
    .withColumn("revenue_category",
        when(col("amount") > 1000, "high")
        .when(col("amount") > 100, "medium")
        .otherwise("low"))

# Write to UC managed table
enriched.write.mode("overwrite") \
    .partitionBy("revenue_category") \
    .saveAsTable("main.analytics.enriched_sales")

# Write summary to UC managed table (preferred over file output)
summary = enriched.groupBy("revenue_category") \
    .agg(sum("amount").alias("total"), count("*").alias("count"))
summary.write.mode("overwrite") \
    .saveAsTable("main.analytics.sales_summary")

# No spark.stop() needed in notebooks
```

## Example 2: Scala Spark JAR Application

### Before
```scala
import org.apache.spark.sql.SparkSession

object ETLJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("yarn")
      .appName("ScalaETL")
      .config("spark.executor.memory", "8g")
      .config("spark.sql.shuffle.partitions", "400")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val inputPath = args(0)  // hdfs:///data/raw/events
    val outputTable = args(1) // analytics.processed_events

    val events = spark.read.json(inputPath)
    val processed = events
      .filter($"event_type" =!= "heartbeat")
      .withColumn("processed_at", current_timestamp())

    processed.write
      .mode("overwrite")
      .saveAsTable(outputTable)

    spark.stop()
  }
}
```

### After
```scala
import org.apache.spark.sql.SparkSession

object ETLJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ScalaETL")
      // master and executor config removed — set in Databricks job/cluster config
      .getOrCreate()

    import spark.implicits._

    val inputPath = args(0)  // /Volumes/main/raw/events
    val outputTable = args(1) // main.analytics.processed_events

    val events = spark.read.json(inputPath)
    val processed = events
      .filter($"event_type" =!= "heartbeat")
      .withColumn("processed_at", current_timestamp())

    processed.write
      .mode("overwrite")
      .saveAsTable(outputTable)

    spark.stop()
  }
}
```

## Example 3: spark-submit to Databricks Job

### Before
```bash
#!/bin/bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class com.example.ETLJob \
  --num-executors 20 \
  --executor-memory 8g \
  --executor-cores 4 \
  --driver-memory 4g \
  --conf spark.sql.shuffle.partitions=400 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --jars hdfs:///jars/utils-1.0.jar \
  --packages mysql:mysql-connector-java:8.0.33 \
  hdfs:///jars/etl-job-2.0.jar \
  hdfs:///data/raw/events \
  analytics.processed_events
```

### After (Databricks CLI)
```bash
databricks jobs create --json '{
  "name": "ETL Job",
  "tasks": [{
    "task_key": "etl_main",
    "spark_jar_task": {
      "main_class_name": "com.example.ETLJob",
      "parameters": [
        "/Volumes/main/raw/events",
        "main.analytics.processed_events"
      ]
    },
    "libraries": [
      {"jar": "dbfs:/jars/etl-job-2.0.jar"},
      {"jar": "dbfs:/jars/utils-1.0.jar"},
      {"maven": {"coordinates": "mysql:mysql-connector-java:8.0.33"}}
    ],
    "new_cluster": {
      "spark_version": "15.4.x-scala2.12",
      "node_type_id": "i3.2xlarge",
      "autoscale": {"min_workers": 4, "max_workers": 20},
      "spark_conf": {
        "spark.sql.shuffle.partitions": "400",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
      }
    }
  }]
}'
```
