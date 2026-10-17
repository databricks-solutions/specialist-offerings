# SparkSession Migration: OSS Spark → Databricks

## Key Principle

In Databricks notebooks, `spark` is pre-initialized. In Databricks Jobs (JARs/wheels), minimal init is needed.

## Notebook Context

```python
# Before (OSS Spark)
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("yarn") \
    .appName("MyETL") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("hive.metastore.uris", "thrift://hiveserver:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# After (Databricks notebook)
# spark is pre-initialized — just use it
# Unity Catalog is the default metastore
# No master, no warehouse dir, no metastore config needed

# If you need specific configs:
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

## JAR/Wheel Job Context

```scala
// Before (OSS Spark — Scala)
val spark = SparkSession.builder()
  .master("yarn")
  .appName("ETLJob")
  .config("spark.executor.memory", "4g")
  .config("spark.executor.instances", "10")
  .enableHiveSupport()
  .getOrCreate()

// After (Databricks JAR job)
val spark = SparkSession.builder()
  .appName("ETLJob")
  .getOrCreate()
// master, executor config, Hive support handled by cluster/job config
```

## Configurations to Remove

| Config | Reason |
|--------|--------|
| `spark.master` | Managed by Databricks |
| `spark.submit.deployMode` | Always cluster mode |
| `spark.yarn.*` | YARN not used |
| `spark.hadoop.fs.defaultFS` | Use UC/DBFS paths |
| `spark.executor.instances` | Auto-scaling handles this |
| `spark.executor.memory` | Set in cluster config |
| `spark.driver.memory` | Set in cluster config |
| `hive.metastore.uris` | Unity Catalog replaces this |
| `spark.sql.warehouse.dir` | Managed by UC |

## Configurations to Update

| OSS Config | Databricks Equivalent |
|-----------|----------------------|
| `spark.sql.shuffle.partitions=200` | Keep or use adaptive: `spark.sql.adaptive.enabled=true` |
| `spark.serializer=org.apache.spark.serializer.KryoSerializer` | Keep (still valid) |
| `spark.sql.sources.partitionOverwriteMode=dynamic` | Keep (same in Databricks) |

## SparkContext / HiveContext / SQLContext (legacy CDH)

Legacy CDH and Spark 1.x jobs often use the pre-SparkSession API. See `PYSPARK_MIGRATION.md` for the full checklist.

```python
# Before (CDH 5.x / Spark 1.6)
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

conf = SparkConf().setAppName("MyETL")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)
sqlContext.setConf("spark.sql.shuffle.partitions", "10")

df = sqlContext.read.json("hdfs:///data/raw/events/")
sqlContext.sql("SELECT * FROM retail_analytics.events")

# After (Databricks notebook)
# spark is pre-initialized — remove all SparkContext/HiveContext imports
spark.conf.set("spark.sql.shuffle.partitions", "10")

df = spark.read.json("/Volumes/main/raw/events/")
spark.sql("SELECT * FROM main.retail_analytics.events")
```

### Variable rename map

| Legacy | Databricks |
|--------|------------|
| `sc` | Not needed (or `spark.sparkContext` if required) |
| `sqlContext` | `spark` |
| `hiveContext` | `spark` |
| `sqlContext.read.*` | `spark.read.*` |
| `sqlContext.sql(...)` | `spark.sql(...)` |
| `sqlContext.setConf(k, v)` | `spark.conf.set(k, v)` |
| `sc.stop()` | Remove (notebooks) or `spark.stop()` (jobs) |

### SparkConf settings

```python
# Before — configs on SparkConf
conf = SparkConf().setAppName("ETL").set("spark.sql.shuffle.partitions", "10")
sc = SparkContext(conf=conf)

# After — configs on spark session
spark = SparkSession.builder.appName("ETL").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "10")
```

Do not port `SparkConf` entries for YARN, HDFS default FS, or Hive metastore URI — see configurations table above.
