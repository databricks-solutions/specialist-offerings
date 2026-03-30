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

## SparkContext / SQLContext

```python
# Before
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

# After (Databricks)
# sc, spark, sqlContext are pre-initialized
# Replace hiveContext with spark
# Replace sqlContext with spark
```
