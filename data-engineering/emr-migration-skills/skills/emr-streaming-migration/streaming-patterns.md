# Common EMR Streaming Patterns to Databricks Equivalents

## 1. DStream WordCount to Structured Streaming Aggregation

### EMR (DStream -- DEPRECATED)

```python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "WordCount")
ssc = StreamingContext(sc, 5)  # 5-second batch interval

lines = ssc.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda line: line.split(" "))
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
word_counts.pprint()

ssc.start()
ssc.awaitTermination()
```

### Databricks (Structured Streaming)

```python
# spark is pre-initialized in Databricks
lines = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

from pyspark.sql.functions import explode, split, col

words = lines.select(explode(split(col("value"), " ")).alias("word"))
word_counts = words.groupBy("word").count()

query = (
    word_counts.writeStream
    .outputMode("complete")
    .format("console")
    .start()
)
query.awaitTermination()
```

---

## 2. DStream updateStateByKey to flatMapGroupsWithState

### EMR (DStream -- DEPRECATED)

```python
from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 10)
ssc.checkpoint("s3://bucket/checkpoint")

def update_running_count(new_values, running_count):
    return sum(new_values) + (running_count or 0)

lines = ssc.socketTextStream("localhost", 9999)
pairs = lines.map(lambda line: (line.split(",")[0], int(line.split(",")[1])))
running_counts = pairs.updateStateByKey(update_running_count)
running_counts.pprint()
```

### Databricks (Structured Streaming with flatMapGroupsWithState)

```python
from pyspark.sql.functions import col, split
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import StructType, StringType, LongType

# Define state update function
def update_running_count(key, values, state: GroupState):
    """Maintain running count per key."""
    if state.hasTimedOut:
        state.remove()
        return []
    
    current_count = state.get if state.exists else 0
    new_count = current_count + sum(v.value for v in values)
    state.update(new_count)
    return [(key[0], new_count)]

# Define schemas
input_schema = StructType().add("key", StringType()).add("value", LongType())
output_schema = StructType().add("key", StringType()).add("running_count", LongType())

lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
parsed = lines.select(
    split(col("value"), ",")[0].alias("key"),
    split(col("value"), ",")[1].cast("long").alias("value")
)

result = parsed.groupBy("key").applyInPandasWithState(
    update_running_count,
    outputStructType=output_schema,
    stateStructType=StructType().add("count", LongType()),
    outputMode="update",
    timeoutConf=GroupStateTimeout.NoTimeout
)

query = result.writeStream.outputMode("update").format("console").start()
```

---

## 3. Kinesis DStream to Structured Streaming with Kinesis Source

### EMR (DStream -- DEPRECATED)

```python
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

ssc = StreamingContext(sc, 10)

kinesis_stream = KinesisUtils.createStream(
    ssc,
    appName="my-kinesis-app",
    streamName="my-stream",
    endpointUrl="https://kinesis.us-east-1.amazonaws.com",
    regionName="us-east-1",
    initialPositionInStream=InitialPositionInStream.LATEST,
    checkpointInterval=10
)

# Process each RDD
def process_rdd(rdd):
    if not rdd.isEmpty():
        df = spark.read.json(rdd.map(lambda r: r.decode("utf-8")))
        df.write.mode("append").parquet("s3://bucket/output/")

kinesis_stream.foreachRDD(process_rdd)
ssc.start()
```

### Databricks (Structured Streaming)

```python
kinesis_df = (
    spark.readStream
    .format("kinesis")
    .option("streamName", "my-stream")
    .option("region", "us-east-1")
    .option("initialPosition", "latest")
    .load()
)

from pyspark.sql.functions import col, from_json, decode
from pyspark.sql.types import StructType, StringType, TimestampType

schema = StructType() \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", TimestampType())

parsed = kinesis_df.select(
    from_json(decode(col("data"), "UTF-8"), schema).alias("parsed")
).select("parsed.*")

query = (
    parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/catalog/schema/checkpoints/kinesis")
    .table("catalog.schema.kinesis_events")
)
```

---

## 4. File-Based Streaming to Auto Loader (cloudFiles)

### EMR (fileStream)

```python
# EMR: monitor S3 directory for new files
ssc = StreamingContext(sc, 60)

file_stream = ssc.textFileStream("s3://bucket/incoming/")
file_stream.foreachRDD(lambda rdd: process_files(rdd))
ssc.start()

# OR with Structured Streaming on EMR:
df = (
    spark.readStream
    .schema(my_schema)
    .json("s3://bucket/incoming/")
)
```

### Databricks (Auto Loader -- MUCH BETTER)

```python
# Auto Loader: efficient, scalable file ingestion
raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/catalog/schema/schemas/incoming")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("s3://bucket/incoming/")
)

query = (
    raw_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/catalog/schema/checkpoints/incoming")
    .option("mergeSchema", "true")
    .table("catalog.schema.incoming_raw")
)
```

**Why Auto Loader is better:**
- File notification mode (no directory listing needed)
- Automatic schema inference and evolution
- Handles millions of files efficiently
- Built-in exactly-once guarantees
- Rescue data column for malformed records

---

## 5. Streaming ETL (Bronze/Silver/Gold) to Delta Live Tables

### EMR (Manual Streaming ETL)

```python
# Bronze: raw ingestion
bronze = spark.readStream.schema(schema).json("s3://bucket/raw/")
bronze_query = bronze.writeStream.format("parquet").option("path", "s3://bucket/bronze/").start()

# Silver: cleaned data (separate job)
silver_input = spark.readStream.parquet("s3://bucket/bronze/")
silver = silver_input.filter(col("value").isNotNull()).dropDuplicates(["id"])
silver_query = silver.writeStream.format("parquet").option("path", "s3://bucket/silver/").start()

# Gold: aggregated (separate job)
gold_input = spark.readStream.parquet("s3://bucket/silver/")
gold = gold_input.groupBy("category").agg(sum("amount").alias("total"))
gold_query = gold.writeStream.format("parquet").outputMode("complete").option("path", "s3://bucket/gold/").start()
```

### Databricks (Delta Live Tables -- RECOMMENDED)

```python
import dlt
from pyspark.sql.functions import col, sum

# Bronze: raw ingestion with Auto Loader
@dlt.table(comment="Raw events from S3")
def bronze_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://bucket/raw/")
    )

# Silver: cleaned and validated
@dlt.table(comment="Cleaned events")
@dlt.expect_or_drop("valid_value", "value IS NOT NULL")
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
def silver_events():
    return dlt.read_stream("bronze_events").dropDuplicates(["id"])

# Gold: business aggregations
@dlt.table(comment="Category totals")
def gold_category_totals():
    return (
        dlt.read_stream("silver_events")
        .groupBy("category")
        .agg(sum("amount").alias("total_amount"))
    )
```

---

## 6. Streaming Joins (Stream-Stream Join)

### EMR

```python
# EMR: join two Kafka streams
orders = spark.readStream.format("kafka").option("subscribe", "orders").load()
payments = spark.readStream.format("kafka").option("subscribe", "payments").load()

# Watermark and join
orders_wm = orders.withWatermark("order_time", "1 hour")
payments_wm = payments.withWatermark("payment_time", "2 hours")

joined = orders_wm.join(
    payments_wm,
    expr("order_id = payment_order_id AND payment_time >= order_time AND payment_time <= order_time + interval 1 hour")
)

joined.writeStream.format("parquet").option("path", "s3://bucket/joined/").start()
```

### Databricks (same API, better sink)

```python
# Databricks: same join logic, Delta sink
orders = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", "orders")
    .load()
    .select(from_json(col("value").cast("string"), order_schema).alias("data"))
    .select("data.*")
    .withWatermark("order_time", "1 hour")
)

payments = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", "payments")
    .load()
    .select(from_json(col("value").cast("string"), payment_schema).alias("data"))
    .select("data.*")
    .withWatermark("payment_time", "2 hours")
)

from pyspark.sql.functions import expr

joined = orders.join(
    payments,
    expr("""
        order_id = payment_order_id 
        AND payment_time >= order_time 
        AND payment_time <= order_time + interval 1 hour
    """)
)

query = (
    joined.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/catalog/schema/checkpoints/order-payments")
    .table("catalog.schema.order_payments")
)
```

**Key Databricks improvements:**
- Delta table sink (ACID, time travel, schema evolution)
- Photon acceleration for joins
- Better autoscaling for variable workloads
- Built-in monitoring via Spark UI and Databricks metrics
