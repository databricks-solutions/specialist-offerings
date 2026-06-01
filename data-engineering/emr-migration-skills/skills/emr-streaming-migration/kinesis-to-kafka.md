# Kinesis Data Streams Migration Options

## Option 1: Continue with Kinesis on Databricks

If you want to keep using Kinesis Data Streams as your source, Databricks supports it via the Kinesis connector.

### Dependency

```
# Maven coordinates (add as cluster library)
com.qubole.spark:spark-sql-kinesis_2.12:1.2.0

# Or for newer versions, check:
# https://github.com/qubole/kinesis-sql
```

### EMR Code (Before)

```python
# EMR with DStream Kinesis receiver
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

kinesis_stream = KinesisUtils.createStream(
    ssc,
    appName="my-app",
    streamName="my-stream",
    endpointUrl="https://kinesis.us-east-1.amazonaws.com",
    regionName="us-east-1",
    initialPositionInStream=InitialPositionInStream.LATEST,
    checkpointInterval=10
)
kinesis_stream.foreachRDD(lambda rdd: process(rdd))
```

### Databricks Code (After -- Structured Streaming)

```python
# Databricks with Structured Streaming Kinesis source
kinesis_df = (
    spark.readStream
    .format("kinesis")
    .option("streamName", "my-stream")
    .option("region", "us-east-1")
    .option("initialPosition", "latest")
    .option("awsAccessKey", dbutils.secrets.get("aws", "access-key"))
    .option("awsSecretKey", dbutils.secrets.get("aws", "secret-key"))
    .load()
)

# Parse the Kinesis record (data is base64 encoded)
from pyspark.sql.functions import col, from_json, decode
parsed_df = kinesis_df.select(
    decode(col("data"), "UTF-8").alias("json_string"),
    col("streamName"),
    col("partitionKey"),
    col("approximateArrivalTimestamp")
)

# Write to Delta table
query = (
    parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/catalog/schema/checkpoints/kinesis-stream")
    .table("catalog.schema.kinesis_raw")
)
```

### Credential Configuration

```python
# Option A: Databricks secrets (RECOMMENDED)
.option("awsAccessKey", dbutils.secrets.get("aws-scope", "access-key"))
.option("awsSecretKey", dbutils.secrets.get("aws-scope", "secret-key"))

# Option B: Instance profile (if configured on Databricks)
# No explicit credentials needed -- uses instance profile attached to cluster

# Option C: Assume role
.option("awsSTSRoleARN", "arn:aws:iam::123456789012:role/KinesisReadRole")
.option("awsSTSSessionName", "databricks-kinesis-session")
```

---

## Option 2: Switch to MSK/Kafka

If you are considering migrating away from Kinesis to Apache Kafka (Amazon MSK), this provides better native Spark integration.

### Why Switch to Kafka?

- Native Spark Kafka connector (no third-party dependency)
- Better offset management
- More mature ecosystem
- Lower cost at high throughput
- Easier local development and testing

### Kinesis to MSK Migration Considerations

1. **Data format**: Kinesis records are base64-encoded blobs; Kafka records have key/value pairs
2. **Partitioning**: Kinesis uses partition keys (hash-based); Kafka uses explicit partitions
3. **Retention**: Kinesis default 24h (up to 365 days); Kafka configurable per topic
4. **Ordering**: Both guarantee ordering within a shard/partition
5. **Consumer groups**: Kinesis has application names; Kafka has consumer groups

### Kafka Connector on Databricks

```python
# Read from Kafka (MSK or any Kafka cluster)
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "b-1.msk-cluster.kafka.us-east-1.amazonaws.com:9092")
    .option("subscribe", "my-topic")
    .option("startingOffsets", "latest")
    .option("kafka.security.protocol", "SSL")
    .option("kafka.ssl.truststore.location", "/tmp/kafka.client.truststore.jks")
    .load()
)

# Parse Kafka value (typically JSON)
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, TimestampType

schema = StructType() \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", TimestampType())

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Write to Delta
query = (
    parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/catalog/schema/checkpoints/kafka-topic")
    .table("catalog.schema.events_raw")
)
```

---

## Option 3: Use Auto Loader for S3-Based Streaming

If Kinesis writes to S3 via Kinesis Data Firehose, you can use Auto Loader instead of reading from Kinesis directly. This is often simpler and more cost-effective.

### Architecture Change

```
BEFORE: Producer -> Kinesis -> Spark Streaming (EMR) -> S3/Hive
AFTER:  Producer -> Kinesis -> Firehose -> S3 -> Auto Loader (Databricks) -> Delta
```

### Auto Loader Code

```python
# Read new files from S3 as they arrive (Auto Loader)
raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")  # or parquet, csv, etc.
    .option("cloudFiles.schemaLocation", "/Volumes/catalog/schema/schemas/firehose-stream")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("s3://my-bucket/firehose-output/")
)

# Write to Delta table (bronze layer)
query = (
    raw_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/catalog/schema/checkpoints/firehose-autoloader")
    .option("mergeSchema", "true")
    .table("catalog.schema.firehose_raw")
)
```

### Auto Loader Benefits Over Direct Kinesis

- **Schema evolution**: Automatically handles new fields
- **Exactly-once**: File-based processing is inherently idempotent
- **Cost**: No Kinesis shard costs; only S3 storage and Firehose delivery
- **Backfill**: Easy to reprocess historical files
- **Scale**: Handles millions of files efficiently with file notification mode
