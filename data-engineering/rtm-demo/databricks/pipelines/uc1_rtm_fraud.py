# Databricks notebook source
# UC-1: Real-Time Mode bank fraud detection.  Kafka `txn-source` -> score -> Kafka `fraud-scored`.
#
# MUST run on the classic Dedicated RTM cluster (databricks/clusters/rtm_cluster.json):
#   - DBR 16.4 LTS+, Photon, no autoscaling, SINGLE_USER access mode.
#   - RTM does NOT support forEachBatch or a Delta/table sink -> Kafka->Kafka only.
#   - outputMode MUST be "update"; trigger is .trigger(realTime="5 minutes").
#
# Scoring is expressed purely with column expressions + one windowed aggregation
# (velocity), all of which are RTM-supported operators.

# COMMAND ----------
# MAGIC %run ./kafka_common

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType,
                               TimestampType)
# CFG, kafka_read_options, kafka_write_options, checkpoint come from %run ./kafka_common.

SRC_TOPIC = "txn-source"
OUT_TOPIC = "fraud-scored"

# RTM state provider is configured at the CLUSTER level (rtm_cluster.json spark_conf):
#   spark.sql.streaming.stateStore.providerClass =
#     org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
# Photon comes from the cluster runtime_engine=PHOTON. Nothing to set here at runtime.

# COMMAND ----------
txn_schema = StructType([
    StructField("txn_id", StringType()),
    StructField("account_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant", StringType()),
    StructField("merchant_category", StringType()),
    StructField("country", StringType()),
    StructField("channel", StringType()),
    StructField("event_ts", TimestampType()),
])

raw = (
    spark.readStream.format("kafka")
    .options(**kafka_read_options(SRC_TOPIC, starting_offsets="latest", min_partitions=None))
    .load()
)

txns = (
    raw.select(F.from_json(F.col("value").cast("string"), txn_schema).alias("t"))
       .select("t.*")
)

# COMMAND ----------
# RESOLVED ON-CLUSTER: RTM rejects ALL streaming joins today
# ([STREAMING_REAL_TIME_MODE.STREAM_STREAM_JOIN_NOT_SUPPORTED]). So the velocity-window +
# join approach is out. We use the STATELESS FALLBACK: pure per-transaction column-expression
# scoring, which is unconditionally RTM-safe (selection/projection only, no state).
#
# Stateless per-txn rule signals (pure column expressions).
final = (
    txns
    .withColumn("sig_large_amount", (F.col("amount") > 1000).cast("int"))
    .withColumn("sig_foreign_geo", (~F.col("country").isin("US")).cast("int"))
    .withColumn("sig_risky_merchant",
                F.col("merchant").isin("WIRE-TRANSFER", "CASINO-XYZ", "UNKNOWN-INTL").cast("int"))
    # composite score from stateless signals
    .withColumn("fraud_score",
                F.col("sig_large_amount") * 2
                + F.col("sig_foreign_geo") * 2
                + F.col("sig_risky_merchant") * 3)
    .withColumn("is_fraud", (F.col("fraud_score") >= 4))
    .withColumn("scored_at", F.current_timestamp())
)

# COMMAND ----------
out = final.select(
    F.col("account_id").alias("key"),
    F.to_json(F.struct(
        "txn_id", "account_id", "amount", "merchant", "country", "channel",
        "event_ts",
        "sig_large_amount", "sig_foreign_geo", "sig_risky_merchant",
        "fraud_score", "is_fraud", "scored_at",
    )).alias("value"),
)

query = (
    out.writeStream
    .format("kafka")
    .options(**kafka_write_options(OUT_TOPIC))
    .option("checkpointLocation", checkpoint("uc1_rtm_fraud"))
    .outputMode("update")            # REQUIRED for RTM
    .trigger(realTime="5 minutes")   # <-- Real-Time Mode
    .start()
)

print(f"[RTM] {SRC_TOPIC} -> {OUT_TOPIC} on {CFG.BOOTSTRAP}")
query.awaitTermination()
