# Databricks notebook source
# UC-1 validation: read `fraud-scored`, measure end-to-end latency and confirm fraud flags.
# Batch read (startingOffsets=earliest) so we can compute stats over what landed.

# COMMAND ----------
import sys, os
sys.path.append(os.path.abspath(".."))
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType,
                               LongType, BooleanType, TimestampType)

try:
    from kafka_common import kafka_read_options, CFG
except ImportError:
    sys.path.append("/Workspace/Repos/customer_rtm/databricks")
    from kafka_common import kafka_read_options, CFG

TOPIC = "fraud-scored"

schema = StructType([
    StructField("txn_id", StringType()),
    StructField("account_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant", StringType()),
    StructField("country", StringType()),
    StructField("channel", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("sig_large_amount", LongType()),
    StructField("sig_foreign_geo", LongType()),
    StructField("sig_risky_merchant", LongType()),
    StructField("fraud_score", LongType()),
    StructField("is_fraud", BooleanType()),
    StructField("scored_at", TimestampType()),
])

# COMMAND ----------
opts = kafka_read_options(TOPIC, starting_offsets="earliest")
raw = spark.read.format("kafka").options(**opts).load()

df = (raw.select(
        F.col("timestamp").alias("kafka_arrival_ts"),
        F.from_json(F.col("value").cast("string"), schema).alias("d"))
      .select("kafka_arrival_ts", "d.*"))

total = df.count()
print(f"messages on {TOPIC}: {total}")

if total:
    # end-to-end latency = scored_at - event_ts (producer->RTM->here proxy via scored_at)
    lat = df.withColumn("latency_ms",
                        (F.col("scored_at").cast("double") - F.col("event_ts").cast("double")) * 1000)
    lat.select(
        F.expr("percentile(latency_ms, 0.5)").alias("p50_ms"),
        F.expr("percentile(latency_ms, 0.95)").alias("p95_ms"),
        F.expr("percentile(latency_ms, 0.99)").alias("p99_ms"),
        F.max("latency_ms").alias("max_ms"),
    ).show(truncate=False)

    df.groupBy("is_fraud").count().show()
    print("sample fraud-flagged:")
    df.filter("is_fraud").select(
        "txn_id", "account_id", "amount", "country", "merchant",
        "fraud_score", "sig_large_amount", "sig_foreign_geo",
        "sig_risky_merchant").show(10, truncate=False)

    assert df.filter("is_fraud").count() > 0, "no fraud-flagged records — check scoring/producer"
    print("✅ UC-1 validation passed: fraud flags present, latency measured.")
else:
    print("⚠️ no messages yet — is the RTM pipeline + producer running?")
