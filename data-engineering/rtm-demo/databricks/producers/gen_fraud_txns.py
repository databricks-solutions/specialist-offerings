# Databricks notebook source
# UC-1 producer: synthetic bank transactions -> Kafka `txn-source`.
# Uses dbldatagen streaming (rate source under the hood) to emit realistic transactions,
# a minority of which carry fraud-shaped signals (large amount, foreign geo, high velocity).
#
# Run on any cluster with network access to the broker. Install: %pip install dbldatagen
# COMMAND ----------
# MAGIC %pip install dbldatagen
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./kafka_common

# COMMAND ----------
import os
import dbldatagen as dg
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType, TimestampType
# CFG, kafka_write_options, checkpoint come from %run ./kafka_common above.

ROWS_PER_SECOND = int(os.environ.get("TXN_ROWS_PER_SEC", "200"))
TOPIC = "txn-source"

# COMMAND ----------
# Transaction spec. ~5% of rows are nudged into fraud-shaped territory via a fraud_flag
# selector, so downstream scoring has real positives to catch.
spec = (
    dg.DataGenerator(spark, name="bank_txns", rows=1_000_000, partitions=6)
    .withColumn("txn_id", StringType(), expr="uuid()", baseColumn="id")
    # account_id: format a random 6-digit number. (dbldatagen template \d needs a SINGLE
    # backslash; a raw string with \\d emits literal 'd'. Using expr is unambiguous.)
    .withColumn("_acct_num", IntegerType(), minValue=100000, maxValue=999999, random=True)
    .withColumn("account_id", StringType(), expr="concat('ACCT-', cast(_acct_num as string))")
    .withColumn("_fraud_flag", IntegerType(), minValue=0, maxValue=99, random=True)
    # amount: normal txns small; fraud-flagged txns large
    .withColumn("amount", DoubleType(),
                expr="case when _fraud_flag < 5 then round(rand()*9000+1000,2) "
                     "else round(rand()*300+5,2) end")
    .withColumn("merchant", StringType(),
                values=["AMAZON", "WALMART", "STARBUCKS", "SHELL", "APPLE",
                        "UNKNOWN-INTL", "CASINO-XYZ", "WIRE-TRANSFER"], random=True)
    .withColumn("merchant_category", StringType(),
                values=["retail", "grocery", "fuel", "dining", "cash", "transfer"],
                random=True)
    # geo: fraud-flagged skew to foreign country codes
    .withColumn("country", StringType(),
                expr="case when _fraud_flag < 5 then element_at(array('NG','RU','CN','BR'), "
                     "cast(rand()*4+1 as int)) else 'US' end")
    .withColumn("channel", StringType(),
                values=["pos", "online", "atm", "mobile"], random=True)
)

stream_df = spec.build(withStreaming=True, options={"rowsPerSecond": ROWS_PER_SECOND})

# COMMAND ----------
# Add event time + serialize to a JSON value; key by account for partition affinity.
payload = (
    stream_df
    .withColumn("event_ts", F.current_timestamp())
    .select(
        F.col("account_id").alias("key"),
        F.to_json(F.struct(
            "txn_id", "account_id", "amount", "merchant", "merchant_category",
            "country", "channel", "event_ts"
        )).alias("value"),
    )
)

query = (
    payload.writeStream
    .format("kafka")
    .options(**kafka_write_options(TOPIC))
    .option("checkpointLocation", checkpoint("producer_txn_source"))
    .trigger(processingTime="1 second")
    .start()
)

print(f"Producing ~{ROWS_PER_SECOND}/s bank txns -> {TOPIC} on {CFG.BOOTSTRAP}")
query.awaitTermination()
