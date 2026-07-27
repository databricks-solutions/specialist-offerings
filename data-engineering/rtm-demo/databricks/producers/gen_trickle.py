# Databricks notebook source
# Low-rate TRICKLE producer: writes a small BATCH of rows to both source topics, then exits.
# Designed to be run on a 1-minute Databricks Job SCHEDULE (trigger via the job, not a stream).
#
# Why batch, not streaming: a continuous streaming producer permanently holds cluster task
# slots and starves the Real-Time Mode pipeline (which needs 6 free slots). A scheduled batch
# spins up, writes ~10 rows/topic, and releases its slots immediately — so UC-1 (RTM) and
# UC-2 keep running uninterrupted.
#
# Rows per run is configurable via the job parameter `rows_per_run` (default 10).

# COMMAND ----------
# MAGIC %pip install dbldatagen
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./kafka_common

# COMMAND ----------
import dbldatagen as dg
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType

try:
    ROWS = int(dbutils.widgets.get("rows_per_run"))
except Exception:
    ROWS = 10

# COMMAND ----------
# --- txn-source batch (bank transactions, ~5% fraud-shaped) ---
txn_spec = (
    dg.DataGenerator(spark, name="txn_trickle", rows=ROWS, partitions=1)
    .withColumn("txn_id", StringType(), expr="uuid()", baseColumn="id")
    .withColumn("_acct_num", IntegerType(), minValue=100000, maxValue=999999, random=True)
    .withColumn("account_id", StringType(), expr="concat('ACCT-', cast(_acct_num as string))")
    .withColumn("_fraud_flag", IntegerType(), minValue=0, maxValue=99, random=True)
    .withColumn("amount", DoubleType(),
                expr="case when _fraud_flag < 5 then round(rand()*9000+1000,2) "
                     "else round(rand()*300+5,2) end")
    .withColumn("merchant", StringType(),
                values=["AMAZON", "WALMART", "STARBUCKS", "SHELL", "APPLE",
                        "UNKNOWN-INTL", "CASINO-XYZ", "WIRE-TRANSFER"], random=True)
    .withColumn("merchant_category", StringType(),
                values=["retail", "grocery", "fuel", "dining", "cash", "transfer"], random=True)
    .withColumn("country", StringType(),
                expr="case when _fraud_flag < 5 then element_at(array('NG','RU','CN','BR'), "
                     "cast(rand()*4+1 as int)) else 'US' end")
    .withColumn("channel", StringType(), values=["pos", "online", "atm", "mobile"], random=True)
)
txn = (txn_spec.build()
       .withColumn("event_ts", F.current_timestamp())
       .select(F.col("account_id").alias("key"),
               F.to_json(F.struct("txn_id", "account_id", "amount", "merchant",
                                  "merchant_category", "country", "channel", "event_ts")).alias("value")))
txn.write.format("kafka").options(**kafka_write_options("txn-source")).save()

# COMMAND ----------
# --- events-source batch (messy generic events) ---
evt_spec = (
    dg.DataGenerator(spark, name="evt_trickle", rows=ROWS, partitions=1)
    .withColumn("event_id", StringType(), expr="uuid()", baseColumn="id")
    .withColumn("event_type", StringType(),
                values=["  Login ", "LOGOUT", "page_view", "Add_To_Cart ", " checkout"], random=True)
    .withColumn("_uid_num", IntegerType(), minValue=1000, maxValue=9999, random=True)
    .withColumn("user_id", StringType(), expr="concat('user_', cast(_uid_num as string))")
    .withColumn("source_system", StringType(),
                values=["web", "ios", "android", "partner-api"], random=True)
    .withColumn("_n", IntegerType(), minValue=1, maxValue=1000, random=True)
)
evt = (evt_spec.build()
       .withColumn("event_ts", F.current_timestamp().cast("string"))
       .withColumn("payload_json",
                   F.to_json(F.struct(F.col("_n").alias("seq"), F.lit("synthetic").alias("origin"))))
       .select(F.col("user_id").alias("key"),
               F.to_json(F.struct("event_id", "event_type", "user_id", "source_system",
                                  "payload_json", "event_ts")).alias("value")))
evt.write.format("kafka").options(**kafka_write_options("events-source")).save()

# COMMAND ----------
dbutils.notebook.exit(f"wrote {ROWS} rows each to txn-source and events-source")
