# Databricks notebook source
# UC-2 producer: light-weight generic events -> Kafka `events-source`.
# Intentionally "messy" (mixed case event types, whitespace, mixed ts formats) so the UC-2
# pipeline's standardization step has something real to normalize.
#
# COMMAND ----------
# MAGIC %pip install dbldatagen
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./kafka_common

# COMMAND ----------
import os
import dbldatagen as dg
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
# CFG, kafka_write_options, checkpoint come from %run ./kafka_common above.

ROWS_PER_SECOND = int(os.environ.get("EVT_ROWS_PER_SEC", "100"))
TOPIC = "events-source"

# COMMAND ----------
spec = (
    dg.DataGenerator(spark, name="generic_events", rows=1_000_000, partitions=6)
    .withColumn("event_id", StringType(), expr="uuid()", baseColumn="id")
    # deliberately inconsistent casing / spacing to exercise standardization
    .withColumn("event_type", StringType(),
                values=["  Login ", "LOGOUT", "page_view", "Add_To_Cart ", " checkout"],
                random=True)
    .withColumn("_uid_num", IntegerType(), minValue=1000, maxValue=9999, random=True)
    .withColumn("user_id", StringType(), expr="concat('user_', cast(_uid_num as string))")
    .withColumn("source_system", StringType(),
                values=["web", "ios", "android", "partner-api"], random=True)
    .withColumn("_n", IntegerType(), minValue=1, maxValue=1000, random=True)
)

stream_df = spec.build(withStreaming=True, options={"rowsPerSecond": ROWS_PER_SECOND})

# COMMAND ----------
payload = (
    stream_df
    .withColumn("event_ts", F.current_timestamp().cast("string"))  # string ts on purpose
    .withColumn("payload_json",
                F.to_json(F.struct(F.col("_n").alias("seq"),
                                   F.lit("synthetic").alias("origin"))))
    .select(
        F.col("user_id").alias("key"),
        F.to_json(F.struct(
            "event_id", "event_type", "user_id", "source_system",
            "payload_json", "event_ts"
        )).alias("value"),
    )
)

query = (
    payload.writeStream
    .format("kafka")
    .options(**kafka_write_options(TOPIC))
    .option("checkpointLocation", checkpoint("producer_events_source"))
    .trigger(processingTime="1 second")
    .start()
)

print(f"Producing ~{ROWS_PER_SECOND}/s generic events -> {TOPIC} on {CFG.BOOTSTRAP}")
query.awaitTermination()
