# Databricks notebook source
# UC-2: Structured Streaming standardization -> managed Iceberg UC table.
#   Kafka `events-source` -> light standardization -> customer_rtm.streaming.events_std (Iceberg)
#   Bad records -> Kafka `events-dlq`.
#
# NOT Real-Time Mode: RTM cannot sink to a table. Standard micro-batch (processingTime).
# Runs on any DBR 16.4 LTS+ compute (serverless OK). Requires managed-Iceberg WRITE preview
# enabled on the workspace (see databricks/00_setup_uc.sql section 4).

# COMMAND ----------
# MAGIC %run ./kafka_common

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# CFG, kafka_read_options, kafka_write_options, checkpoint come from %run ./kafka_common.

SRC_TOPIC = "events-source"
DLQ_TOPIC = "events-dlq"
TARGET = f"{CFG.UC_CATALOG}.{CFG.UC_SCHEMA}.events_std"

# COMMAND ----------
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("source_system", StringType()),
    StructField("payload_json", StringType()),
    StructField("event_ts", StringType()),   # arrives as string; parsed below
])

raw = (
    spark.readStream.format("kafka")
    .options(**kafka_read_options(SRC_TOPIC, starting_offsets="latest"))
    .load()
)

parsed = raw.select(
    F.col("value").cast("string").alias("raw_value"),
    F.from_json(F.col("value").cast("string"), event_schema).alias("e"),
)

# Valid = parseable + has an event_id. Everything else -> DLQ.
valid = parsed.filter(F.col("e").isNotNull() & F.col("e.event_id").isNotNull())
invalid = parsed.filter(F.col("e").isNull() | F.col("e.event_id").isNull())

# COMMAND ----------
# Light standardization: trim + upper the event_type, coalesce nulls, parse ts, add ingest_ts.
standardized = valid.select(
    F.col("e.event_id").alias("event_id"),
    F.upper(F.trim(F.col("e.event_type"))).alias("event_type"),
    F.col("e.user_id").alias("user_id"),
    F.coalesce(F.col("e.source_system"), F.lit("unknown")).alias("source_system"),
    F.col("e.payload_json").alias("payload_json"),
    F.to_timestamp(F.col("e.event_ts")).alias("event_ts"),
    F.current_timestamp().alias("ingest_ts"),
)

# COMMAND ----------
# DLQ writer (batch write inside foreachBatch — fine here, this is NOT RTM).
dlq_opts = kafka_write_options(DLQ_TOPIC)

def route_batch(batch_df, batch_id):
    bad = (batch_df.filter(F.col("e").isNull() | F.col("e.event_id").isNull())
                   .select(F.to_json(F.struct(
                       F.col("raw_value"),
                       F.lit("SCHEMA_OR_ID_MISSING").alias("dlq_reason"),
                       F.current_timestamp().cast("string").alias("dlq_ts"),
                   )).alias("value")))
    if not bad.isEmpty():
        (bad.write.format("kafka").options(**dlq_opts).save())

# DLQ stream (separate query so the main table write stays a clean append).
dlq_query = (
    parsed.writeStream
    .foreachBatch(route_batch)
    .option("checkpointLocation", checkpoint("uc2_dlq"))
    .trigger(processingTime="30 seconds")
    .start()
)

# COMMAND ----------
# Main write -> managed Iceberg table (append).
# RESOLVED ON-CLUSTER: a direct streaming sink (.toTable / .format) into a managed Iceberg
# table fails with [MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED] "Managed Iceberg tables do not
# support Delta streaming writes." The supported pattern is foreachBatch doing a BATCH append
# (each micro-batch is a normal batch write, which managed Iceberg does support).
def write_iceberg_batch(batch_df, batch_id):
    (batch_df.write.format("iceberg").mode("append").saveAsTable(TARGET))

main_query = (
    standardized.writeStream
    .foreachBatch(write_iceberg_batch)
    .option("checkpointLocation", checkpoint("uc2_iceberg_standardize"))
    .trigger(processingTime="30 seconds")
    .start()
)

print(f"[micro-batch] {SRC_TOPIC} -> {TARGET} (Iceberg); bad -> {DLQ_TOPIC}")
main_query.awaitTermination()
