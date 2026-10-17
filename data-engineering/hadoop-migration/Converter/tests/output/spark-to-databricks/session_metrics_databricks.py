# Converted from legacy CDH PySpark (mechanical codemod)
# Catalog placeholder: main — confirm with customer
# Review: coalesce(1) removed, session init stripped for notebook use

spark.conf.set("spark.sql.shuffle.partitions", "10")

from pyspark.sql import functions as F

# Read enriched sessions from Hive
enriched = spark.table("main.retail_analytics.enriched_sessions")

# Compute daily aggregates by segment and device type
daily_agg = enriched \
    .groupBy("event_date", "customer_segment", "is_mobile") \
    .agg(
        F.countDistinct("session_id").alias("total_sessions"),
        F.countDistinct("user_id").alias("unique_users"),
        F.avg("total_events").alias("avg_events_per_session"),
        F.avg("total_session_time").alias("avg_session_duration"),
        F.avg("unique_pages").alias("avg_pages_per_session")
    )

# Write to Hive
daily_agg  \
    .write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("main.retail_analytics.daily_session_aggregates")

row_count = daily_agg.count()
print(f"SessionMetrics completed. Wrote {row_count} aggregate rows.")
