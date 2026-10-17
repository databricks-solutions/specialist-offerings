#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
session_metrics.py -- PySpark daily session aggregations.
Compatible with Spark 1.6 / CDH 5.7 (Python 2.6+)
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import functions as F


def main():
    conf = SparkConf().setAppName("SessionMetrics")
    sc = SparkContext(conf=conf)
    sqlContext = HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "10")

    try:
        # Read enriched sessions from Hive
        enriched = sqlContext.sql("SELECT * FROM retail_analytics.enriched_sessions")

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
        daily_agg \
            .coalesce(1) \
            .write \
            .mode("overwrite") \
            .format("parquet") \
            .saveAsTable("retail_analytics.daily_session_aggregates")

        row_count = daily_agg.count()
        print("SessionMetrics completed. Wrote %d aggregate rows." % row_count)

    except Exception as e:
        print("SessionMetrics failed: %s" % str(e))
        import traceback
        traceback.print_exc()
        raise
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
