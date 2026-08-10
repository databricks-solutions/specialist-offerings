#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
clickstream_transform.py -- PySpark version of RetailETLJob.scala
Compatible with Spark 1.6 / CDH 5.7 (Python 2.6+)
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main():
    conf = SparkConf().setAppName("ClickstreamTransform")
    sc = SparkContext(conf=conf)
    sqlContext = HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "10")

    try:
        # Step 1: Read raw clickstream from HDFS
        raw_clickstream = sqlContext.read.json("hdfs:///data/raw/clickstream/dt=*/hour=*/")

        # Step 2: Transform -- enrich with page categories using SQL expr
        transformed = raw_clickstream \
            .withColumn("event_hour", F.hour(F.col("event_timestamp"))) \
            .withColumn("page_category", F.expr(
                "CASE "
                "WHEN page_url LIKE '%/product/%' THEN 'product_page' "
                "WHEN page_url LIKE '%/cart%' THEN 'cart' "
                "WHEN page_url LIKE '%/checkout%' THEN 'checkout' "
                "ELSE 'other' END")) \
            .withColumn("is_mobile", F.expr(
                "user_agent LIKE '%Mobile%' OR user_agent LIKE '%Android%'"))

        # Step 3: Compute session metrics with window functions
        window_spec = Window.partitionBy("session_id").orderBy("event_timestamp")
        with_order = transformed \
            .withColumn("event_order", F.row_number().over(window_spec)) \
            .withColumn("prev_ts", F.lag("event_timestamp", 1).over(window_spec)) \
            .withColumn("time_on_page",
                F.unix_timestamp(F.col("event_timestamp")) - F.unix_timestamp(F.col("prev_ts")))

        session_metrics = with_order \
            .groupBy("session_id", "user_id", "event_date") \
            .agg(
                F.count("*").alias("total_events"),
                F.countDistinct("page_url").alias("unique_pages"),
                F.sum("time_on_page").alias("total_session_time"),
                F.first("event_timestamp").alias("session_start"),
                F.last("event_timestamp").alias("session_end"),
                F.max("is_mobile").alias("is_mobile")
            )

        # Step 4: Add customer segment
        enriched = session_metrics.withColumn("customer_segment", F.expr(
            "CASE "
            "WHEN total_events > 10 THEN 'premium' "
            "WHEN total_events > 3 THEN 'regular' "
            "ELSE 'new' END"))

        # Step 5: Write to Hive as parquet table
        enriched.write \
            .mode("overwrite") \
            .format("parquet") \
            .saveAsTable("retail_analytics.enriched_sessions")

        cnt = enriched.count()
        print("ClickstreamTransform completed. Rows: %d" % cnt)

    except Exception as e:
        print("ClickstreamTransform failed: %s" % str(e))
        import traceback
        traceback.print_exc()
        raise
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
