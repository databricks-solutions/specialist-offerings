package com.example.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Open-source Spark ETL job for retail analytics pipeline.
 * Reads from HDFS, transforms data, and writes to Hive tables.
 */
object RetailETLJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RetailETLJob")
      .master("yarn")
      .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.dynamicAllocation.minExecutors", "2")
      .config("spark.dynamicAllocation.maxExecutors", "20")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val datePartition = args(0)
    val inputBasePath = args(1)

    try {
      // Check if input data exists
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val inputPath = new Path(s"$inputBasePath/clickstream/$datePartition")
      if (!fs.exists(inputPath)) {
        println(s"No input data found at $inputPath. Exiting.")
        System.exit(0)
      }

      // Step 1: Read raw clickstream data from HDFS
      val rawClickstream = spark.read
        .format("json")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(s"hdfs://namenode:8020/data/raw/clickstream/$datePartition")

      // Step 2: Transform clickstream data
      val transformedClickstream = rawClickstream
        .filter(col("_corrupt_record").isNull)
        .drop("_corrupt_record")
        .withColumn("event_hour", hour(col("event_timestamp")))
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("page_category",
          when(col("page_url").contains("/product/"), "product_page")
            .when(col("page_url").contains("/cart"), "cart")
            .when(col("page_url").contains("/checkout"), "checkout")
            .otherwise("other"))
        .withColumn("is_mobile",
          col("user_agent").contains("Mobile") || col("user_agent").contains("Android"))

      // Step 3: Compute session metrics
      val windowSpec = Window.partitionBy("session_id").orderBy("event_timestamp")
      val sessionMetrics = transformedClickstream
        .withColumn("event_order", row_number().over(windowSpec))
        .withColumn("prev_timestamp", lag("event_timestamp", 1).over(windowSpec))
        .withColumn("time_on_page",
          unix_timestamp(col("event_timestamp")) - unix_timestamp(col("prev_timestamp")))
        .groupBy("session_id", "user_id", "event_date")
        .agg(
          count("*").as("total_events"),
          countDistinct("page_url").as("unique_pages"),
          sum("time_on_page").as("total_session_time"),
          first("event_timestamp").as("session_start"),
          last("event_timestamp").as("session_end"),
          collect_set("page_category").as("categories_visited"),
          max("is_mobile").as("is_mobile")
        )

      // Step 4: Read customer data from Hive
      val customers = spark.sql("SELECT * FROM retail_analytics.dim_customers WHERE is_active = true")

      // Step 5: Enrich sessions with customer data
      val enrichedSessions = sessionMetrics
        .join(customers, Seq("user_id"), "left")
        .withColumn("customer_segment",
          when(col("lifetime_value") > 10000, "premium")
            .when(col("lifetime_value") > 1000, "regular")
            .otherwise("new"))

      // Step 6: Write enriched sessions to Hive (partitioned)
      enrichedSessions
        .repartition(col("event_date"))
        .write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("event_date")
        .saveAsTable("retail_analytics.enriched_sessions")

      // Step 7: Compute daily aggregates
      val dailyAggregates = enrichedSessions
        .groupBy("event_date", "customer_segment", "is_mobile")
        .agg(
          countDistinct("session_id").as("total_sessions"),
          countDistinct("user_id").as("unique_users"),
          avg("total_events").as("avg_events_per_session"),
          avg("total_session_time").as("avg_session_duration"),
          avg("unique_pages").as("avg_pages_per_session")
        )

      dailyAggregates
        .coalesce(1)
        .write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable("retail_analytics.daily_session_aggregates")

      // Step 8: Write output marker file
      val outputPath = new Path(s"/data/processed/$datePartition/_SUCCESS")
      fs.create(outputPath).close()

      println(s"ETL pipeline completed successfully for $datePartition")

    } catch {
      case e: Exception =>
        println(s"ETL pipeline failed: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
}
