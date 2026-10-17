#!/bin/bash
# Spark submit script for the Retail ETL job

DATE_PARTITION=${1:-$(date +%Y-%m-%d)}
INPUT_BASE_PATH=${2:-"hdfs://namenode:8020/data/raw"}

spark-submit \
  --class com.example.etl.RetailETLJob \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 10 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=2 \
  --conf spark.dynamicAllocation.maxExecutors=20 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 \
  --conf spark.yarn.queue=etl-production \
  --jars hdfs://namenode:8020/lib/mysql-connector-java-8.0.28.jar \
  --files /etc/hive/conf/hive-site.xml \
  --packages org.apache.spark:spark-avro_2.12:3.3.0 \
  hdfs://namenode:8020/lib/retail-etl-1.0.0.jar \
  "$DATE_PARTITION" \
  "$INPUT_BASE_PATH"
