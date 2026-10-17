"""Easy-tier PySpark: DataFrame API only."""

from pyspark.sql import functions as F


def main(spark):
    df = spark.read.parquet("/Volumes/main/raw/sales/")
    result = df.groupBy("region").agg(F.sum("amount").alias("total"))
    result.write.mode("overwrite").saveAsTable("sales_by_region")
