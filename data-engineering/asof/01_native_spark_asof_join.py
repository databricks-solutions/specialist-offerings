# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · As-of join with native Spark
# MAGIC
# MAGIC Attach to every trade the latest quote (`ask`, `bid`) for the same symbol at or before the trade time, using only
# MAGIC Spark window functions. The logic lives in [`asof_spark.py`](asof_spark.py) next to this notebook (unit-tested in
# MAGIC `tests/`); this notebook wires it to tables.
# MAGIC
# MAGIC **Scenario 1 · no skew.** Up to roughly a million quotes per symbol: one window per symbol ordered by time is enough.
# MAGIC
# MAGIC **Scenario 2 · skewed symbols.** A handful of symbols carry tens of millions of quotes and their window becomes the
# MAGIC long pole. Split every symbol into `num_buckets` time ranges holding about the same number of quotes, assign each trade to
# MAGIC the range it falls in, and window over `(sym, bucket)` instead. The result is identical to scenario 1 (see
# MAGIC `tests/test_asof_spark.py::test_bucketed_join_matches_plain_join`); only the window sizes change.
# MAGIC
# MAGIC Semantics match kdb+ `aj`: a quote stamped at exactly the trade time is eligible. Pass `allow_equal_time=False` for
# MAGIC strictly earlier quotes.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "asof_demo")
dbutils.widgets.text("trades_table", "equity_trades")
dbutils.widgets.text("quotes_table", "equity_quotes")
dbutils.widgets.text("symbols", "", "symbols (comma-separated, empty = all)")
dbutils.widgets.text("output_table", "asof_native")
dbutils.widgets.text("num_buckets", "500")
dbutils.widgets.dropdown("persist_buckets", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
trades_table = f"{catalog}.{schema}.{dbutils.widgets.get('trades_table')}"
quotes_table = f"{catalog}.{schema}.{dbutils.widgets.get('quotes_table')}"
output_table = f"{catalog}.{schema}.{dbutils.widgets.get('output_table')}"
symbols = [s.strip() for s in dbutils.widgets.get("symbols").split(",") if s.strip()]
num_buckets = int(dbutils.widgets.get("num_buckets"))
persist_buckets = dbutils.widgets.get("persist_buckets") == "true"

KEY_COLS = ["sym"]
TIME_COL = "time"
QUOTE_VALUE_COLS = ["ask", "bid"]

# COMMAND ----------

from pyspark.sql import functions as F

from asof_spark import as_of_join, bucket_by_quote_density

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "auto")


def read(table: str):
    df = spark.table(table)
    return df.filter(F.col("sym").isin(symbols)) if symbols else df


trades_df = read(trades_table)
quotes_df = read(quotes_table).select(*KEY_COLS, TIME_COL, *QUOTE_VALUE_COLS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1 · one window per symbol

# COMMAND ----------

result = as_of_join(trades_df, quotes_df, KEY_COLS, TIME_COL, QUOTE_VALUE_COLS)
result.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)
display(spark.table(output_table).orderBy("sym", "time").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2 · bucket skewed symbols, then window over `(sym, bucket)`
# MAGIC
# MAGIC Bucketing is a one-off sort of the quotes per symbol (`ntile`) plus a range join of trades onto bucket boundaries.
# MAGIC When the same quotes are joined repeatedly, set `persist_buckets = true` to write the bucketed tables once,
# MAGIC clustered on `(sym, time, bucket)`, and reuse them.

# COMMAND ----------

trades_b, quotes_b = bucket_by_quote_density(trades_df, quotes_df, KEY_COLS, TIME_COL, num_buckets=num_buckets)

if persist_buckets:
    for name, df in ((f"{trades_table}_buckets", trades_b), (f"{quotes_table}_buckets", quotes_b)):
        df.createOrReplaceTempView("_asof_bucketed")
        spark.sql(f"CREATE OR REPLACE TABLE {name} CLUSTER BY (sym, time, bucket) AS SELECT * FROM _asof_bucketed")
    trades_b, quotes_b = spark.table(f"{trades_table}_buckets"), spark.table(f"{quotes_table}_buckets")

result_bucketed = as_of_join(trades_b, quotes_b, KEY_COLS, TIME_COL, QUOTE_VALUE_COLS, bucket_col="bucket").drop("bucket")
result_bucketed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{output_table}_bucketed")
display(spark.table(f"{output_table}_bucketed").orderBy("sym", "time").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks
# MAGIC Bucket sizes should be flat per symbol, and both scenarios must agree row for row.

# COMMAND ----------

display(quotes_b.groupBy("sym", "bucket").count().groupBy("sym").agg(F.min("count").alias("min_quotes_per_bucket"), F.max("count").alias("max_quotes_per_bucket")).orderBy("sym"))

# COMMAND ----------

a, b = spark.table(output_table), spark.table(f"{output_table}_bucketed")
print("rows:", a.count(), b.count())
print("rows only in scenario 1:", a.exceptAll(b).count())
print("rows only in scenario 2:", b.exceptAll(a).count())
