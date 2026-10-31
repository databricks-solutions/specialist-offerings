# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Generate synthetic trades and quotes
# MAGIC
# MAGIC Creates two Delta tables shaped like consolidated tick data so every notebook in this folder runs end to end
# MAGIC without access to a vendor feed:
# MAGIC
# MAGIC | table | columns |
# MAGIC |---|---|
# MAGIC | `<catalog>.<schema>.<trades_table>` | `sym STRING, time TIMESTAMP, price DOUBLE, size BIGINT` |
# MAGIC | `<catalog>.<schema>.<quotes_table>` | `sym STRING, time TIMESTAMP, ask DOUBLE, asize BIGINT, bid DOUBLE, bsize BIGINT` |
# MAGIC
# MAGIC One symbol (`skew_symbol`) receives `skew_factor` times more quotes than the others, so the skewed-symbol path in
# MAGIC `01_native_spark_asof_join` has something to work on. Both tables are liquid-clustered on `(sym, time)`, the layout
# MAGIC every approach in this folder benefits from. Point the widgets at your own tables instead if you already have tick data.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "asof_demo")
dbutils.widgets.text("trades_table", "equity_trades")
dbutils.widgets.text("quotes_table", "equity_quotes")
dbutils.widgets.text("num_symbols", "20")
dbutils.widgets.text("quotes_per_symbol", "200000")
dbutils.widgets.text("trades_per_symbol", "20000")
dbutils.widgets.text("skew_symbol", "SYM00")
dbutils.widgets.text("skew_factor", "25")
dbutils.widgets.text("start_date", "2024-01-02")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
trades_table = f"{catalog}.{schema}.{dbutils.widgets.get('trades_table')}"
quotes_table = f"{catalog}.{schema}.{dbutils.widgets.get('quotes_table')}"
num_symbols = int(dbutils.widgets.get("num_symbols"))
quotes_per_symbol = int(dbutils.widgets.get("quotes_per_symbol"))
trades_per_symbol = int(dbutils.widgets.get("trades_per_symbol"))
skew_symbol = dbutils.widgets.get("skew_symbol")
skew_factor = int(dbutils.widgets.get("skew_factor"))
start_date = dbutils.widgets.get("start_date")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

from pyspark.sql import functions as F

SECONDS_IN_WINDOW = 5 * 24 * 3600  # five trading days of wall-clock time
symbols = spark.range(num_symbols).select(F.format_string("SYM%02d", F.col("id")).alias("sym"))


def events(per_symbol: int, seed: int):
    """One row per event: (sym, time, mid). Times are uniform over the window; mid prices sit around a per-symbol base."""
    weighted = symbols.withColumn(
        "n", F.when(F.col("sym") == skew_symbol, F.lit(per_symbol * skew_factor)).otherwise(F.lit(per_symbol))
    )
    exploded = weighted.select("sym", F.explode(F.sequence(F.lit(1), F.col("n"))).alias("i"))
    base_price = 20 + (F.abs(F.xxhash64("sym")) % 400).cast("double")
    start_epoch = F.unix_timestamp(F.to_timestamp(F.lit(start_date)))
    return exploded.select(
        "sym",
        F.timestamp_seconds(start_epoch + F.floor(F.rand(seed) * SECONDS_IN_WINDOW)).alias("time"),
        (base_price + 2 * F.randn(seed + 1)).alias("mid"),
    )


quotes = (
    events(quotes_per_symbol, seed=11)
    .withColumn("spread", 0.01 + F.rand(13) * 0.05)
    .select(
        "sym",
        "time",
        F.round(F.col("mid") + F.col("spread") / 2, 4).alias("ask"),
        (1 + F.floor(F.rand(17) * 20)).cast("bigint").alias("asize"),
        F.round(F.col("mid") - F.col("spread") / 2, 4).alias("bid"),
        (1 + F.floor(F.rand(19) * 20)).cast("bigint").alias("bsize"),
    )
)

trades = events(trades_per_symbol, seed=23).select(
    "sym",
    "time",
    F.round(F.col("mid") + 0.02 * F.randn(29), 4).alias("price"),
    (1 + F.floor(F.rand(31) * 1000)).cast("bigint").alias("size"),
)

# COMMAND ----------

for name, df in ((quotes_table, quotes), (trades_table, trades)):
    df.createOrReplaceTempView("_asof_source")
    spark.sql(f"CREATE OR REPLACE TABLE {name} CLUSTER BY (sym, time) AS SELECT * FROM _asof_source")
    spark.sql(f"OPTIMIZE {name}")
    print(name, spark.table(name).count(), "rows")

# COMMAND ----------

display(
    spark.table(quotes_table).groupBy("sym").count().withColumnRenamed("count", "quotes")
    .join(spark.table(trades_table).groupBy("sym").count().withColumnRenamed("count", "trades"), "sym")
    .orderBy(F.desc("quotes"))
)
