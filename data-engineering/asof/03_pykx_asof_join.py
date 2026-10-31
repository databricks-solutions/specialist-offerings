# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · As-of join with PyKX (kdb+ `aj`) inside a cogrouped pandas UDF
# MAGIC
# MAGIC For teams whose as-of logic already exists in q, run it unchanged on Databricks: Spark groups trades and quotes by
# MAGIC symbol, hands each group to a pandas UDF on a worker, and PyKX executes the q `aj` there. Spark does the
# MAGIC distribution and the Delta I/O; q does the join. No kdb+ server is involved.
# MAGIC
# MAGIC **License.** PyKX needs a kdb+ license file (`kc.lic` / `k4.lic`) to run q code. Put it in a location every worker can
# MAGIC read, for example a Unity Catalog volume (`/Volumes/<catalog>/<schema>/<volume>/lic/`), and point the `qlic_dir`
# MAGIC widget at that directory. Never commit the license to source control.
# MAGIC
# MAGIC **Compute.** Classic (non-serverless) compute. Each Spark task runs single-threaded q; raise `spark.task.cpus` only if
# MAGIC you also set `QARGS='-s <n>'` to give q the extra cores.

# COMMAND ----------

# MAGIC %pip install -q -U "pykx==3.1.5" "pytz>=2022.1" "toml~=0.10.2"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "asof_demo")
dbutils.widgets.text("trades_table", "equity_trades")
dbutils.widgets.text("quotes_table", "equity_quotes")
dbutils.widgets.text("symbols", "", "symbols (comma-separated, empty = all)")
dbutils.widgets.text("output_table", "asof_pykx")
dbutils.widgets.text("qlic_dir", "/Volumes/main/asof_demo/licenses/kx/", "directory holding kc.lic / k4.lic")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
trades_table = f"{catalog}.{schema}.{dbutils.widgets.get('trades_table')}"
quotes_table = f"{catalog}.{schema}.{dbutils.widgets.get('quotes_table')}"
output_table = f"{catalog}.{schema}.{dbutils.widgets.get('output_table')}"
symbols = [s.strip() for s in dbutils.widgets.get("symbols").split(",") if s.strip()]
QLIC_DIR = dbutils.widgets.get("qlic_dir")

# Schema of the pandas DataFrame the UDF returns: the trade columns followed by the quote columns `aj` adds.
# Adjust if your trades table has different columns or types.
RESULT_SCHEMA = "sym string, time timestamp, price double, size bigint, ask double, bid double"

# COMMAND ----------

# MAGIC %md
# MAGIC ## The q code, wrapped as a pandas UDF
# MAGIC `ajWrapper` sorts both tables by `time` within `sym`, applies the grouped (`g#`) and sorted (`s#`) attributes q expects
# MAGIC for a fast `aj`, and joins. Everything inside the function runs on the worker that owns the group.

# COMMAND ----------

Q_AJ_WRAPPER = """
ajWrapper:{[t;q]
  t:update `g#sym, `s#time from `time`sym xasc t;
  q:update `g#sym, `s#time from `time`sym xasc q;
  :aj[`sym`time; t; select sym, time, ask, bid from q]
 }
"""


def asof_join_udf_pykx(trade_pdf, quote_pdf):
    """Cogrouped pandas UDF: one call per symbol group (or a few symbols per partition)."""
    import os

    os.environ["QLIC"] = QLIC_DIR  # must be set before pykx is imported on the worker
    import pykx as kx

    kx.q(Q_AJ_WRAPPER)
    return kx.q["ajWrapper"](trade_pdf, quote_pdf).pd()

# COMMAND ----------

from pyspark.sql import functions as F

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


def read(table: str):
    df = spark.table(table)
    return df.filter(F.col("sym").isin(symbols)) if symbols else df


trades_df = read(trades_table).select("sym", "time", "price", "size")
quotes_df = read(quotes_table).select("sym", "time", "ask", "bid")

# Each symbol's trades and quotes land in the same task. To bound task size on very skewed symbols, repartition both
# sides on `sym` first, e.g. `.repartition(<num cores>, "sym")`; the UDF itself is unchanged.
result = trades_df.groupby("sym").cogroup(quotes_df.groupby("sym")).applyInPandas(asof_join_udf_pykx, schema=RESULT_SCHEMA)
result.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

display(spark.table(output_table).groupBy("sym").count().orderBy("sym"))

# COMMAND ----------

display(spark.table(output_table).orderBy("sym", "time").limit(10))
