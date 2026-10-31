# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Compare two as-of join results
# MAGIC
# MAGIC Row-level reconciliation of two result tables (native Spark vs Feature Engineering, PyKX vs a warehouse export,
# MAGIC ...). Two normalisations are applied before comparing, because they caused every false mismatch we met:
# MAGIC
# MAGIC * **Timestamp precision.** Engines differ in sub-millisecond handling; both sides are truncated to the unit in
# MAGIC   `truncate_time_to` (`millisecond` by default, `none` to disable).
# MAGIC * **Time zone of an imported side.** Exports from systems that write wall-clock time land in the session zone. Set
# MAGIC   `right_time_zone` (e.g. `America/Los_Angeles`) to convert the right side to UTC before comparing.

# COMMAND ----------

dbutils.widgets.text("left_table", "main.asof_demo.asof_native")
dbutils.widgets.text("right_table", "main.asof_demo.asof_feature_engineering")
dbutils.widgets.text("key_cols", "sym,time")
dbutils.widgets.text("compare_cols", "price,size,ask,bid")
dbutils.widgets.dropdown("truncate_time_to", "millisecond", ["none", "second", "millisecond", "microsecond"])
dbutils.widgets.text("right_time_zone", "", "time zone of the right side's wall-clock timestamps (empty = already UTC)")

left_table = dbutils.widgets.get("left_table")
right_table = dbutils.widgets.get("right_table")
key_cols = [c.strip() for c in dbutils.widgets.get("key_cols").split(",") if c.strip()]
compare_cols = [c.strip() for c in dbutils.widgets.get("compare_cols").split(",") if c.strip()]
truncate_time_to = dbutils.widgets.get("truncate_time_to")
right_time_zone = dbutils.widgets.get("right_time_zone")
TIME_COL = "time"

# COMMAND ----------

from pyspark.sql import functions as F


def normalise(table: str, time_zone: str = ""):
    df = spark.table(table)
    t = F.col(TIME_COL)
    if time_zone:
        t = F.to_utc_timestamp(t, time_zone)
    if truncate_time_to != "none":
        t = F.date_trunc(truncate_time_to, t)
    return df.withColumn(TIME_COL, t).select(*key_cols, *compare_cols)


left = normalise(left_table)
right = normalise(right_table, right_time_zone)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row counts, time range and symmetric difference

# COMMAND ----------

summary = (
    left.agg(F.lit(left_table).alias("table"), F.count("*").alias("rows"), F.min(TIME_COL).alias("min_time"), F.max(TIME_COL).alias("max_time"))
    .unionByName(right.agg(F.lit(right_table).alias("table"), F.count("*").alias("rows"), F.min(TIME_COL).alias("min_time"), F.max(TIME_COL).alias("max_time")))
)
display(summary)

# COMMAND ----------

left_only = left.exceptAll(right)
right_only = right.exceptAll(left)
print(f"rows only in left  ({left_table}): {left_only.count()}")
print(f"rows only in right ({right_table}): {right_only.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mismatches side by side
# MAGIC Keys present in both tables whose compared columns differ. An empty result means the two engines agree.

# COMMAND ----------

l, r = left.alias("l"), right.alias("r")
joined = l.join(r, [F.col(f"l.{k}") == F.col(f"r.{k}") for k in key_cols], "full_outer")
differs = F.lit(False)
for c in compare_cols:
    differs = differs | ~(F.col(f"l.{c}").eqNullSafe(F.col(f"r.{c}")))
mismatches = joined.filter(differs).select(
    *[F.coalesce(F.col(f"l.{k}"), F.col(f"r.{k}")).alias(k) for k in key_cols],
    *[F.col(f"l.{c}").alias(f"left_{c}") for c in compare_cols],
    *[F.col(f"r.{c}").alias(f"right_{c}") for c in compare_cols],
)
print("mismatching keys:", mismatches.count())
display(mismatches.orderBy(*key_cols).limit(50))
