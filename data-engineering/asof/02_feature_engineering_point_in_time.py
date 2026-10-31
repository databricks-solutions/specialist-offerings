# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · As-of join as a point-in-time lookup (Feature Engineering in Unity Catalog)
# MAGIC
# MAGIC Databricks Feature Engineering already implements the as-of join: register the quotes as a **time series feature
# MAGIC table** (primary key `sym`, timeseries column `time`) and ask for a training set keyed on the trades. The
# MAGIC point-in-time lookup returns, for each trade, the feature row with the latest `time <= trade.time`.
# MAGIC
# MAGIC Why consider it: zero join code, lineage recorded in Unity Catalog, and the same feature table serves model
# MAGIC training and online lookups. Set `use_spark_native_join=True` to have the lookup planned as a native Spark join
# MAGIC instead of the default algorithm; on large, liquid-clustered tables this was the faster path in our runs.
# MAGIC
# MAGIC Requires the `databricks-feature-engineering` package (installed below) and Unity Catalog.

# COMMAND ----------

# MAGIC %pip install -q databricks-feature-engineering

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "asof_demo")
dbutils.widgets.text("trades_table", "equity_trades")
dbutils.widgets.text("quotes_table", "equity_quotes")
dbutils.widgets.text("feature_table", "equity_quotes_features")
dbutils.widgets.text("output_table", "asof_feature_engineering")
dbutils.widgets.dropdown("use_spark_native_join", "true", ["true", "false"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
trades_table = f"{catalog}.{schema}.{dbutils.widgets.get('trades_table')}"
quotes_table = f"{catalog}.{schema}.{dbutils.widgets.get('quotes_table')}"
feature_table = f"{catalog}.{schema}.{dbutils.widgets.get('feature_table')}"
output_table = f"{catalog}.{schema}.{dbutils.widgets.get('output_table')}"
use_spark_native_join = dbutils.widgets.get("use_spark_native_join") == "true"

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Walk-through on three rows
# MAGIC `ts` on the feature side, `event_ts` on the lookup side. Row 1 looks up before the feature exists (null), row 2 at
# MAGIC the exact feature time (match), row 3 after it (match).

# COMMAND ----------

from pyspark.sql import functions as F

demo_features = f"{catalog}.{schema}.pit_demo_user_features"
features_df = spark.createDataFrame(
    [(1, "2023-10-01 00:00:00", 5, True), (2, "2023-10-02 00:00:00", 3, False), (3, "2023-10-03 00:00:00", 8, True)],
    "user_id INT, ts STRING, purchases_30d INT, is_free_trial_active BOOLEAN",
).withColumn("ts", F.to_timestamp("ts"))

if not spark.catalog.tableExists(demo_features):
    fe.create_table(name=demo_features, primary_keys=["user_id", "ts"], timeseries_column="ts", df=features_df)

events_df = spark.createDataFrame(
    [(1, "2023-09-30 23:59:59", True), (2, "2023-10-02 00:00:00", False), (3, "2023-10-04 00:00:00", True)],
    "user_id INT, event_ts STRING, did_click BOOLEAN",
).withColumn("event_ts", F.to_timestamp("event_ts"))

demo_set = fe.create_training_set(
    df=events_df,
    feature_lookups=[
        FeatureLookup(
            table_name=demo_features,
            feature_names=["purchases_30d", "is_free_trial_active"],
            lookup_key="user_id",
            timestamp_lookup_key="event_ts",
        )
    ],
    label="did_click",
)
display(demo_set.load_df().orderBy("user_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trades × quotes

# COMMAND ----------

quotes_df = spark.table(quotes_table)
trades_df = spark.table(trades_table)

if not spark.catalog.tableExists(feature_table):
    fe.create_table(
        name=feature_table,
        primary_keys=["sym", "time"],
        timeseries_column="time",
        df=quotes_df,
        description="Quotes as a time series feature table; point-in-time lookups give the latest quote at or before a trade",
    )
    # Same layout the other approaches rely on; the lookup prunes files on sym and time.
    spark.sql(f"ALTER TABLE {feature_table} CLUSTER BY (sym, time)")
    spark.sql(f"OPTIMIZE {feature_table}")

# COMMAND ----------

training_set = fe.create_training_set(
    df=trades_df,
    feature_lookups=[
        FeatureLookup(
            table_name=feature_table,
            feature_names=["ask", "asize", "bid", "bsize"],
            lookup_key="sym",
            timestamp_lookup_key="time",
        )
    ],
    label=None,
    exclude_columns=[],
    use_spark_native_join=use_spark_native_join,
)

training_set.load_df().write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spot check one trade
# MAGIC The quote carried onto the trade must be the newest one at or before the trade time.

# COMMAND ----------

sample = spark.table(output_table).filter(F.col("ask").isNotNull()).orderBy(F.rand(1)).limit(1).collect()[0]
print("trade:", sample.asDict())
display(
    spark.table(quotes_table)
    .filter((F.col("sym") == sample["sym"]) & (F.col("time") <= sample["time"]))
    .orderBy(F.desc("time"))
    .limit(3)
)
