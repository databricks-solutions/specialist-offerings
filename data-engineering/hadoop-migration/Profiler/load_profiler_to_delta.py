# Databricks notebook source
# MAGIC %md
# MAGIC # Load Hadoop Profiler DuckDB to Delta Tables
# MAGIC
# MAGIC Loads all tables from a DuckDB profiler export into Unity Catalog Delta tables.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Upload `hadoop_profiler.duckdb` to a Unity Catalog Volume
# MAGIC 2. Ensure you have `CREATE CATALOG` / `CREATE SCHEMA` privileges (or pre-create them)
# MAGIC
# MAGIC **Usage:** Set widget values and Run All.

# COMMAND ----------

# Configuration widgets
dbutils.widgets.text("catalog", "aa_catalog", "UC Catalog")
dbutils.widgets.text("schema", "hadoop_profiler", "Schema")
dbutils.widgets.text("duckdb_path", "/Volumes/aa_catalog/hadoop_profiler/profiler_uploads/hadoop_profiler.duckdb", "DuckDB File Path")
dbutils.widgets.dropdown("overwrite", "true", ["true", "false"], "Overwrite Existing Tables")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
duckdb_path = dbutils.widgets.get("duckdb_path")
overwrite = dbutils.widgets.get("overwrite") == "true"

print(f"Catalog:    {catalog}")
print(f"Schema:     {schema}")
print(f"DuckDB:     {duckdb_path}")
print(f"Overwrite:  {overwrite}")

# COMMAND ----------

# MAGIC %pip install duckdb

# COMMAND ----------

# Imports and DuckDB connection
import duckdb
import pandas as pd
import os

# Re-read widgets after Python restart
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
duckdb_path = dbutils.widgets.get("duckdb_path")
overwrite = dbutils.widgets.get("overwrite") == "true"

# Verify the file exists
if not os.path.exists(duckdb_path):
    raise FileNotFoundError(f"DuckDB file not found: {duckdb_path}")

file_size_mb = os.path.getsize(duckdb_path) / (1024 * 1024)
print(f"DuckDB file: {duckdb_path} ({file_size_mb:.1f} MB)")

# Connect read-only
duckdb_conn = duckdb.connect(duckdb_path, read_only=True)

# List all tables with row counts
table_rows = duckdb_conn.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
).fetchall()

source_counts = {}
for (tbl,) in table_rows:
    cnt = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0]
    source_counts[tbl] = cnt

summary_df = pd.DataFrame(
    [{"table": t, "rows": c} for t, c in source_counts.items()]
)
print(f"\nDuckDB tables: {len(source_counts)}, Total rows: {sum(source_counts.values())}")
display(spark.createDataFrame(summary_df))

# COMMAND ----------

# Create catalog and schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")
print(f"Using: {catalog}.{schema}")

# Clean up compatibility views from previous runs (for idempotent re-load)
if overwrite:
    _COMPAT_VIEWS = [
        "yarn_analysis", "yarn_analysis_vw", "oozie_analysis_vw",
        "hourly_yarn_view", "yarn_nodes", "cm_hosts",
        "impala_extract", "cm_hostroles", "cm_roleconfig",
        "cm_clustercpu", "cm_clustermemory", "cm_hdfsstats",
        "cm_ts_impala_utlization", "cm_ts_yarn_memory_allocation",
    ]
    _RENAMED_TABLES = [
        ("_yarn_nodes", "yarn_nodes"),
        ("_cm_hosts", "cm_hosts"),
        ("_yarn_analysis_vw", "yarn_analysis_vw"),
        ("_oozie_analysis_vw", "oozie_analysis_vw"),
        ("_hourly_yarn_view", "hourly_yarn_view"),
    ]
    for v in _COMPAT_VIEWS:
        try:
            spark.sql(f"DROP VIEW IF EXISTS `{catalog}`.`{schema}`.`{v}`")
        except Exception:
            pass
    for base_name, original_name in _RENAMED_TABLES:
        try:
            spark.sql(f"ALTER TABLE `{catalog}`.`{schema}`.`{base_name}` RENAME TO `{catalog}`.`{schema}`.`{original_name}`")
        except Exception:
            pass
    print("Cleaned up previous compatibility views")

# COMMAND ----------

# Load all DuckDB tables to Delta
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    DoubleType, BooleanType, TimestampType,
)

# Map DuckDB types to Spark types for empty-table schema inference
_DUCKDB_TO_SPARK = {
    "VARCHAR": StringType(), "TEXT": StringType(),
    "BIGINT": LongType(), "INTEGER": IntegerType(), "SMALLINT": IntegerType(),
    "DOUBLE": DoubleType(), "FLOAT": DoubleType(),
    "BOOLEAN": BooleanType(),
    "TIMESTAMP": TimestampType(), "TIMESTAMP WITH TIME ZONE": TimestampType(),
}

def _spark_schema_from_duckdb(conn, table):
    """Read column metadata from DuckDB and build a Spark StructType."""
    cols = conn.execute(
        f"SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_name='{table}' AND table_schema='main' ORDER BY ordinal_position"
    ).fetchall()
    fields = [
        StructField(name, _DUCKDB_TO_SPARK.get(dtype.upper(), StringType()), True)
        for name, dtype in cols
    ]
    return StructType(fields)

write_mode = "overwrite" if overwrite else "error"
loaded = []
failed = []

for table_name, expected_rows in source_counts.items():
    try:
        pdf = duckdb_conn.execute(f'SELECT * FROM "{table_name}"').fetchdf()
        if pdf.empty:
            spark_schema = _spark_schema_from_duckdb(duckdb_conn, table_name)
            sdf = spark.createDataFrame([], spark_schema)
        else:
            sdf = spark.createDataFrame(pdf)
        full_name = f"{catalog}.{schema}.{table_name}"
        sdf.write.format("delta").mode(write_mode).saveAsTable(full_name)
        actual = sdf.count()
        loaded.append({"table": table_name, "rows": actual})
        print(f"  OK  {table_name}: {actual} rows")
    except Exception as e:
        failed.append({"table": table_name, "error": str(e)})
        print(f"  FAIL {table_name}: {e}")

print(f"\nLoaded {len(loaded)}/{len(source_counts)} tables")
if failed:
    print(f"Failed: {[f['table'] for f in failed]}")

# COMMAND ----------

# Create compatibility views matching original profiler table schema.
# The DuckDB exporter uses snake_case columns and different table names.
# The Lakeview dashboard expects the original camelCase column names.
# These views bridge the gap so dashboard SQL works without modification.

print("Creating compatibility views for dashboard...")

# Step 1: Rename tables that share names with original schema but have different columns
_TABLE_RENAMES = {
    "yarn_nodes": "_yarn_nodes",
    "cm_hosts": "_cm_hosts",
    "yarn_analysis_vw": "_yarn_analysis_vw",
    "oozie_analysis_vw": "_oozie_analysis_vw",
    "hourly_yarn_view": "_hourly_yarn_view",
}
for original_name, base_name in _TABLE_RENAMES.items():
    try:
        spark.sql(f"ALTER TABLE `{catalog}`.`{schema}`.`{original_name}` RENAME TO `{catalog}`.`{schema}`.`{base_name}`")
        print(f"  Renamed {original_name} -> {base_name}")
    except Exception as e:
        print(f"  Skip rename {original_name}: {e}")

# Step 2: Create all compatibility views
_COMPAT_VIEW_SQL = {}

# yarn_analysis: base YARN table with original column names (over yarn_applications)
_COMPAT_VIEW_SQL["yarn_analysis"] = f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.yarn_analysis AS
SELECT
    application_id,
    name,
    state,
    `user`,
    queue,
    final_status AS finalstatus,
    application_type AS applicationType,
    started_time AS epochStatedTime,
    finished_time AS epochFinishedTime,
    CAST(elapsed_time_ms AS INT) AS elapsedTimeMSecs,
    memory_seconds AS memorySeconds,
    CAST(vcore_seconds AS INT) AS vCoreSeconds,
    to_timestamp(started_time / 1000) AS jobStartTime,
    to_timestamp(finished_time / 1000) AS jobEndTime,
    CAST(elapsed_time_ms / 1000 AS INT) AS elapsedTimesecs,
    CAST(elapsed_time_ms / 60000 AS INT) AS elapsedTimemins,
    CAST(memory_seconds / 1024 AS INT) AS memoryMB,
    CAST(memory_seconds / 1024 / 1024 AS INT) AS memoryGB,
    COALESCE(allocated_vcores, 0) AS vcores,
    ROW_NUMBER() OVER (ORDER BY application_id) AS row_id
FROM `{catalog}`.`{schema}`.yarn_applications
"""

# yarn_analysis_vw: derived view with job classification + cost estimates
_COMPAT_VIEW_SQL["yarn_analysis_vw"] = f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.yarn_analysis_vw AS
SELECT
    CASE
        WHEN job_type LIKE '%Oozie%' THEN split(name, 'ID=')[0]
        ELSE name
    END AS short_name,
    job_type AS JobType,
    application_id,
    name,
    state,
    `user`,
    queue,
    final_status AS finalstatus,
    application_type AS applicationType,
    started_time AS epochStatedTime,
    finished_time AS epochFinishedTime,
    CAST(elapsed_time_ms AS INT) AS elapsedTimeMSecs,
    memory_seconds AS memorySeconds,
    CAST(vcore_seconds AS INT) AS vCoreSeconds,
    to_timestamp(started_time / 1000) AS jobStartTime,
    to_timestamp(finished_time / 1000) AS jobEndTime,
    CAST(elapsed_time_ms / 1000 AS INT) AS elapsedTimesecs,
    CAST(elapsed_time_ms / 60000 AS INT) AS elapsedTimemins,
    CAST(memory_seconds / 1024 AS INT) AS memoryMB,
    CAST(memory_seconds / 1024 / 1024 AS INT) AS memoryGB,
    COALESCE(allocated_vcores, 0) AS vcores,
    ROW_NUMBER() OVER (ORDER BY application_id) AS row_id,
    memory_gb_hours AS MemoryGBHour,
    memory_gb_hours / 64.0 * 0.9 * 1.1 AS number_vm_instances_hour,
    dollar_dbus,
    dollar_vm,
    total_cost
FROM `{catalog}`.`{schema}`._yarn_analysis_vw
"""

# oozie_analysis_vw: Oozie-filtered view with parsed oozie fields
_COMPAT_VIEW_SQL["oozie_analysis_vw"] = f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.oozie_analysis_vw AS
SELECT
    regexp_extract(name, 'T=(.*):W=.*') AS oozie_type,
    regexp_extract(name, 'W=(.*):A=.*') AS oozie_job,
    regexp_extract(name, 'A=(.*):ID=.*') AS oozie_action,
    *
FROM `{catalog}`.`{schema}`.yarn_analysis_vw
WHERE name LIKE 'oozie:launcher%'
"""

# hourly_yarn_view: hourly YARN aggregation with original column names
_COMPAT_VIEW_SQL["hourly_yarn_view"] = f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.hourly_yarn_view AS
SELECT
    date_trunc('hour', to_timestamp(started_time / 1000)) AS hour,
    AVG(CAST(allocated_mb AS DOUBLE)) AS avg_memory_mb,
    MAX(CAST(allocated_mb AS BIGINT)) AS max_memory_mb,
    MIN(CAST(allocated_mb AS BIGINT)) AS min_memory_mb,
    MAX(CAST(allocated_vcores AS BIGINT)) AS max_cores,
    MIN(CAST(allocated_vcores AS BIGINT)) AS min_cores,
    AVG(CAST(allocated_vcores AS DOUBLE)) AS avg_cores,
    CAST(COUNT(*) AS DOUBLE) AS avg_count
FROM `{catalog}`.`{schema}`.yarn_applications
WHERE started_time IS NOT NULL AND started_time > 0
GROUP BY date_trunc('hour', to_timestamp(started_time / 1000))
"""

# yarn_nodes: alias DuckDB snake_case to original camelCase
_COMPAT_VIEW_SQL["yarn_nodes"] = f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.yarn_nodes AS
SELECT
    node_host_name AS nodeHostName,
    state,
    avail_memory_mb AS availMemoryMB,
    used_memory_mb AS usedMemoryMB,
    available_virtual_cores AS availableVirtualCores,
    used_virtual_cores AS usedVirtualCores,
    version,
    ROW_NUMBER() OVER (ORDER BY node_id) AS row_id
FROM `{catalog}`.`{schema}`._yarn_nodes
"""

# cm_hosts: alias DuckDB snake_case to original camelCase
_COMPAT_VIEW_SQL["cm_hosts"] = f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.cm_hosts AS
SELECT
    host_id AS hostId,
    hostname,
    ip_address AS ipAddress,
    rack_id AS rackId,
    num_cores AS numCores,
    num_physical_cores AS numPhysicalCores,
    total_phys_mem_bytes AS totalPhysMemBytes,
    total_phys_mem_gb,
    commission_state AS commissionState,
    maintenance_mode AS maintenanceMode
FROM `{catalog}`.`{schema}`._cm_hosts
"""

# impala_extract: over impala_queries with original column names
_COMPAT_VIEW_SQL["impala_extract"] = f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.impala_extract AS
SELECT
    query_id AS queryId,
    statement,
    query_type AS queryType,
    query_state AS queryState,
    `user`,
    database_name AS `database`,
    start_time AS startTime,
    end_time AS endTime,
    duration_millis AS durationMillis,
    duration_minutes AS durationMinutes,
    rows_produced AS rowsProduced,
    coordinator,
    CAST(start_time AS TIMESTAMP) AS startDate,
    CAST(NULL AS STRING) AS memory_accrual,
    CAST(NULL AS STRING) AS memory_aggregate_peak,
    CAST(NULL AS STRING) AS memory_per_node_peak,
    CAST(NULL AS STRING) AS thread_cpu_time
FROM `{catalog}`.`{schema}`.impala_queries
"""

# cm_hostroles: over cm_host_roles with original column names
_COMPAT_VIEW_SQL["cm_hostroles"] = f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.cm_hostroles AS
SELECT
    entity_name AS entityName,
    role_name AS roleName,
    role_type AS roleType,
    service_name AS serviceName,
    service_type AS serviceType,
    hostname,
    cluster_name AS clusterName,
    cluster_name AS clusterDisplayName,
    CONCAT(service_name, '-', hostname) AS serviceNameAndNode
FROM `{catalog}`.`{schema}`.cm_host_roles
"""

# cm_roleconfig: over cm_config with original column names
# (UC is case-insensitive, so this also handles cm_roleConfig)
_COMPAT_VIEW_SQL["cm_roleconfig"] = f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.cm_roleconfig AS
SELECT
    config_key AS name,
    config_value AS value,
    service_name AS refName,
    role_type AS roleType,
    service_name AS referenceName,
    CAST(NULL AS STRING) AS cdhVersion,
    CAST(NULL AS STRING) AS cmVersion,
    CAST(NULL AS STRING) AS clusterName,
    CAST(NULL AS STRING) AS variable
FROM `{catalog}`.`{schema}`.cm_config
"""

# cm_clustercpu: EAV -> wide format (also handles cm_ClusterCPU)
_COMPAT_VIEW_SQL["cm_clustercpu"] = f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.cm_clustercpu AS
SELECT
    CAST(metric_timestamp AS STRING) AS timestamp,
    value AS cpuPercent
FROM `{catalog}`.`{schema}`.cm_cpu_utilization
"""

# cm_clustermemory: EAV -> wide format (also handles cm_Clustermemory)
_COMPAT_VIEW_SQL["cm_clustermemory"] = f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.cm_clustermemory AS
SELECT
    CAST(metric_timestamp AS STRING) AS timestamp,
    value AS clusterMemory
FROM `{catalog}`.`{schema}`.cm_memory_utilization
"""

# cm_hdfsstats: EAV -> wide pivot
_COMPAT_VIEW_SQL["cm_hdfsstats"] = f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.cm_hdfsstats AS
SELECT
    MAX(CASE WHEN metric_name LIKE '%capacity%' AND metric_name NOT LIKE '%used%' THEN value END) AS dfs_capacity,
    MAX(CASE WHEN metric_name LIKE '%capacity_used%' THEN value END) AS dfs_capacity_used
FROM `{catalog}`.`{schema}`.cm_hdfs_usage
"""

# cm_ts_impala_utlization: EAV -> wide pivot by timestamp
_COMPAT_VIEW_SQL["cm_ts_impala_utlization"] = f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.cm_ts_impala_utlization AS
SELECT
    CAST(metric_timestamp AS STRING) AS timestamp,
    MAX(CASE WHEN metric_name = 'total_mem_rss_across_impalads_mean' THEN value END) AS total_mem_rss_across_impalads_mean,
    MAX(CASE WHEN metric_name LIKE '%mem_rss_across_impalads_min' THEN value END) AS mem_rss_across_impalads_min,
    MAX(CASE WHEN metric_name LIKE '%mem_rss_across_impalads_max' THEN value END) AS mem_rss_across_impalads_max,
    MAX(CASE WHEN metric_name LIKE '%num_queries_rate%mean' THEN value END) AS num_queries_rate_across_impalads_mean,
    MAX(CASE WHEN metric_name LIKE '%total_mem_tracker%mean' THEN value END) AS total_mem_tracker_process_limit_across_impalads_mean,
    MAX(CASE WHEN metric_name LIKE '%total_num_queries%mean' THEN value END) AS total_num_queries_rate_across_impalads_mean
FROM `{catalog}`.`{schema}`.cm_impala_utilization
GROUP BY metric_timestamp
"""

# cm_ts_yarn_memory_allocation: EAV -> wide pivot by timestamp
_COMPAT_VIEW_SQL["cm_ts_yarn_memory_allocation"] = f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.cm_ts_yarn_memory_allocation AS
SELECT
    CAST(metric_timestamp AS STRING) AS timestamp,
    MAX(CASE WHEN metric_name LIKE '%allocated_memory_mb%mean' THEN value END) AS total_allocated_memory_mb_across_yarn_pools_mean,
    MAX(CASE WHEN metric_name LIKE '%available_memory_mb%min' THEN value END) AS total_available_memory_mb_across_yarn_pools_min,
    MAX(CASE WHEN metric_name LIKE '%available_vcores%mean' THEN value END) AS total_available_vcores_across_yarn_pools_mean,
    MAX(CASE WHEN metric_name LIKE '%allocated_vcores%mean' THEN value END) AS total_allocated_vcores_across_yarn_pools_mean
FROM `{catalog}`.`{schema}`.cm_yarn_memory_cpu
GROUP BY metric_timestamp
"""

created = 0
view_failed = []
for view_name, sql in _COMPAT_VIEW_SQL.items():
    try:
        spark.sql(sql)
        created += 1
        print(f"  OK  {view_name}")
    except Exception as e:
        view_failed.append(view_name)
        print(f"  FAIL {view_name}: {e}")

print(f"\nCompatibility views: {created}/{len(_COMPAT_VIEW_SQL)} created")
if view_failed:
    print(f"Failed: {view_failed}")

# COMMAND ----------

# Validate row counts: Delta vs DuckDB
validation = []
for table_name, duckdb_count in source_counts.items():
    try:
        full_name = f"{catalog}.{schema}.{table_name}"
        delta_count = spark.table(full_name).count()
        match = delta_count == duckdb_count
        validation.append({
            "table": table_name,
            "duckdb_rows": duckdb_count,
            "delta_rows": delta_count,
            "status": "PASS" if match else "MISMATCH"
        })
    except Exception as e:
        validation.append({
            "table": table_name,
            "duckdb_rows": duckdb_count,
            "delta_rows": -1,
            "status": f"ERROR: {e}"
        })

validation_df = spark.createDataFrame(pd.DataFrame(validation))
display(validation_df)

mismatches = [v for v in validation if v["status"] != "PASS"]
if mismatches:
    print(f"\nWARNING: {len(mismatches)} table(s) with issues:")
    for m in mismatches:
        print(f"  {m['table']}: {m['status']} (duckdb={m['duckdb_rows']}, delta={m['delta_rows']})")
else:
    print(f"\nAll {len(validation)} tables validated - PASS")

# COMMAND ----------

# Summary report
print("=" * 60)
print("LOAD SUMMARY")
print("=" * 60)
print(f"Tables loaded:  {len(loaded)}/{len(source_counts)}")
print(f"Total rows:     {sum(r['rows'] for r in loaded)}")
print(f"Failures:       {len(failed)}")
print()

# Key metrics from yarn_analysis_vw (use _yarn_analysis_vw base table for DuckDB column names)
try:
    job_types = spark.sql(f"""
        SELECT job_type, COUNT(*) AS count,
               ROUND(SUM(total_cost), 2) AS total_cost,
               ROUND(SUM(memory_gb_hours), 1) AS memory_gb_hours
        FROM `{catalog}`.`{schema}`._yarn_analysis_vw
        GROUP BY job_type ORDER BY count DESC
    """)
    print("YARN Job Type Breakdown:")
    display(job_types)
except Exception as e:
    print(f"(yarn_analysis_vw not available: {e})")

# Export metadata
try:
    meta = spark.sql(f"""
        SELECT export_timestamp, tables_created, total_rows,
               dbu_rate, vm_rate, duckdb_version
        FROM `{catalog}`.`{schema}`.export_metadata
    """)
    print("\nExport Metadata:")
    display(meta)
except Exception as e:
    print(f"(export_metadata not available: {e})")

# COMMAND ----------

# Cleanup
duckdb_conn.close()
print("DuckDB connection closed.")
print(f"Delta tables available at: {catalog}.{schema}.*")
