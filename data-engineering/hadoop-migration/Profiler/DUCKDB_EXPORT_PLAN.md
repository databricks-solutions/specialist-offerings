# Design & Implementation Plan: DuckDB Export for Hadoop Profiler

## Context

The existing Profiler (`profiler.sh`) writes raw JSON files from REST API responses (YARN RM, Spark HS, Cloudera Manager, Impala). Currently, a separate Databricks notebook pipeline (7 stages, from `Profiler Extract V2_17.zip`) reads these JSON files and transforms them into ~35 Delta tables for analysis.

**Problem:** Shipping raw JSON to Databricks requires running the full notebook pipeline. This is fragile, hard to version, and requires Databricks compute just for ETL.

**Solution:** Add a Python post-processor (`duckdb_exporter`) that converts profiler JSON output into a single portable DuckDB file with structured tables matching the Delta table schemas. This DuckDB file can be directly uploaded to Databricks and trivially converted to Delta tables.

**Approach:** Keep `profiler.sh` unchanged. Add a new `duckdb_exporter/` Python module in the Profiler directory that reads JSON output and writes a DuckDB database.

---

## Directory Structure

```
src/hadoop/Profiler/
├── profiler.sh                      # UNCHANGED
├── profiler.conf                    # UNCHANGED
├── requirements.txt                 # NEW: duckdb, pyyaml
├── duckdb_exporter.conf.yaml        # NEW: config file
├── DUCKDB_EXPORT_PLAN.md            # THIS FILE
├── duckdb_exporter/
│   ├── __init__.py
│   ├── __main__.py                  # CLI: python -m duckdb_exporter export ...
│   ├── config.py                    # YAML config loader (mirrors Analyzer pattern)
│   ├── exporter.py                  # Main orchestrator
│   ├── schema.py                    # All CREATE TABLE DDL statements
│   ├── utils.py                     # Timestamp parsing, path helpers
│   ├── loaders/
│   │   ├── __init__.py
│   │   ├── yarn_loader.py           # 4 YARN JSON types → 4 base tables
│   │   ├── spark_loader.py          # Spark_Applications → 1 table
│   │   ├── impala_loader.py         # impala_*.json → 1 table
│   │   └── cm_loader.py             # 11 CM JSON types → 11 tables
│   └── transforms/
│       ├── __init__.py
│       ├── yarn_analysis.py         # yarn_analysis_vw, oozie_analysis_vw, hourly_yarn_view
│       └── summary_tables.py        # workload_summary_by_user/queue/type
└── tests/
    ├── __init__.py
    ├── test_yarn_loader.py
    ├── test_spark_loader.py
    ├── test_impala_loader.py
    ├── test_cm_loader.py
    ├── test_transforms.py
    └── test_exporter.py
```

**~25 new files** (11 Python modules + 6 test files + 6 `__init__.py` + config + requirements)

---

## Profiler JSON → DuckDB Table Mapping

### Source JSON Files (17 types from real CDH 5.x output)

| Source Directory | JSON File Pattern | DuckDB Table | JSON Root Path |
|---|---|---|---|
| `YARN/` | `YarnApplicationDump_*.json` | `yarn_applications` | `apps.app[]` |
| `YARN/` | `YarnMetricsDump_*.json` | `yarn_cluster_metrics` | `clusterMetrics` |
| `YARN/` | `YarnNodesDump_*.json` | `yarn_nodes` | `nodes.node[]` |
| `YARN/` | `YarnSchedulerDump_*.json` | `yarn_scheduler_queues` | `scheduler.schedulerInfo.rootQueue.childQueues[]` |
| `SPARK/` | `Spark_Applications_*.json` | `spark_applications` | `[]` (top-level array) |
| `IMPALA/` | `impala_*_*.json` | `impala_queries` | `queries[]` |
| `CM/` | `cmHosts_*.json` | `cm_hosts` | `items[]` |
| `CM/` | `cmServices_*.json` | `cm_services` | `items[]` |
| `CM/` | `cmConfig_*.json` | `cm_config` | deployment export (flattened) |
| `CM/` | `cmExport_*.json` | `cm_export` | raw JSON blob |
| `CM/` | `cmHostRoles_*.json` | `cm_host_roles` | `items[].timeSeries[].metadata.attributes` |
| `CM/` | `cmHDFSUsage_*.json` | `cm_hdfs_usage` | `items[].timeSeries[].data[]` |
| `CM/` | `cmClusterCPUUtilization_*.json` | `cm_cpu_utilization` | `items[].timeSeries[].data[]` |
| `CM/` | `cmClusterMemoryUtilization_*.json` | `cm_memory_utilization` | `items[].timeSeries[].data[]` |
| `CM/` | `cmYarnMemoryAndCPU_*.json` | `cm_yarn_memory_cpu` | `items[].timeSeries[].data[]` |
| `CM/` | `cmYarnUtilization_*.json` | `cm_yarn_utilization` | `items[].timeSeries[].data[]` |
| `CM/` | `cmImpalaUtilization_*.json` | `cm_impala_utilization` | `items[].timeSeries[].data[]` |

### Derived Tables (7)

| Table | Source | Purpose |
|---|---|---|
| `yarn_analysis_vw` | `yarn_applications` | Job type classification + cost estimates |
| `oozie_analysis_vw` | `yarn_analysis_vw` | Filtered to Oozie launcher apps |
| `hourly_yarn_view` | `yarn_analysis_vw` | Hourly aggregation |
| `workload_summary_by_user` | `yarn_analysis_vw` | Per-user cost/usage summary |
| `workload_summary_by_queue` | `yarn_analysis_vw` | Per-queue cost/usage summary |
| `workload_summary_by_type` | `yarn_analysis_vw` | Per-job-type summary |
| `export_metadata` | (generated) | Export audit trail |

---

## Table Schemas (Complete DDL)

### `yarn_applications`

```sql
CREATE TABLE yarn_applications (
    application_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    user VARCHAR,
    queue VARCHAR,
    state VARCHAR,
    final_status VARCHAR,
    application_type VARCHAR,        -- MAPREDUCE | SPARK
    started_time BIGINT,             -- epoch ms
    finished_time BIGINT,
    elapsed_time_ms BIGINT,
    memory_seconds BIGINT,
    vcore_seconds BIGINT,
    allocated_mb INTEGER,
    allocated_vcores INTEGER,
    running_containers INTEGER,
    diagnostics VARCHAR,
    tracking_url VARCHAR,
    log_aggregation_status VARCHAR,
    application_tags VARCHAR,
    extraction_timestamp TIMESTAMP
);
```

### `yarn_cluster_metrics`

```sql
CREATE TABLE yarn_cluster_metrics (
    apps_submitted INTEGER,
    apps_completed INTEGER,
    apps_pending INTEGER,
    apps_running INTEGER,
    apps_failed INTEGER,
    apps_killed INTEGER,
    reserved_mb BIGINT,
    available_mb BIGINT,
    allocated_mb BIGINT,
    total_mb BIGINT,
    reserved_vcores INTEGER,
    available_vcores INTEGER,
    allocated_vcores INTEGER,
    total_vcores INTEGER,
    containers_allocated INTEGER,
    containers_reserved INTEGER,
    containers_pending INTEGER,
    total_nodes INTEGER,
    active_nodes INTEGER,
    lost_nodes INTEGER,
    unhealthy_nodes INTEGER,
    decommissioned_nodes INTEGER,
    rebooted_nodes INTEGER,
    extraction_timestamp TIMESTAMP
);
```

### `yarn_nodes`

```sql
CREATE TABLE yarn_nodes (
    node_id VARCHAR PRIMARY KEY,
    node_host_name VARCHAR,
    rack VARCHAR,
    state VARCHAR,
    node_http_address VARCHAR,
    version VARCHAR,
    last_health_update BIGINT,
    health_report VARCHAR,
    num_containers INTEGER,
    used_memory_mb BIGINT,
    avail_memory_mb BIGINT,
    used_virtual_cores INTEGER,
    available_virtual_cores INTEGER,
    extraction_timestamp TIMESTAMP
);
```

### `yarn_scheduler_queues`

```sql
CREATE TABLE yarn_scheduler_queues (
    queue_name VARCHAR PRIMARY KEY,
    scheduler_type VARCHAR,          -- fairScheduler | capacityScheduler
    scheduling_policy VARCHAR,
    max_resources_memory INTEGER,
    max_resources_vcores INTEGER,
    min_resources_memory INTEGER,
    min_resources_vcores INTEGER,
    used_resources_memory INTEGER,
    used_resources_vcores INTEGER,
    steady_fair_memory INTEGER,
    steady_fair_vcores INTEGER,
    num_pending_apps INTEGER,
    num_active_apps INTEGER,
    preemptable BOOLEAN,
    extraction_timestamp TIMESTAMP
);
```

### `spark_applications`

```sql
CREATE TABLE spark_applications (
    application_id VARCHAR,
    attempt_id VARCHAR,
    name VARCHAR,
    spark_user VARCHAR,
    start_time VARCHAR,              -- ISO timestamp string
    end_time VARCHAR,
    duration_ms BIGINT,              -- computed: end - start
    completed BOOLEAN,
    extraction_timestamp TIMESTAMP,
    PRIMARY KEY (application_id, attempt_id)
);
```

### `impala_queries`

```sql
CREATE TABLE impala_queries (
    query_id VARCHAR PRIMARY KEY,
    statement TEXT,
    query_type VARCHAR,
    query_state VARCHAR,
    user VARCHAR,
    database_name VARCHAR,
    start_time VARCHAR,
    end_time VARCHAR,
    duration_millis BIGINT,
    duration_minutes DOUBLE,         -- computed: duration_millis / 60000
    rows_produced BIGINT,
    coordinator VARCHAR,
    extraction_timestamp TIMESTAMP
);
```

### `cm_hosts`

```sql
CREATE TABLE cm_hosts (
    host_id VARCHAR PRIMARY KEY,
    hostname VARCHAR,
    ip_address VARCHAR,
    rack_id VARCHAR,
    num_cores INTEGER,
    num_physical_cores INTEGER,
    total_phys_mem_bytes BIGINT,
    total_phys_mem_gb DOUBLE,        -- computed: bytes / 1073741824
    commission_state VARCHAR,
    maintenance_mode BOOLEAN,
    extraction_timestamp TIMESTAMP
);
```

### `cm_services`

```sql
CREATE TABLE cm_services (
    service_name VARCHAR PRIMARY KEY,
    service_type VARCHAR,
    display_name VARCHAR,
    cluster_name VARCHAR,
    service_state VARCHAR,
    health_summary VARCHAR,
    config_staleness_status VARCHAR,
    maintenance_mode BOOLEAN,
    extraction_timestamp TIMESTAMP
);
```

### `cm_config`

```sql
CREATE TABLE cm_config (
    config_key VARCHAR,
    config_value VARCHAR,
    service_name VARCHAR,
    role_type VARCHAR,
    extraction_timestamp TIMESTAMP
);
```

### `cm_export`

```sql
CREATE TABLE cm_export (
    export_json TEXT,
    extraction_timestamp TIMESTAMP
);
```

### `cm_host_roles`

```sql
CREATE TABLE cm_host_roles (
    entity_name VARCHAR,
    role_name VARCHAR,
    role_type VARCHAR,
    service_name VARCHAR,
    service_type VARCHAR,
    hostname VARCHAR,
    cluster_name VARCHAR,
    extraction_timestamp TIMESTAMP
);
```

### CM Timeseries Tables (6 tables, shared schema)

All 6 CM timeseries tables (`cm_hdfs_usage`, `cm_cpu_utilization`, `cm_memory_utilization`, `cm_yarn_memory_cpu`, `cm_yarn_utilization`, `cm_impala_utilization`) share:

```sql
CREATE TABLE cm_<name> (
    metric_name VARCHAR,
    entity_name VARCHAR,
    metric_timestamp TIMESTAMP,
    value DOUBLE,
    unit VARCHAR,
    cluster_name VARCHAR,
    extraction_timestamp TIMESTAMP
);
```

The CM timeseries JSON structure is:
```json
{"items": [{"timeSeries": [{"metadata": {"metricName": "...", "entityName": "...", ...},
                             "data": [{"timestamp": "ISO", "value": 123.0, "type": "SAMPLE"}]}]}]}
```

### `yarn_analysis_vw` (Derived)

```sql
CREATE TABLE yarn_analysis_vw AS
SELECT
    *,
    CAST(memory_seconds AS DOUBLE) / 3600 / 1024 AS memory_gb_hours,
    CAST(vcore_seconds AS DOUBLE) / 3600 AS vcore_hours,
    CAST(elapsed_time_ms AS DOUBLE) / 60000 AS elapsed_time_mins,
    CASE
        WHEN name LIKE 'oozie:launcher:T=spark%' THEN 'Spark (Oozie)'
        WHEN name LIKE 'oozie:launcher:T=hive%' THEN 'Hive (Oozie)'
        WHEN name LIKE 'oozie:launcher:T=sqoop%' THEN 'Sqoop (Oozie)'
        WHEN name LIKE 'oozie:launcher%' THEN 'Oozie Launcher'
        WHEN application_type = 'SPARK' THEN 'Spark'
        WHEN name SIMILAR TO '(SELECT|INSERT|CREATE|DROP|ALTER|LOAD).*' THEN 'Hive'
        WHEN LOWER(name) LIKE '%sqoop%' THEN 'Sqoop'
        WHEN application_type = 'MAPREDUCE' THEN 'MapReduce'
        ELSE 'Other'
    END AS job_type,
    (CAST(memory_seconds AS DOUBLE) / 3600 / 1024) * :dbu_rate AS dollar_dbus,
    (CAST(memory_seconds AS DOUBLE) / 3600 / 1024) * :vm_rate AS dollar_vm,
    ((CAST(memory_seconds AS DOUBLE) / 3600 / 1024) * :dbu_rate) +
    ((CAST(memory_seconds AS DOUBLE) / 3600 / 1024) * :vm_rate) AS total_cost
FROM yarn_applications;
```

### `oozie_analysis_vw` (Derived)

```sql
CREATE TABLE oozie_analysis_vw AS
SELECT * FROM yarn_analysis_vw
WHERE name LIKE 'oozie:launcher%';
```

### `hourly_yarn_view` (Derived)

```sql
CREATE TABLE hourly_yarn_view AS
SELECT
    strftime(to_timestamp(started_time / 1000), '%Y-%m-%d %H:00:00') AS hour_bucket,
    COUNT(*) AS total_apps,
    SUM(memory_gb_hours) AS total_memory_gb_hours,
    SUM(vcore_hours) AS total_vcore_hours,
    SUM(total_cost) AS total_cost,
    COUNT(DISTINCT user) AS unique_users,
    COUNT(DISTINCT queue) AS unique_queues
FROM yarn_analysis_vw
WHERE started_time IS NOT NULL
GROUP BY hour_bucket
ORDER BY hour_bucket;
```

### Summary Tables (Derived)

```sql
CREATE TABLE workload_summary_by_user AS
SELECT user, COUNT(*) AS total_jobs, SUM(total_cost) AS total_cost,
       SUM(memory_gb_hours) AS total_memory_gb_hours,
       AVG(elapsed_time_mins) AS avg_duration_mins
FROM yarn_analysis_vw GROUP BY user ORDER BY total_cost DESC;

CREATE TABLE workload_summary_by_queue AS
SELECT queue, COUNT(*) AS total_jobs, COUNT(DISTINCT user) AS unique_users,
       SUM(total_cost) AS total_cost, SUM(memory_gb_hours) AS total_memory_gb_hours
FROM yarn_analysis_vw GROUP BY queue ORDER BY total_cost DESC;

CREATE TABLE workload_summary_by_type AS
SELECT job_type, COUNT(*) AS total_jobs, AVG(elapsed_time_mins) AS avg_duration_mins,
       SUM(total_cost) AS total_cost, SUM(memory_gb_hours) AS total_memory_gb_hours
FROM yarn_analysis_vw GROUP BY job_type ORDER BY total_jobs DESC;
```

### `export_metadata`

```sql
CREATE TABLE export_metadata (
    export_timestamp TIMESTAMP,
    profiler_output_dir VARCHAR,
    tables_created INTEGER,
    total_rows INTEGER,
    dbu_rate DOUBLE,
    vm_rate DOUBLE,
    duckdb_version VARCHAR
);
```

---

## CLI Design

```bash
# Main command: export profiler JSON → DuckDB
python -m duckdb_exporter export \
    --profiler-output ~/cloudera-profiler-output/Output \
    --output ~/cloudera-profiler-output/hadoop_profiler.duckdb

# With config file
python -m duckdb_exporter export --config duckdb_exporter.conf.yaml

# Custom cost rates
python -m duckdb_exporter export \
    --profiler-output ./Output --output ./hadoop_profiler.duckdb \
    --dbu-rate 0.20 --vm-rate 0.12

# Export only specific sources (skip CM/Impala if not present)
python -m duckdb_exporter export \
    --profiler-output ./Output --output ./hadoop_profiler.duckdb \
    --sources yarn,spark

# Validate profiler output (dry run — reports what files found)
python -m duckdb_exporter validate --profiler-output ./Output

# Query existing DuckDB file
python -m duckdb_exporter query \
    --db ./hadoop_profiler.duckdb \
    --sql "SELECT job_type, COUNT(*) FROM yarn_analysis_vw GROUP BY job_type"

# Verbose mode
python -m duckdb_exporter -v export --profiler-output ./Output --output ./out.duckdb
```

---

## Config File (`duckdb_exporter.conf.yaml`)

```yaml
profiler_output:
  base_dir: "/path/to/profiler-output/Output"

output:
  db_path: "./hadoop_profiler.duckdb"
  overwrite: true              # overwrite existing DB or fail

cost_rates:
  dbu_rate: 0.15               # $/GB-hour for DBU pricing
  vm_rate: 0.10                # $/GB-hour for VM pricing

sources:                       # toggle which data sources to export
  yarn: true
  spark: true
  impala: true
  cm: true
```

---

## Key Design Decisions

1. **Keep profiler.sh unchanged** — DuckDB export is a separate post-processing step
2. **Single DuckDB file** — all 24 tables in one portable file (~10-50MB typical)
3. **Match Delta table schemas** — column names/types match the reference Databricks pipeline
4. **Include derived tables** — job type classification, cost estimates, hourly/user/queue summaries computed during export
5. **Graceful degradation** — missing JSON files (e.g., no CM on HDP) are skipped with warnings
6. **Cost rates are configurable** — DBU/VM rates passed via config or CLI args
7. **JSON files preserved** — DuckDB is additive, original JSON files are untouched

---

## Implementation Order

### Step 1: Project scaffold + config
- Directory structure + `__init__.py` files
- `requirements.txt` (`duckdb>=0.10.0`, `pyyaml>=6.0`)
- `config.py` — YAML config loader
- `duckdb_exporter.conf.yaml` — default template
- `schema.py` — all CREATE TABLE DDL as constants
- `utils.py` — timestamp parsing, file discovery (glob patterns)
- `__main__.py` — CLI skeleton

### Step 2: YARN loader + transforms
- `loaders/yarn_loader.py` — 4 functions for 4 YARN JSON types
- `transforms/yarn_analysis.py` — yarn_analysis_vw, oozie_analysis_vw, hourly_yarn_view

### Step 3: Spark + Impala loaders
- `loaders/spark_loader.py` — flatten attempts array
- `loaders/impala_loader.py` — handle paginated files

### Step 4: CM loader
- `loaders/cm_loader.py` — 11 CM JSON types (hosts, services, config, export, host_roles, 6 timeseries)

### Step 5: Orchestrator + summary tables
- `exporter.py` — coordinate loaders + transforms + export_metadata
- `transforms/summary_tables.py` — by_user, by_queue, by_type tables

### Step 6: Tests
- Unit tests for each loader + transforms
- End-to-end test against `~/cloudera-profiler-output/Output/` (17 JSON files → 24 tables)

---

## Reference Files (Reuse Patterns)

- `../Analyzer/analyzer/config.py` — YAML config loader pattern with dataclasses
- `../Analyzer/analyzer/cli.py` — argparse + logging setup pattern
- `../Analyzer/analyzer/parsers/yarn_parser.py` — Job type classification regex (lines 15-58)

---

## Verification Checklist

1. Run `python -m duckdb_exporter export --profiler-output ~/cloudera-profiler-output/Output --output /tmp/test.duckdb`
2. Verify 24 tables created: `SHOW TABLES` should return 24 rows
3. Verify YARN apps: `SELECT COUNT(*) FROM yarn_applications` → expected 23
4. Verify Spark apps: `SELECT COUNT(*) FROM spark_applications` → expected 4 (with attempts)
5. Verify Impala queries: `SELECT COUNT(*) FROM impala_queries` → expected 12
6. Verify job type classification in `yarn_analysis_vw` → Hive ~12, Spark ~9, MapReduce ~2
7. Verify cost columns are computed: `SELECT SUM(total_cost) FROM yarn_analysis_vw` → non-null
8. Verify CM hosts: `SELECT COUNT(*) FROM cm_hosts` → expected 1
9. Verify CM services: `SELECT COUNT(*) FROM cm_services` → expected 10+
10. Run `python -m pytest tests/ -v` → all tests pass

---

## Databricks Integration (Post-Export)

Once the DuckDB file is generated, loading into Databricks is straightforward:

```python
# In Databricks notebook
import duckdb

# Upload the .duckdb file to DBFS or Volumes first
conn = duckdb.connect("/dbfs/path/to/hadoop_profiler.duckdb", read_only=True)

# Convert each table to Delta
for table in conn.execute("SHOW TABLES").fetchall():
    table_name = table[0]
    df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
    spark.createDataFrame(df).write.format("delta").mode("overwrite").saveAsTable(f"profiler_db.{table_name}")

conn.close()
```

This replaces the entire 7-stage Databricks notebook pipeline with a single loop.
