"""DuckDB table schema definitions for profiler data."""

# ── YARN Tables ──────────────────────────────────────────────────────────────

YARN_APPLICATIONS = """
CREATE TABLE IF NOT EXISTS yarn_applications (
    application_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    "user" VARCHAR,
    queue VARCHAR,
    state VARCHAR,
    final_status VARCHAR,
    application_type VARCHAR,
    started_time BIGINT,
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
"""

YARN_CLUSTER_METRICS = """
CREATE TABLE IF NOT EXISTS yarn_cluster_metrics (
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
"""

YARN_NODES = """
CREATE TABLE IF NOT EXISTS yarn_nodes (
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
"""

YARN_SCHEDULER_QUEUES = """
CREATE TABLE IF NOT EXISTS yarn_scheduler_queues (
    queue_name VARCHAR PRIMARY KEY,
    scheduler_type VARCHAR,
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
"""

# ── Spark Tables ─────────────────────────────────────────────────────────────

SPARK_APPLICATIONS = """
CREATE TABLE IF NOT EXISTS spark_applications (
    application_id VARCHAR,
    attempt_id VARCHAR,
    name VARCHAR,
    spark_user VARCHAR,
    start_time VARCHAR,
    end_time VARCHAR,
    duration_ms BIGINT,
    completed BOOLEAN,
    extraction_timestamp TIMESTAMP,
    PRIMARY KEY (application_id, attempt_id)
);
"""

# ── Impala Tables ────────────────────────────────────────────────────────────

IMPALA_QUERIES = """
CREATE TABLE IF NOT EXISTS impala_queries (
    query_id VARCHAR PRIMARY KEY,
    statement TEXT,
    query_type VARCHAR,
    query_state VARCHAR,
    "user" VARCHAR,
    database_name VARCHAR,
    start_time VARCHAR,
    end_time VARCHAR,
    duration_millis BIGINT,
    duration_minutes DOUBLE,
    rows_produced BIGINT,
    coordinator VARCHAR,
    extraction_timestamp TIMESTAMP
);
"""

# ── CM Tables ────────────────────────────────────────────────────────────────

CM_HOSTS = """
CREATE TABLE IF NOT EXISTS cm_hosts (
    host_id VARCHAR PRIMARY KEY,
    hostname VARCHAR,
    ip_address VARCHAR,
    rack_id VARCHAR,
    num_cores INTEGER,
    num_physical_cores INTEGER,
    total_phys_mem_bytes BIGINT,
    total_phys_mem_gb DOUBLE,
    commission_state VARCHAR,
    maintenance_mode BOOLEAN,
    extraction_timestamp TIMESTAMP
);
"""

CM_SERVICES = """
CREATE TABLE IF NOT EXISTS cm_services (
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
"""

CM_CONFIG = """
CREATE TABLE IF NOT EXISTS cm_config (
    config_key VARCHAR,
    config_value VARCHAR,
    service_name VARCHAR,
    role_type VARCHAR,
    extraction_timestamp TIMESTAMP
);
"""

CM_EXPORT = """
CREATE TABLE IF NOT EXISTS cm_export (
    export_json TEXT,
    extraction_timestamp TIMESTAMP
);
"""

CM_HOST_ROLES = """
CREATE TABLE IF NOT EXISTS cm_host_roles (
    entity_name VARCHAR,
    role_name VARCHAR,
    role_type VARCHAR,
    service_name VARCHAR,
    service_type VARCHAR,
    hostname VARCHAR,
    cluster_name VARCHAR,
    extraction_timestamp TIMESTAMP
);
"""

# Generic timeseries schema used by all 6 CM timeseries tables
CM_TIMESERIES_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {table_name} (
    metric_name VARCHAR,
    entity_name VARCHAR,
    metric_timestamp TIMESTAMP,
    value DOUBLE,
    unit VARCHAR,
    cluster_name VARCHAR,
    extraction_timestamp TIMESTAMP
);
"""

CM_TIMESERIES_TABLES = [
    "cm_hdfs_usage",
    "cm_cpu_utilization",
    "cm_memory_utilization",
    "cm_yarn_memory_cpu",
    "cm_yarn_utilization",
    "cm_impala_utilization",
]

# ── Metadata Table ───────────────────────────────────────────────────────────

EXPORT_METADATA = """
CREATE TABLE IF NOT EXISTS export_metadata (
    export_timestamp TIMESTAMP,
    profiler_output_dir VARCHAR,
    tables_created INTEGER,
    total_rows INTEGER,
    dbu_rate DOUBLE,
    vm_rate DOUBLE,
    duckdb_version VARCHAR
);
"""

# ── All base table DDLs ─────────────────────────────────────────────────────

BASE_TABLES = [
    YARN_APPLICATIONS,
    YARN_CLUSTER_METRICS,
    YARN_NODES,
    YARN_SCHEDULER_QUEUES,
    SPARK_APPLICATIONS,
    IMPALA_QUERIES,
    CM_HOSTS,
    CM_SERVICES,
    CM_CONFIG,
    CM_EXPORT,
    CM_HOST_ROLES,
    EXPORT_METADATA,
]


def create_all_base_tables(conn):
    """Create all base tables in the DuckDB connection."""
    for ddl in BASE_TABLES:
        conn.execute(ddl)
    for table_name in CM_TIMESERIES_TABLES:
        conn.execute(CM_TIMESERIES_TEMPLATE.format(table_name=table_name))
