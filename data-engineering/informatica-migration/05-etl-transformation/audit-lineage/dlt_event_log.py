"""
DLT Event Log: Pipeline Monitoring and Quality Metrics
======================================================

WHAT THIS DOES:
- Provides detailed execution logs for DLT pipelines
- Tracks data quality metrics (expectations pass/fail rates)
- Records pipeline performance and throughput

REPLACES IN INFORMATICA:
- Session logs and statistics
- Performance Monitor
- Error logs and rejection counts

EVENT LOG CONTENTS:
- Pipeline start/stop events
- Table creation and update events
- Data quality metrics (expectations)
- Error messages and stack traces
- Performance statistics (rows, bytes, duration)

ACCESS METHODS:
1. DLT UI: Pipeline → Event Log tab
2. Event Log Delta Table: /pipelines/{pipeline_id}/system/events
3. Programmatic: Read as Delta table

COMMON USE CASES:
- Monitor pipeline health
- Track data quality trends
- Debug failed runs
- Capacity planning (rows processed, duration)
- SLA compliance reporting
"""

from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, ArrayType, StringType

# =============================================================================
# Read DLT Event Log
# =============================================================================
# Replace {pipeline_id} with your actual pipeline ID
# Find it in DLT UI → Pipeline Settings → Pipeline ID

pipeline_id = "your-pipeline-id-here"
event_log_path = f"/pipelines/{pipeline_id}/system/events"

event_log = spark.read.format("delta").load(event_log_path)

# =============================================================================
# Query 1: Pipeline Execution Summary
# =============================================================================
pipeline_runs = (
    event_log
    .where("event_type = 'update_progress'")
    .selectExpr(
        "id as run_id",
        "timestamp",
        "details:update_progress:state as state",
        "details:update_progress:update_id as update_id"
    )
    .orderBy(col("timestamp").desc())
)

print("Recent Pipeline Runs:")
pipeline_runs.show(10, truncate=False)


# =============================================================================
# Query 2: Data Quality Metrics (Expectations)
# =============================================================================
quality_metrics = (
    event_log
    .where("event_type = 'flow_progress'")
    .where("details:flow_progress:data_quality IS NOT NULL")
    .selectExpr(
        "timestamp",
        "details:flow_progress:flow_name as table_name",
        "details:flow_progress:metrics.num_output_rows as rows_processed",
        "details:flow_progress:data_quality.dropped_records as dropped_records",
        "details:flow_progress:data_quality.expectations as expectations"
    )
    .orderBy(col("timestamp").desc())
)

print("\nData Quality Metrics:")
quality_metrics.show(20, truncate=False)


# =============================================================================
# Query 3: Detailed Expectation Results
# =============================================================================
# Parse expectations array to get individual expectation results
expectation_details = (
    event_log
    .where("event_type = 'flow_progress'")
    .where("details:flow_progress:data_quality.expectations IS NOT NULL")
    .selectExpr(
        "timestamp",
        "details:flow_progress:flow_name as table_name",
        "explode(from_json(details:flow_progress:data_quality.expectations, "
        "'array<struct<name:string,dataset:string,passed_records:long,failed_records:long>>')) as exp"
    )
    .selectExpr(
        "timestamp",
        "table_name",
        "exp.name as expectation_name",
        "exp.passed_records",
        "exp.failed_records",
        "ROUND(exp.passed_records * 100.0 / (exp.passed_records + exp.failed_records), 2) as pass_rate"
    )
)

print("\nExpectation Pass Rates:")
expectation_details.show(50, truncate=False)


# =============================================================================
# Query 4: Pipeline Errors and Failures
# =============================================================================
errors = (
    event_log
    .where("event_type IN ('flow_progress', 'update_progress')")
    .where("details:flow_progress:status = 'FAILED' OR "
           "details:update_progress:state = 'FAILED'")
    .selectExpr(
        "timestamp",
        "event_type",
        "details:flow_progress:flow_name as table_name",
        "error.message as error_message",
        "error.stack_trace as stack_trace"
    )
    .orderBy(col("timestamp").desc())
)

print("\nRecent Errors:")
errors.show(10, truncate=False)


# =============================================================================
# Query 5: Throughput Metrics (Rows per Second)
# =============================================================================
throughput = (
    event_log
    .where("event_type = 'flow_progress'")
    .where("details:flow_progress:metrics IS NOT NULL")
    .selectExpr(
        "timestamp",
        "details:flow_progress:flow_name as table_name",
        "details:flow_progress:metrics.num_output_rows as rows",
        "details:flow_progress:metrics.time_taken_ms as duration_ms"
    )
    .withColumn("rows_per_second",
                col("rows") / (col("duration_ms") / 1000.0))
    .orderBy(col("timestamp").desc())
)

print("\nThroughput Metrics:")
throughput.show(20, truncate=False)


# =============================================================================
# Save Quality Metrics to Gold Table for Dashboards
# =============================================================================
# quality_metrics.write.mode("append").saveAsTable("main.audit.dlt_quality_metrics")
