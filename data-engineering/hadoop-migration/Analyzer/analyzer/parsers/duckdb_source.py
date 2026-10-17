"""Read profiler data from a DuckDB file instead of raw JSON."""

import logging
from typing import Any, Dict, List, Tuple

import duckdb

from analyzer.models import CodeArtifact, WorkloadInventoryItem, WorkloadType
from analyzer.parsers.yarn_parser import (
    _classify_app_type,
    _extract_oozie_info,
    _infer_code_artifacts,
    _infer_entry_point,
    HIVE_QUERY_PATTERN,
)

logger = logging.getLogger(__name__)


def load_from_duckdb(
    db_path: str,
) -> Tuple[List[WorkloadInventoryItem], List[WorkloadInventoryItem], List[WorkloadInventoryItem], Dict[str, Any]]:
    """Load yarn, spark, impala items from a DuckDB profiler database.

    Returns (yarn_items, spark_items, impala_items, summaries).
    """
    conn = duckdb.connect(db_path, read_only=True)
    try:
        yarn_items = _load_yarn(conn)
        spark_items = _load_spark(conn)
        impala_items = _load_impala(conn)
        summaries = _load_summaries(conn)
    finally:
        conn.close()

    logger.info(
        "Loaded from DuckDB: %d YARN, %d Spark, %d Impala",
        len(yarn_items), len(spark_items), len(impala_items),
    )
    return yarn_items, spark_items, impala_items, summaries


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Check if a table exists in the database."""
    result = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return result[0] > 0


def _load_yarn(conn: duckdb.DuckDBPyConnection) -> List[WorkloadInventoryItem]:
    """Load YARN applications from DuckDB.

    Prefers yarn_analysis_vw (has cost/normalized fields) with fallback
    to yarn_applications for older DuckDB files.
    """
    use_enriched = _table_exists(conn, "yarn_analysis_vw")

    if use_enriched:
        source_table = "yarn_analysis_vw"
    elif _table_exists(conn, "yarn_applications"):
        source_table = "yarn_applications"
    else:
        logger.warning("No YARN table found in DuckDB")
        return []

    base_columns = [
        "application_id", "name", "user", "queue", "final_status",
        "application_type", "started_time", "finished_time",
        "elapsed_time_ms", "memory_seconds", "vcore_seconds", "diagnostics",
    ]
    enriched_columns = [
        "job_type", "memory_gb_hours", "vcore_hours",
        "elapsed_time_mins", "dollar_dbus", "dollar_vm", "total_cost",
    ]

    select_cols = ', '.join(
        f'"{c}"' if c == "user" else c for c in base_columns
    )
    if use_enriched:
        select_cols += ", " + ", ".join(enriched_columns)

    rows = conn.execute(f"SELECT {select_cols} FROM {source_table}").fetchall()

    columns = base_columns + (enriched_columns if use_enriched else [])

    items = []
    for row in rows:
        r = dict(zip(columns, row))

        # Build a dict matching the JSON structure that yarn_parser helpers expect
        app = {
            "id": r["application_id"],
            "name": r["name"] or "",
            "applicationType": r["application_type"] or "",
            "user": r["user"] or "",
            "queue": r["queue"] or "",
        }

        workload_type = _classify_app_type(app)
        wf_name, action_name, wf_id = _extract_oozie_info(app["name"])

        tags = []
        if wf_name:
            tags.append("oozie-launched")
        if workload_type == WorkloadType.HIVE and HIVE_QUERY_PATTERN.match(app["name"]):
            tags.append("hive-initiated")

        item = WorkloadInventoryItem(
            workload_id=r["application_id"],
            workload_name=app["name"],
            workload_type=workload_type,
            user=app["user"],
            queue=app["queue"],
            entry_point=_infer_entry_point(app),
            code_artifacts=_infer_code_artifacts(app),
            oozie_workflow_name=wf_name,
            oozie_workflow_id=wf_id,
            yarn_app_id=r["application_id"],
            source="yarn",
            tags=tags,
            final_status=r["final_status"],
            started_time=r["started_time"],
            finished_time=r["finished_time"],
            elapsed_time=r["elapsed_time_ms"],
            memory_seconds=r["memory_seconds"],
            vcore_seconds=r["vcore_seconds"],
            diagnostics=r["diagnostics"] or None,
        )

        # Enriched fields from yarn_analysis_vw
        if use_enriched:
            item.job_type = r.get("job_type")
            item.memory_gb_hours = r.get("memory_gb_hours")
            item.vcore_hours = r.get("vcore_hours")
            item.elapsed_time_mins = r.get("elapsed_time_mins")
            item.dollar_dbus = r.get("dollar_dbus")
            item.dollar_vm = r.get("dollar_vm")
            item.total_cost = r.get("total_cost")

        items.append(item)

    logger.info(
        "Loaded %d YARN applications from DuckDB (%s)",
        len(items), source_table,
    )
    return items


def _load_spark(conn: duckdb.DuckDBPyConnection) -> List[WorkloadInventoryItem]:
    """Load Spark applications from DuckDB."""
    if not _table_exists(conn, "spark_applications"):
        logger.warning("Table spark_applications not found in DuckDB")
        return []

    rows = conn.execute("""
        SELECT application_id, name, spark_user
        FROM spark_applications
    """).fetchall()

    columns = ["application_id", "name", "spark_user"]

    items = []
    for row in rows:
        r = dict(zip(columns, row))
        name = r["name"] or ""

        # Infer entry point (check .py before class-name to avoid false match)
        entry_point = None
        artifacts = []
        if name.endswith(".py"):
            entry_point = name
            artifacts.append(CodeArtifact(
                path=name,
                location_type="local",
                artifact_type="py",
            ))
        elif "." in name and not name.startswith("PySpark") and " " not in name:
            entry_point = name

        tags = []
        if name.startswith("PySpark"):
            tags.append("pyspark")

        item = WorkloadInventoryItem(
            workload_id=r["application_id"],
            workload_name=name,
            workload_type=WorkloadType.SPARK,
            user=r["spark_user"] or "",
            queue="",
            entry_point=entry_point,
            code_artifacts=artifacts,
            yarn_app_id=r["application_id"],
            source="spark_hs",
            tags=tags,
        )
        items.append(item)

    logger.info("Loaded %d Spark applications from DuckDB", len(items))
    return items


def _load_impala(conn: duckdb.DuckDBPyConnection) -> List[WorkloadInventoryItem]:
    """Load Impala queries from DuckDB."""
    if not _table_exists(conn, "impala_queries"):
        logger.warning("Table impala_queries not found in DuckDB")
        return []

    rows = conn.execute("""
        SELECT query_id, statement, query_type, "user",
               database_name, rows_produced, duration_millis
        FROM impala_queries
    """).fetchall()

    columns = [
        "query_id", "statement", "query_type", "user",
        "database_name", "rows_produced", "duration_millis",
    ]

    items = []
    for row in rows:
        r = dict(zip(columns, row))
        statement = (r["statement"] or "").strip()
        query_type = r["query_type"] or ""

        artifacts = []
        if statement:
            artifacts.append(CodeArtifact(
                path=statement,
                location_type="embedded",
                artifact_type="sql",
            ))

        display_name = statement[:80] + "..." if len(statement) > 80 else statement

        item = WorkloadInventoryItem(
            workload_id=r["query_id"],
            workload_name=display_name,
            workload_type=WorkloadType.IMPALA,
            user=r["user"] or "",
            queue="",
            code_artifacts=artifacts,
            source="impala",
            tags=[f"query_type:{query_type.lower()}"] if query_type else [],
            database=r["database_name"],
            query_type=query_type,
            rows_produced=r["rows_produced"],
            duration_millis=r["duration_millis"],
        )
        items.append(item)

    logger.info("Loaded %d Impala queries from DuckDB", len(items))
    return items


def _load_table_as_dicts(
    conn: duckdb.DuckDBPyConnection, table_name: str,
) -> List[Dict[str, Any]]:
    """Load all rows from a table as a list of dicts. Returns [] if missing."""
    if not _table_exists(conn, table_name):
        return []
    result = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    columns = [desc[0] for desc in conn.description]
    return [dict(zip(columns, row)) for row in result]


def _load_summaries(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """Load pre-computed summary tables from DuckDB."""
    return {
        "by_job_type": _load_table_as_dicts(conn, "workload_summary_by_type"),
        "by_user": _load_table_as_dicts(conn, "workload_summary_by_user"),
        "by_queue": _load_table_as_dicts(conn, "workload_summary_by_queue"),
        "demand_profile": _load_table_as_dicts(conn, "hourly_yarn_view"),
    }
