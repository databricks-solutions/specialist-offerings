"""Main orchestrator for the DuckDB export pipeline."""

import logging
import os
from datetime import datetime, timezone

import duckdb

from duckdb_exporter.config import ExporterConfig
from duckdb_exporter.schema import create_all_base_tables
from duckdb_exporter.loaders.yarn_loader import (
    load_yarn_applications, load_yarn_metrics, load_yarn_nodes, load_yarn_scheduler,
)
from duckdb_exporter.loaders.spark_loader import load_spark_applications
from duckdb_exporter.loaders.impala_loader import load_impala_queries
from duckdb_exporter.loaders.cm_loader import load_all_cm
from duckdb_exporter.transforms.yarn_analysis import create_all_yarn_analysis
from duckdb_exporter.transforms.summary_tables import create_all_summary_tables

logger = logging.getLogger(__name__)


def run_export(config: ExporterConfig):
    """Run the full export pipeline: JSON → DuckDB."""
    base_dir = config.profiler_output.base_dir
    db_path = config.output.db_path

    if not os.path.isdir(base_dir):
        raise FileNotFoundError(f"Profiler output directory not found: {base_dir}")

    # Handle existing DB file
    if os.path.exists(db_path):
        if config.output.overwrite:
            logger.info("Overwriting existing DB: %s", db_path)
            os.remove(db_path)
        else:
            raise FileExistsError(f"DB file already exists (set overwrite=true): {db_path}")

    # Ensure output directory exists
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    logger.info("Exporting profiler data from %s to %s", base_dir, db_path)

    conn = duckdb.connect(db_path)
    try:
        # Create all base table schemas
        create_all_base_tables(conn)
        total_rows = 0
        tables_created = 0

        # Load YARN data
        if config.sources.yarn:
            logger.info("Loading YARN data...")
            rows = load_yarn_applications(conn, base_dir)
            if rows:
                tables_created += 1
            total_rows += rows

            rows = load_yarn_metrics(conn, base_dir)
            if rows:
                tables_created += 1
            total_rows += rows

            rows = load_yarn_nodes(conn, base_dir)
            if rows:
                tables_created += 1
            total_rows += rows

            rows = load_yarn_scheduler(conn, base_dir)
            if rows:
                tables_created += 1
            total_rows += rows

        # Load Spark data
        if config.sources.spark:
            logger.info("Loading Spark data...")
            rows = load_spark_applications(conn, base_dir)
            if rows:
                tables_created += 1
            total_rows += rows

        # Load Impala data
        if config.sources.impala:
            logger.info("Loading Impala data...")
            rows = load_impala_queries(conn, base_dir)
            if rows:
                tables_created += 1
            total_rows += rows

        # Load CM data
        if config.sources.cm:
            logger.info("Loading Cloudera Manager data...")
            rows = load_all_cm(conn, base_dir)
            total_rows += rows
            # Count CM tables that got data
            for tbl in ["cm_hosts", "cm_services", "cm_config", "cm_export",
                        "cm_host_roles", "cm_hdfs_usage", "cm_cpu_utilization",
                        "cm_memory_utilization", "cm_yarn_memory_cpu",
                        "cm_yarn_utilization", "cm_impala_utilization"]:
                try:
                    cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                    if cnt > 0:
                        tables_created += 1
                except Exception:
                    pass

        # Create derived tables (only if YARN data was loaded)
        yarn_count = 0
        try:
            yarn_count = conn.execute("SELECT COUNT(*) FROM yarn_applications").fetchone()[0]
        except Exception:
            pass

        if yarn_count > 0:
            logger.info("Creating derived analysis tables...")
            rows = create_all_yarn_analysis(
                conn, config.cost_rates.dbu_rate, config.cost_rates.vm_rate
            )
            total_rows += rows
            tables_created += 3  # yarn_analysis_vw, oozie_analysis_vw, hourly_yarn_view

            rows = create_all_summary_tables(conn)
            total_rows += rows
            tables_created += 3  # by_user, by_queue, by_type
        else:
            logger.warning("No YARN applications found — skipping derived tables")

        # Write export metadata
        conn.execute(
            "INSERT INTO export_metadata VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                datetime.now(timezone.utc),
                base_dir,
                tables_created,
                total_rows,
                config.cost_rates.dbu_rate,
                config.cost_rates.vm_rate,
                duckdb.__version__,
            ],
        )

        # Print summary
        print(f"\nExport complete: {db_path}")
        print(f"  Tables with data: {tables_created}")
        print(f"  Total rows: {total_rows}")
        print(f"  DuckDB version: {duckdb.__version__}")
        print(f"  Cost rates: DBU=${config.cost_rates.dbu_rate}/GB-hr, VM=${config.cost_rates.vm_rate}/GB-hr")

        # Show table summary
        all_tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()
        print(f"\n  Tables ({len(all_tables)}):")
        for (tbl,) in all_tables:
            try:
                cnt = conn.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0]
                print(f"    {tbl}: {cnt} rows")
            except Exception:
                print(f"    {tbl}: (empty)")

    finally:
        conn.close()
