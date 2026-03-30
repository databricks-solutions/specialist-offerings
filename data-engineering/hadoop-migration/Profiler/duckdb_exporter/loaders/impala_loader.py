"""Impala data loaders for DuckDB."""

import json
import logging
import os

from duckdb_exporter.utils import (
    find_json_files,
    extract_timestamp_from_impala_filename,
)

logger = logging.getLogger(__name__)


def load_impala_queries(conn, base_dir: str) -> int:
    """Load Impala query data from JSON files into DuckDB.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "IMPALA", "impala_*.json")
    if not files:
        logger.info("No Impala query files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_impala_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Extract queries array
            queries = data.get("queries", [])
            if not queries:
                logger.warning("No queries found in %s", filepath)
                continue

            # Prepare batch insert data
            rows = []
            for query in queries:
                query_id = query.get("queryId")
                statement = query.get("statement")
                query_type = query.get("queryType")
                query_state = query.get("queryState")
                user = query.get("user")
                database = query.get("database")
                start_time = query.get("startTime")
                end_time = query.get("endTime")
                duration_millis = query.get("durationMillis")
                rows_produced = query.get("rowsProduced")

                # Compute duration in minutes
                duration_minutes = None
                if duration_millis is not None:
                    try:
                        duration_minutes = float(duration_millis) / 60000.0
                    except (ValueError, TypeError):
                        logger.debug("Failed to compute duration_minutes for query %s", query_id)

                # Extract coordinator from attributes if present
                attributes = query.get("attributes", {})
                coordinator = attributes.get("coordinator") if isinstance(attributes, dict) else None

                row = (
                    query_id,
                    statement,
                    query_type,
                    query_state,
                    user,
                    database,
                    start_time,
                    end_time,
                    duration_millis,
                    duration_minutes,
                    rows_produced,
                    coordinator,
                    extraction_ts,
                )
                rows.append(row)

            # Use INSERT OR REPLACE to handle duplicates from paginated files
            conn.executemany("""
                INSERT OR REPLACE INTO impala_queries (
                    query_id, statement, query_type, query_state, "user",
                    database_name, start_time, end_time, duration_millis,
                    duration_minutes, rows_produced, coordinator,
                    extraction_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)

            total_rows += len(rows)
            logger.info("Loaded %d Impala queries from %s", len(rows), os.path.basename(filepath))

            # Log warnings if present
            warnings = data.get("warnings", [])
            if warnings:
                logger.warning("File %s contains %d warnings", os.path.basename(filepath), len(warnings))
                for warning in warnings:
                    logger.debug("Warning: %s", warning)

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows
