"""Spark data loaders for DuckDB."""

import json
import logging
import os
from datetime import datetime

from duckdb_exporter.utils import (
    find_json_files,
    extract_timestamp_from_filename,
)

logger = logging.getLogger(__name__)


def parse_spark_timestamp(timestamp_str: str) -> datetime:
    """Parse Spark timestamp string to datetime.

    Args:
        timestamp_str: Timestamp in format like "2026-03-19T01:50:30.563GMT"

    Returns:
        datetime object or None if parsing fails
    """
    if not timestamp_str:
        return None

    try:
        # Handle GMT suffix
        if timestamp_str.endswith("GMT"):
            timestamp_str = timestamp_str[:-3]

        # Parse ISO format with milliseconds
        return datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
    except Exception as e:
        logger.debug("Failed to parse timestamp '%s': %s", timestamp_str, e)
        return None


def compute_duration_ms(start_time_str: str, end_time_str: str) -> int:
    """Compute duration in milliseconds between start and end times.

    Args:
        start_time_str: Start time string
        end_time_str: End time string

    Returns:
        Duration in milliseconds or None if computation fails
    """
    start_time = parse_spark_timestamp(start_time_str)
    end_time = parse_spark_timestamp(end_time_str)

    if start_time and end_time:
        delta = end_time - start_time
        return int(delta.total_seconds() * 1000)
    return None


def load_spark_applications(conn, base_dir: str) -> int:
    """Load Spark application data from JSON files into DuckDB.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "SPARK", "Spark_Applications*.json")
    if not files:
        logger.info("No Spark application files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Data is a top-level array
            if not isinstance(data, list):
                logger.warning("Expected array in %s, got %s", filepath, type(data))
                continue

            # Prepare batch insert data
            rows = []
            for app in data:
                app_id = app.get("id")
                app_name = app.get("name")
                attempts = app.get("attempts", [])

                # If no attempts, create a single row with default values
                if not attempts:
                    attempts = [{}]

                # Create one row per attempt
                for attempt in attempts:
                    attempt_id = attempt.get("attemptId", "1")
                    start_time = attempt.get("startTime", "")
                    end_time = attempt.get("endTime", "")
                    spark_user = attempt.get("sparkUser")
                    completed = attempt.get("completed", False)

                    # Compute duration
                    duration_ms = compute_duration_ms(start_time, end_time)

                    row = (
                        app_id,
                        attempt_id,
                        app_name,
                        spark_user,
                        start_time,
                        end_time,
                        duration_ms,
                        completed,
                        extraction_ts,
                    )
                    rows.append(row)

            # Use INSERT OR REPLACE to handle duplicates
            conn.executemany("""
                INSERT OR REPLACE INTO spark_applications (
                    application_id, attempt_id, name, spark_user,
                    start_time, end_time, duration_ms, completed,
                    extraction_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)

            total_rows += len(rows)
            logger.info("Loaded %d Spark application attempts from %s", len(rows), os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows
