"""YARN data loaders for DuckDB."""

import json
import logging
import os

from duckdb_exporter.utils import (
    find_json_files,
    extract_timestamp_from_filename,
)

logger = logging.getLogger(__name__)


def load_yarn_applications(conn, base_dir: str) -> int:
    """Load YARN application data from JSON files into DuckDB.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "YARN", "YarnApplicationDump*.json")
    if not files:
        logger.info("No YARN application files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Extract apps array from nested structure
            apps = data.get("apps", {}).get("app", [])
            if not apps:
                logger.warning("No apps found in %s", filepath)
                continue

            # Prepare batch insert data
            rows = []
            for app in apps:
                row = (
                    app.get("id"),
                    app.get("name"),
                    app.get("user"),
                    app.get("queue"),
                    app.get("state"),
                    app.get("finalStatus"),
                    app.get("applicationType"),
                    app.get("startedTime"),
                    app.get("finishedTime"),
                    app.get("elapsedTime"),
                    app.get("memorySeconds"),
                    app.get("vcoreSeconds"),
                    app.get("allocatedMB"),
                    app.get("allocatedVCores"),
                    app.get("runningContainers"),
                    app.get("diagnostics"),
                    app.get("trackingUrl"),
                    app.get("logAggregationStatus"),
                    app.get("applicationTags"),
                    extraction_ts,
                )
                rows.append(row)

            # Use INSERT OR REPLACE to handle duplicates
            conn.executemany("""
                INSERT OR REPLACE INTO yarn_applications (
                    application_id, name, "user", queue, state, final_status,
                    application_type, started_time, finished_time, elapsed_time_ms,
                    memory_seconds, vcore_seconds, allocated_mb, allocated_vcores,
                    running_containers, diagnostics, tracking_url,
                    log_aggregation_status, application_tags, extraction_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)

            total_rows += len(rows)
            logger.info("Loaded %d applications from %s", len(rows), os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows


def load_yarn_metrics(conn, base_dir: str) -> int:
    """Load YARN cluster metrics from JSON files into DuckDB.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "YARN", "YarnMetricsDump*.json")
    if not files:
        logger.info("No YARN metrics files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Extract metrics object
            metrics = data.get("clusterMetrics", {})
            if not metrics:
                logger.warning("No clusterMetrics found in %s", filepath)
                continue

            # Insert single row
            conn.execute("""
                INSERT INTO yarn_cluster_metrics (
                    apps_submitted, apps_completed, apps_pending, apps_running,
                    apps_failed, apps_killed, reserved_mb, available_mb,
                    allocated_mb, total_mb, reserved_vcores, available_vcores,
                    allocated_vcores, total_vcores, containers_allocated,
                    containers_reserved, containers_pending, total_nodes,
                    active_nodes, lost_nodes, unhealthy_nodes,
                    decommissioned_nodes, rebooted_nodes, extraction_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                metrics.get("appsSubmitted"),
                metrics.get("appsCompleted"),
                metrics.get("appsPending"),
                metrics.get("appsRunning"),
                metrics.get("appsFailed"),
                metrics.get("appsKilled"),
                metrics.get("reservedMB"),
                metrics.get("availableMB"),
                metrics.get("allocatedMB"),
                metrics.get("totalMB"),
                metrics.get("reservedVirtualCores"),
                metrics.get("availableVirtualCores"),
                metrics.get("allocatedVirtualCores"),
                metrics.get("totalVirtualCores"),
                metrics.get("containersAllocated"),
                metrics.get("containersReserved"),
                metrics.get("containersPending"),
                metrics.get("totalNodes"),
                metrics.get("activeNodes"),
                metrics.get("lostNodes"),
                metrics.get("unhealthyNodes"),
                metrics.get("decommissionedNodes"),
                metrics.get("rebootedNodes"),
                extraction_ts,
            ))

            total_rows += 1
            logger.info("Loaded cluster metrics from %s", os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows


def load_yarn_nodes(conn, base_dir: str) -> int:
    """Load YARN node data from JSON files into DuckDB.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "YARN", "YarnNodesDump*.json")
    if not files:
        logger.info("No YARN nodes files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Extract nodes array from nested structure
            nodes = data.get("nodes", {}).get("node", [])
            if not nodes:
                logger.warning("No nodes found in %s", filepath)
                continue

            # Prepare batch insert data
            rows = []
            for node in nodes:
                row = (
                    node.get("id"),
                    node.get("nodeHostName"),
                    node.get("rack"),
                    node.get("state"),
                    node.get("nodeHTTPAddress"),
                    node.get("version"),
                    node.get("lastHealthUpdate"),
                    node.get("healthReport"),
                    node.get("numContainers"),
                    node.get("usedMemoryMB"),
                    node.get("availMemoryMB"),
                    node.get("usedVirtualCores"),
                    node.get("availableVirtualCores"),
                    extraction_ts,
                )
                rows.append(row)

            # Use INSERT OR REPLACE to handle duplicates
            conn.executemany("""
                INSERT OR REPLACE INTO yarn_nodes (
                    node_id, node_host_name, rack, state, node_http_address,
                    version, last_health_update, health_report, num_containers,
                    used_memory_mb, avail_memory_mb, used_virtual_cores,
                    available_virtual_cores, extraction_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)

            total_rows += len(rows)
            logger.info("Loaded %d nodes from %s", len(rows), os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows


def load_yarn_scheduler(conn, base_dir: str) -> int:
    """Load YARN scheduler queue data from JSON files into DuckDB.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "YARN", "YarnSchedulerDump*.json")
    if not files:
        logger.info("No YARN scheduler files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Navigate nested structure
            scheduler_info = data.get("scheduler", {}).get("schedulerInfo", {})
            if not scheduler_info:
                logger.warning("No schedulerInfo found in %s", filepath)
                continue

            scheduler_type = scheduler_info.get("type")
            root_queue = scheduler_info.get("rootQueue", {})

            # Extract child queues
            child_queues = root_queue.get("childQueues", [])
            if not child_queues:
                logger.warning("No childQueues found in %s", filepath)
                continue

            # Prepare batch insert data
            rows = []
            for queue in child_queues:
                max_res = queue.get("maxResources", {})
                min_res = queue.get("minResources", {})
                used_res = queue.get("usedResources", {})
                steady_fair_res = queue.get("steadyFairResources", {})

                row = (
                    queue.get("queueName"),
                    scheduler_type,
                    queue.get("schedulingPolicy"),
                    max_res.get("memory"),
                    max_res.get("vCores"),
                    min_res.get("memory"),
                    min_res.get("vCores"),
                    used_res.get("memory"),
                    used_res.get("vCores"),
                    steady_fair_res.get("memory"),
                    steady_fair_res.get("vCores"),
                    queue.get("numPendingApps"),
                    queue.get("numActiveApps"),
                    queue.get("preemptable"),
                    extraction_ts,
                )
                rows.append(row)

            # Use INSERT OR REPLACE to handle duplicates
            conn.executemany("""
                INSERT OR REPLACE INTO yarn_scheduler_queues (
                    queue_name, scheduler_type, scheduling_policy,
                    max_resources_memory, max_resources_vcores,
                    min_resources_memory, min_resources_vcores,
                    used_resources_memory, used_resources_vcores,
                    steady_fair_memory, steady_fair_vcores,
                    num_pending_apps, num_active_apps, preemptable,
                    extraction_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)

            total_rows += len(rows)
            logger.info("Loaded %d queues from %s", len(rows), os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows
