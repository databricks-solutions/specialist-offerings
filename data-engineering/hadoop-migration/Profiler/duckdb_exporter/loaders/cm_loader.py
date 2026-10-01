"""Cloudera Manager (CM) data loaders for DuckDB."""

import json
import logging
import os

from duckdb_exporter.utils import (
    find_json_files,
    extract_timestamp_from_filename,
)

logger = logging.getLogger(__name__)


def load_cm_hosts(conn, base_dir: str) -> int:
    """Load CM hosts data from JSON files into DuckDB.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "CM", "cmHosts_*.json")
    if not files:
        logger.warning("No CM hosts files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Extract hosts array
            hosts = data.get("items", [])
            if not hosts:
                logger.warning("No hosts found in %s", filepath)
                continue

            # Prepare batch insert data
            rows = []
            for host in hosts:
                total_mem_bytes = host.get("totalPhysMemBytes", 0)
                total_mem_gb = total_mem_bytes / 1073741824.0 if total_mem_bytes else None

                row = (
                    host.get("hostId"),
                    host.get("hostname"),
                    host.get("ipAddress"),
                    host.get("rackId"),
                    host.get("numCores"),
                    host.get("numPhysicalCores"),
                    total_mem_bytes,
                    total_mem_gb,
                    host.get("commissionState"),
                    host.get("maintenanceMode"),
                    extraction_ts,
                )
                rows.append(row)

            # Use INSERT OR REPLACE to handle duplicates
            conn.executemany("""
                INSERT OR REPLACE INTO cm_hosts (
                    host_id, hostname, ip_address, rack_id, num_cores,
                    num_physical_cores, total_phys_mem_bytes, total_phys_mem_gb,
                    commission_state, maintenance_mode, extraction_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)

            total_rows += len(rows)
            logger.info("Loaded %d rows into cm_hosts from %s", len(rows), os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows


def load_cm_services(conn, base_dir: str) -> int:
    """Load CM services data from JSON files into DuckDB.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "CM", "cmServices_*.json")
    if not files:
        logger.warning("No CM services files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Extract services array
            services = data.get("items", [])
            if not services:
                logger.warning("No services found in %s", filepath)
                continue

            # Prepare batch insert data
            rows = []
            for service in services:
                cluster_ref = service.get("clusterRef", {})
                row = (
                    service.get("name"),
                    service.get("type"),
                    service.get("displayName"),
                    cluster_ref.get("clusterName"),
                    service.get("serviceState"),
                    service.get("healthSummary"),
                    service.get("configStalenessStatus"),
                    service.get("maintenanceMode"),
                    extraction_ts,
                )
                rows.append(row)

            # Use INSERT OR REPLACE to handle duplicates
            conn.executemany("""
                INSERT OR REPLACE INTO cm_services (
                    service_name, service_type, display_name, cluster_name,
                    service_state, health_summary, config_staleness_status,
                    maintenance_mode, extraction_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)

            total_rows += len(rows)
            logger.info("Loaded %d rows into cm_services from %s", len(rows), os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows


def load_cm_config(conn, base_dir: str) -> int:
    """Load CM config data from JSON files into DuckDB.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "CM", "cmConfig_*.json")
    if not files:
        logger.warning("No CM config files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            rows = []

            # Try to parse as items array format
            if "items" in data and isinstance(data["items"], list):
                for item in data["items"]:
                    row = (
                        item.get("name"),
                        str(item.get("value", "")),
                        None,  # service_name
                        None,  # role_type
                        extraction_ts,
                    )
                    rows.append(row)
            # Try to parse as nested service config format
            elif isinstance(data, dict):
                for service_name, service_data in data.items():
                    if isinstance(service_data, dict):
                        for role_type, role_data in service_data.items():
                            if isinstance(role_data, dict):
                                for key, value in role_data.items():
                                    row = (
                                        key,
                                        str(value),
                                        service_name,
                                        role_type,
                                        extraction_ts,
                                    )
                                    rows.append(row)
            # Fallback: store entire JSON as raw_export
            else:
                row = (
                    "raw_export",
                    json.dumps(data),
                    None,
                    None,
                    extraction_ts,
                )
                rows.append(row)
                logger.warning("Unexpected config structure in %s, storing as raw_export", filepath)

            if rows:
                conn.executemany("""
                    INSERT INTO cm_config (
                        config_key, config_value, service_name, role_type,
                        extraction_timestamp
                    ) VALUES (?, ?, ?, ?, ?)
                """, rows)

                total_rows += len(rows)
                logger.info("Loaded %d rows into cm_config from %s", len(rows), os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows


def load_cm_export(conn, base_dir: str) -> int:
    """Load CM export data from JSON files into DuckDB.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "CM", "cmExport_*.json")
    if not files:
        logger.warning("No CM export files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            # Read entire file as text blob
            with open(filepath, 'r') as f:
                export_json = f.read()

            # Insert single row
            conn.execute("""
                INSERT INTO cm_export (
                    export_json, extraction_timestamp
                ) VALUES (?, ?)
            """, (export_json, extraction_ts))

            total_rows += 1
            logger.info("Loaded %d rows into cm_export from %s", 1, os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows


def load_cm_host_roles(conn, base_dir: str) -> int:
    """Load CM host roles data from JSON files into DuckDB.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "CM", "cmHostRoles_*.json")
    if not files:
        logger.warning("No CM host roles files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Extract timeseries items
            items = data.get("items", [])
            if not items:
                logger.warning("No items found in %s", filepath)
                continue

            # Collect unique roles (deduplicate by entity_name)
            roles_dict = {}
            for item in items:
                for ts in item.get("timeSeries", []):
                    metadata = ts.get("metadata", {})
                    attributes = metadata.get("attributes", {})

                    entity_name = attributes.get("entityName")
                    if not entity_name:
                        continue

                    # Deduplicate: keep first occurrence
                    if entity_name not in roles_dict:
                        roles_dict[entity_name] = (
                            entity_name,
                            attributes.get("roleName"),
                            attributes.get("roleType"),
                            attributes.get("serviceName"),
                            attributes.get("serviceType"),
                            attributes.get("hostname"),
                            attributes.get("clusterName"),
                            extraction_ts,
                        )

            rows = list(roles_dict.values())

            if rows:
                conn.executemany("""
                    INSERT INTO cm_host_roles (
                        entity_name, role_name, role_type, service_name,
                        service_type, hostname, cluster_name, extraction_timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, rows)

                total_rows += len(rows)
                logger.info("Loaded %d rows into cm_host_roles from %s", len(rows), os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows


def load_cm_timeseries(conn, base_dir: str, json_pattern: str, table_name: str) -> int:
    """Load CM timeseries data from JSON files into DuckDB.

    This is a generic loader for all CM timeseries tables.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output
        json_pattern: JSON file pattern to match
        table_name: Target table name

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "CM", json_pattern)
    if not files:
        logger.warning("No files matching %s found", json_pattern)
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Extract timeseries items
            items = data.get("items", [])
            if not items:
                logger.warning("No items found in %s", filepath)
                continue

            rows = []
            for item in items:
                for ts in item.get("timeSeries", []):
                    metadata = ts.get("metadata", {})
                    metric_name = metadata.get("metricName")
                    entity_name = metadata.get("entityName")
                    unit_numerators = metadata.get("unitNumerators", [])
                    unit = unit_numerators[0] if unit_numerators else None
                    attributes = metadata.get("attributes", {})
                    cluster_name = attributes.get("clusterName")

                    # Extract data points
                    data_points = ts.get("data", [])
                    if not data_points:
                        # Some timeseries may have no data points
                        continue

                    for point in data_points:
                        # Parse ISO timestamp
                        timestamp_str = point.get("timestamp")
                        value = point.get("value")

                        row = (
                            metric_name,
                            entity_name,
                            timestamp_str,  # DuckDB will parse ISO format
                            value,
                            unit,
                            cluster_name,
                            extraction_ts,
                        )
                        rows.append(row)

            if rows:
                conn.executemany(f"""
                    INSERT INTO {table_name} (
                        metric_name, entity_name, metric_timestamp, value,
                        unit, cluster_name, extraction_timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, rows)

                total_rows += len(rows)
                logger.info("Loaded %d rows into %s from %s", len(rows), table_name, os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows


def load_all_cm_timeseries(conn, base_dir: str) -> int:
    """Load all CM timeseries tables.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Total number of rows inserted across all timeseries tables
    """
    timeseries_configs = [
        ("cmHDFSUsage_*.json", "cm_hdfs_usage"),
        ("cmClusterCPUUtilization_*.json", "cm_cpu_utilization"),
        ("cmClusterMemoryUtilization_*.json", "cm_memory_utilization"),
        ("cmYarnMemoryAndCPU_*.json", "cm_yarn_memory_cpu"),
        ("cmYarnUtilization_*.json", "cm_yarn_utilization"),
        ("cmImpalaUtilization_*.json", "cm_impala_utilization"),
    ]

    total_rows = 0
    for json_pattern, table_name in timeseries_configs:
        rows = load_cm_timeseries(conn, base_dir, json_pattern, table_name)
        total_rows += rows

    return total_rows


def load_all_cm(conn, base_dir: str) -> int:
    """Load all CM data into DuckDB.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Total number of rows inserted
    """
    logger.info("Loading CM data from %s", base_dir)

    total_rows = 0
    total_rows += load_cm_hosts(conn, base_dir)
    total_rows += load_cm_services(conn, base_dir)
    total_rows += load_cm_config(conn, base_dir)
    total_rows += load_cm_export(conn, base_dir)
    total_rows += load_cm_host_roles(conn, base_dir)
    total_rows += load_all_cm_timeseries(conn, base_dir)

    logger.info("CM data loading complete: %d total rows", total_rows)
    return total_rows
