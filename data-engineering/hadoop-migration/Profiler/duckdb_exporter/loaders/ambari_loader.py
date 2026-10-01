"""Ambari (HDP) data loaders for DuckDB."""

import json
import logging
import os

from duckdb_exporter.utils import (
    find_json_files,
    extract_timestamp_from_filename,
)

logger = logging.getLogger(__name__)


def load_ambari_hosts(conn, base_dir: str) -> int:
    """Load Ambari host data from JSON files into DuckDB.

    Handles error responses gracefully (HTTP 404, 403, 500) by returning 0 rows.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "AMBARI", "AmbariHost*.json")
    if not files:
        logger.info("No Ambari host files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Check for error response (status field indicates error)
            if "status" in data and isinstance(data["status"], int) and data["status"] >= 400:
                logger.warning("Ambari host file returned error %d: %s", data["status"], data.get("message", ""))
                continue

            # Extract hosts array from nested structure
            hosts = data.get("items", [])
            if not hosts:
                logger.warning("No hosts found in %s", filepath)
                continue

            # Prepare batch insert data
            rows = []
            for host in hosts:
                host_info = host.get("Hosts", {})
                disk_info_list = host_info.get("disk_info", [])

                # If no disk info, insert one row with null disk fields
                if not disk_info_list:
                    row = (
                        host_info.get("host_name"),
                        host_info.get("cpu_count"),
                        host_info.get("total_mem"),
                        host_info.get("os_type"),
                        None,  # mountpoint
                        None,  # disk_used_percent
                        None,  # disk_size_mb
                        None,  # disk_used_mb
                        extraction_ts,
                    )
                    rows.append(row)
                else:
                    # Create one row per disk
                    for disk in disk_info_list:
                        row = (
                            host_info.get("host_name"),
                            host_info.get("cpu_count"),
                            host_info.get("total_mem"),
                            host_info.get("os_type"),
                            disk.get("mountpoint"),
                            disk.get("percent"),
                            disk.get("size"),
                            disk.get("used"),
                            extraction_ts,
                        )
                        rows.append(row)

            # Use INSERT OR REPLACE to handle duplicates
            conn.executemany("""
                INSERT OR REPLACE INTO ambari_hosts (
                    host_name, cpu_count, total_mem, os_type, mountpoint,
                    disk_used_percent, disk_size_mb, disk_used_mb, extraction_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)

            total_rows += len(rows)
            logger.info("Loaded %d rows into ambari_hosts from %s", len(rows), os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows


def load_ambari_host_components(conn, base_dir: str) -> int:
    """Load Ambari host components from JSON files into DuckDB."""
    files = find_json_files(base_dir, "AMBARI", "AmbariComponents*.json")
    if not files:
        logger.info("No Ambari components files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Check for error response
            if "status" in data and isinstance(data["status"], int) and data["status"] >= 400:
                logger.warning("Ambari components file returned error %d: %s", data["status"], data.get("message", ""))
                continue

            # Extract components array
            items = data.get("items", [])
            if not items:
                logger.warning("No components found in %s", filepath)
                continue

            rows = []
            for item in items:
                cluster_name = item.get("Hosts", {}).get("cluster_name")
                host_name = item.get("Hosts", {}).get("host_name")
                host_components = item.get("host_components", [])

                for comp in host_components:
                    row = (
                        cluster_name,
                        comp.get("HostRoles", {}).get("component_name"),
                        host_name,
                        extraction_ts,
                    )
                    rows.append(row)

            conn.executemany("""
                INSERT INTO ambari_host_components (
                    cluster_name, component_name, host_name, extraction_timestamp
                ) VALUES (?, ?, ?, ?)
            """, rows)

            total_rows += len(rows)
            logger.info("Loaded %d rows into ambari_host_components from %s", len(rows), os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows


def load_ambari_stack(conn, base_dir: str) -> int:
    """Load Ambari stack data from JSON files into DuckDB."""
    files = find_json_files(base_dir, "AMBARI", "AmbariStack*.json")
    if not files:
        logger.info("No Ambari stack files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Check for error response
            if "status" in data and isinstance(data["status"], int) and data["status"] >= 400:
                logger.warning("Ambari stack file returned error %d: %s", data["status"], data.get("message", ""))
                continue

            # Extract ClusterStackVersions
            stack_versions = data.get("ClusterStackVersions", {})
            if not stack_versions:
                logger.warning("No ClusterStackVersions found in %s", filepath)
                # Create empty row
                conn.execute("""
                    INSERT INTO ambari_stack (
                        cluster_name, stack, version, services, extraction_timestamp
                    ) VALUES (?, ?, ?, ?, ?)
                """, (None, None, None, [], extraction_ts))
                total_rows += 1
                continue

            cluster_name = stack_versions.get("cluster_name")
            stack = stack_versions.get("stack")
            version = stack_versions.get("version")
            services = stack_versions.get("repository_summary", {}).get("services", [])

            conn.execute("""
                INSERT INTO ambari_stack (
                    cluster_name, stack, version, services, extraction_timestamp
                ) VALUES (?, ?, ?, ?, ?)
            """, (cluster_name, stack, version, services, extraction_ts))

            total_rows += 1
            logger.info("Loaded ambari_stack from %s", os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows


def load_ambari_services(conn, base_dir: str) -> int:
    """Load Ambari services from JSON files into DuckDB."""
    files = find_json_files(base_dir, "AMBARI", "AmbariServices*.json")
    if not files:
        logger.info("No Ambari services files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Check for error response
            if "status" in data and isinstance(data["status"], int) and data["status"] >= 400:
                logger.warning("Ambari services file returned error %d: %s", data["status"], data.get("message", ""))
                continue

            # Extract services array
            items = data.get("items", [])
            if not items:
                logger.warning("No services found in %s", filepath)
                continue

            rows = []
            for item in items:
                service_name = item.get("ServiceInfo", {}).get("service_name")
                if service_name:
                    row = (service_name, extraction_ts)
                    rows.append(row)

            conn.executemany("""
                INSERT INTO ambari_services (
                    service_installed, extraction_timestamp
                ) VALUES (?, ?)
            """, rows)

            total_rows += len(rows)
            logger.info("Loaded %d rows into ambari_services from %s", len(rows), os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows


def load_ambari_yarn_and_hbase_allocation(conn, base_dir: str) -> tuple:
    """Load Ambari YARN and HBase allocation from Blueprint JSON files.

    Returns:
        Tuple of (yarn_rows, hbase_rows)
    """
    files = find_json_files(base_dir, "AMBARI", "AmbariBlueprint*.json")
    if not files:
        logger.info("No Ambari blueprint files found")
        return 0, 0

    yarn_rows = 0
    hbase_rows = 0

    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Check for error response
            if "status" in data and isinstance(data["status"], int) and data["status"] >= 400:
                logger.warning("Ambari blueprint file returned error %d: %s", data["status"], data.get("message", ""))
                continue

            # Extract configurations
            configurations = data.get("configurations", [])
            if not configurations:
                logger.warning("No configurations found in %s", filepath)
                # Create dummy rows
                conn.execute("""
                    INSERT INTO ambari_yarn_allocation (
                        service, total_vcores, total_memory, max_alloc_mb, max_alloc_vcores,
                        min_alloc_mb, min_alloc_vcores, extraction_timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, ('YARN', 0, 0, 0, 0, 0, 0, extraction_ts))
                yarn_rows += 1

                conn.execute("""
                    INSERT INTO ambari_hbase_allocation (
                        service, hbase_region_memory, hbase_master_memory, extraction_timestamp
                    ) VALUES (?, ?, ?, ?)
                """, ('HBASE', 0, 0, extraction_ts))
                hbase_rows += 1
                continue

            # Process configurations to find yarn-site and hbase-env
            for config_item in configurations:
                if "yarn-site" in config_item:
                    properties = config_item["yarn-site"].get("properties", {})
                    if properties:
                        conn.execute("""
                            INSERT INTO ambari_yarn_allocation (
                                service, total_vcores, total_memory, max_alloc_mb, max_alloc_vcores,
                                min_alloc_mb, min_alloc_vcores, extraction_timestamp
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            'YARN',
                            properties.get("yarn.nodemanager.resource.cpu-vcores"),
                            properties.get("yarn.nodemanager.resource.memory-mb"),
                            properties.get("yarn.scheduler.maximum-allocation-mb"),
                            properties.get("yarn.scheduler.maximum-allocation-vcores"),
                            properties.get("yarn.scheduler.minimum-allocation-mb"),
                            properties.get("yarn.scheduler.minimum-allocation-vcores"),
                            extraction_ts,
                        ))
                        yarn_rows += 1

                if "hbase-env" in config_item:
                    properties = config_item["hbase-env"].get("properties", {})
                    if properties:
                        conn.execute("""
                            INSERT INTO ambari_hbase_allocation (
                                service, hbase_region_memory, hbase_master_memory, extraction_timestamp
                            ) VALUES (?, ?, ?, ?)
                        """, (
                            'HBASE',
                            properties.get("hbase_regionserver_heapsize"),
                            properties.get("hbase_master_heapsize"),
                            extraction_ts,
                        ))
                        hbase_rows += 1

            # If no YARN config found, insert default
            if yarn_rows == 0:
                conn.execute("""
                    INSERT INTO ambari_yarn_allocation (
                        service, total_vcores, total_memory, max_alloc_mb, max_alloc_vcores,
                        min_alloc_mb, min_alloc_vcores, extraction_timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, ('YARN', 0, 0, 0, 0, 0, 0, extraction_ts))
                yarn_rows += 1

            # If no HBASE config found, insert default
            if hbase_rows == 0:
                conn.execute("""
                    INSERT INTO ambari_hbase_allocation (
                        service, hbase_region_memory, hbase_master_memory, extraction_timestamp
                    ) VALUES (?, ?, ?, ?)
                """, ('HBASE', 0, 0, extraction_ts))
                hbase_rows += 1

            logger.info("Loaded Ambari YARN/HBase allocation from %s", os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return yarn_rows, hbase_rows


def load_hdfs_stats(conn, base_dir: str) -> int:
    """Load HDFS stats from AmbariHDFS JSON files into DuckDB.

    Extracts HDFS capacity metrics in GB units.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "AMBARI", "AmbariHDFS*.json")
    if not files:
        logger.info("No Ambari HDFS files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Check for error response
            if "status" in data and isinstance(data["status"], int) and data["status"] >= 400:
                logger.warning("Ambari HDFS file returned error %d: %s", data["status"], data.get("message", ""))
                continue

            # Try to extract HDFS metrics
            metrics = data.get("metrics", {})
            dfs_nameystem = metrics.get("dfs", {}).get("FSNamesystem", {})

            # Try CapacityTotal first (in bytes), then CapacityTotalGB
            capacity_total_gb = None
            capacity_remaining_gb = None
            capacity_used_gb = None

            if "CapacityTotal" in dfs_nameystem:
                # Value is in bytes, convert to GB
                capacity_total_bytes = dfs_nameystem.get("CapacityTotal")
                if capacity_total_bytes:
                    capacity_total_gb = float(capacity_total_bytes) / 1024 / 1024 / 1024
            elif "CapacityTotalGB" in dfs_nameystem:
                capacity_total_gb = dfs_nameystem.get("CapacityTotalGB")

            if "CapacityRemaining" in dfs_nameystem:
                capacity_remaining_bytes = dfs_nameystem.get("CapacityRemaining")
                if capacity_remaining_bytes:
                    capacity_remaining_gb = float(capacity_remaining_bytes) / 1024 / 1024 / 1024
            elif "CapacityRemainingGB" in dfs_nameystem:
                capacity_remaining_gb = dfs_nameystem.get("CapacityRemainingGB")

            if "CapacityUsed" in dfs_nameystem:
                capacity_used_bytes = dfs_nameystem.get("CapacityUsed")
                if capacity_used_bytes:
                    capacity_used_gb = float(capacity_used_bytes) / 1024 / 1024 / 1024
            elif "CapacityUsedGB" in dfs_nameystem:
                capacity_used_gb = dfs_nameystem.get("CapacityUsedGB")

            # Extract service component info
            service_comp_info = data.get("ServiceComponentInfo", {})
            cluster_name = service_comp_info.get("cluster_name")
            component_name = service_comp_info.get("component_name")
            service_name = service_comp_info.get("service_name")

            conn.execute("""
                INSERT INTO hdfs_stats (
                    capacity_total_gb, capacity_remaining_gb, capacity_used_gb,
                    cluster_name, component_name, service_name, extraction_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                capacity_total_gb,
                capacity_remaining_gb,
                capacity_used_gb,
                cluster_name,
                component_name,
                service_name,
                extraction_ts,
            ))

            total_rows += 1
            logger.info("Loaded HDFS stats from %s (total_gb=%s)", os.path.basename(filepath), capacity_total_gb)

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows
