"""Ranger security data loaders for DuckDB."""

import json
import logging
import os

from duckdb_exporter.utils import (
    find_json_files,
    extract_timestamp_from_filename,
)

logger = logging.getLogger(__name__)


def load_ranger_policies(conn, base_dir: str) -> int:
    """Load Ranger policies from JSON files into DuckDB.

    Handles error responses gracefully (HTTP 404, 403, 500) by returning 0 rows.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "RANGER", "Ranger_Policies*.json")
    if not files:
        logger.info("No Ranger policies files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Check for error response
            if "status" in data and isinstance(data["status"], int) and data["status"] >= 400:
                logger.warning("Ranger policies file returned error %d: %s", data["status"], data.get("message", ""))
                continue

            # Extract policies array
            policies_list = data.get("vXPolicies", [])
            if not policies_list:
                logger.warning("No vXPolicies found in %s", filepath)
                continue

            rows = []
            for policy in policies_list:
                row = (
                    policy.get("description"),
                    policy.get("isAuditEnabled"),
                    policy.get("isEnabled"),
                    policy.get("isRecursive"),
                    json.dumps(policy.get("permMapList", {})),  # Store as JSON
                    policy.get("policyName"),
                    policy.get("replacePerm"),
                    policy.get("repositoryName"),
                    policy.get("repositoryType"),
                    policy.get("resourceName"),
                    policy.get("udfs"),
                    policy.get("version"),
                    extraction_ts,
                )
                rows.append(row)

            conn.executemany("""
                INSERT INTO ranger_policies (
                    description, is_audit_enabled, is_enabled, is_recursive,
                    permission_list, policy_name, replace_perm, repository_name,
                    repository_type, resource_name, udfs, version, extraction_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)

            total_rows += len(rows)
            logger.info("Loaded %d rows into ranger_policies from %s", len(rows), os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows


def load_ranger_repos(conn, base_dir: str) -> int:
    """Load Ranger repositories from JSON files into DuckDB.

    Args:
        conn: DuckDB connection
        base_dir: Base directory containing profiler output

    Returns:
        Number of rows inserted
    """
    files = find_json_files(base_dir, "RANGER", "Ranger_Repos*.json")
    if not files:
        logger.info("No Ranger repos files found")
        return 0

    total_rows = 0
    for filepath in files:
        try:
            extraction_ts = extract_timestamp_from_filename(filepath)

            with open(filepath, 'r') as f:
                data = json.load(f)

            # Check for error response
            if "status" in data and isinstance(data["status"], int) and data["status"] >= 400:
                logger.warning("Ranger repos file returned error %d: %s", data["status"], data.get("message", ""))
                continue

            # Extract repos array
            repos_list = data.get("vXRepositories", [])
            if not repos_list:
                logger.warning("No vXRepositories found in %s", filepath)
                continue

            rows = []
            for repo in repos_list:
                row = (
                    repo.get("isActive"),
                    repo.get("name"),
                    repo.get("owner"),
                    repo.get("repositoryType"),
                    json.dumps(repo.get("config", {})),  # Store as JSON
                    extraction_ts,
                )
                rows.append(row)

            conn.executemany("""
                INSERT INTO ranger_repos (
                    is_active, name, owner, repository_type, config, extraction_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, rows)

            total_rows += len(rows)
            logger.info("Loaded %d rows into ranger_repos from %s", len(rows), os.path.basename(filepath))

        except Exception as e:
            logger.error("Failed to load %s: %s", filepath, e)
            continue

    return total_rows
