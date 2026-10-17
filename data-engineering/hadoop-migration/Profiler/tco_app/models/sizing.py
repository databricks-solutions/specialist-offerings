"""Right-sizing engine.

Uses system.compute.node_types to recommend instance types based on
workload memory/vcore requirements from the profiler data.
"""

import pandas as pd
from utils.db_connector import execute_query


def get_node_types(cloud: str) -> pd.DataFrame:
    """Fetch available node types for a cloud from system.compute.node_types."""
    return execute_query(f"""
        SELECT
            node_type_id AS node_type,
            num_cores AS core_count,
            memory_mb,
            ROUND(memory_mb / 1024.0, 1) AS memory_gb,
            num_gpus AS gpu_count,
            is_deprecated
        FROM system.compute.node_types
        WHERE cloud_type = '{cloud}'
          AND is_deprecated = false
        ORDER BY memory_mb ASC
    """)


def recommend_node_type(
    peak_memory_gb: float,
    peak_vcores: int,
    cloud: str,
    min_memory_per_node_gb: float = 32.0,
) -> pd.DataFrame:
    """Recommend node types that fit the peak workload requirements.

    Returns top 3 options with estimated worker counts.
    """
    return execute_query(f"""
        SELECT
            node_type_id AS node_type,
            num_cores AS core_count,
            ROUND(memory_mb / 1024.0, 1) AS memory_gb,
            CEIL({peak_memory_gb} * 1024 / memory_mb) AS min_workers,
            CEIL({peak_vcores} / num_cores) AS min_workers_by_cpu,
            GREATEST(
                CEIL({peak_memory_gb} * 1024 / memory_mb),
                CEIL({peak_vcores} / num_cores)
            ) AS recommended_workers
        FROM system.compute.node_types
        WHERE cloud_type = '{cloud}'
          AND is_deprecated = false
          AND memory_mb >= {min_memory_per_node_gb * 1024}
        ORDER BY memory_mb ASC
        LIMIT 3
    """)


def get_peak_workload(catalog: str, schema: str) -> dict:
    """Get peak hourly resource usage from profiler data."""
    hourly_tbl = f"{catalog}.{schema}.hourly_yarn_view"

    df = execute_query(f"""
        SELECT
            ROUND(MAX(max_memory_mb) / 1024.0, 1) AS peak_memory_gb,
            MAX(max_cores) AS peak_vcores,
            ROUND(AVG(avg_memory_mb) / 1024.0, 1) AS avg_memory_gb,
            ROUND(AVG(avg_cores), 0) AS avg_vcores
        FROM {hourly_tbl}
    """)

    if df.empty:
        return {"peak_memory_gb": 0, "peak_vcores": 0,
                "avg_memory_gb": 0, "avg_vcores": 0}
    return df.iloc[0].to_dict()


def get_sizing_recommendations(
    catalog: str,
    schema: str,
    cloud: str,
) -> dict:
    """Full sizing recommendation: peak workload → node types → worker counts."""
    peak = get_peak_workload(catalog, schema)
    peak_mem = float(peak.get("peak_memory_gb", 0) or 0)
    peak_cpu = int(peak.get("peak_vcores", 0) or 0)

    if peak_mem == 0:
        return {"peak": peak, "recommendations": pd.DataFrame()}

    recs = recommend_node_type(peak_mem, peak_cpu, cloud)
    return {"peak": peak, "recommendations": recs}
