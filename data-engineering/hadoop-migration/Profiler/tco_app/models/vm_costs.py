"""Cloud VM compute cost model.

Calculates worker + driver VM costs per workload stream (ETL, Interactive, BI/SQL).
Reads instance prices from tco_lookup_vm_instances (live-fetched or seed data).
"""

import logging
from utils.db_connector import execute_query, qualified_table
from models.constants import VM_DISCOUNTS, HOURS_PER_YEAR

logger = logging.getLogger(__name__)


def get_instance_price(instance_type: str, cloud: str, catalog: str, schema: str) -> dict:
    """Get VM instance pricing from the lookup table."""
    tbl = qualified_table("tco_lookup_vm_instances", catalog, schema)
    df = execute_query(f"""
        SELECT on_demand_price, reserved_price, spot_price, vcpus, memory_gb
        FROM {tbl}
        WHERE instance_type = '{instance_type}' AND cloud = '{cloud}'
        LIMIT 1
    """)
    if df.empty:
        logger.warning("No price found for %s on %s, using $0", instance_type, cloud)
        return {"on_demand_price": 0, "reserved_price": 0, "spot_price": 0,
                "vcpus": 0, "memory_gb": 0}
    return df.iloc[0].to_dict()


def calculate_vm_costs(
    assumptions: dict,
    stream_clusters: dict,
    catalog: str,
    schema: str,
) -> dict:
    """Calculate annual VM costs for ETL, Interactive, and BI/SQL streams.

    Args:
        assumptions: Full assumption dict.
        stream_clusters: Dict mapping stream name to {clusters, workers_per_cluster, hours_per_day}.
        catalog, schema: For DB lookups.

    Returns:
        Dict with per-stream VM costs and total.
    """
    cloud = assumptions.get("target_cloud", "AWS")
    worker_type = assumptions.get("worker_instance_type", "m6id.2xlarge")
    driver_type = assumptions.get("driver_instance_type", "m6id.xlarge")
    vm_discount_type = assumptions.get("vm_discount_type", "on_demand")

    worker_price = get_instance_price(worker_type, cloud, catalog, schema)
    driver_price = get_instance_price(driver_type, cloud, catalog, schema)

    discount = VM_DISCOUNTS.get(vm_discount_type, 0.0)

    price_key = {
        "on_demand": "on_demand_price",
        "reserved": "reserved_price",
        "spot": "spot_price",
    }.get(vm_discount_type, "on_demand_price")

    worker_rate = float(worker_price.get(price_key, 0) or 0)
    driver_rate = float(driver_price.get(price_key, 0) or 0)

    results = {}
    total_vm_cost = 0.0

    for stream, cfg in stream_clusters.items():
        clusters = cfg.get("clusters", 0)
        workers = cfg.get("workers_per_cluster", 0)
        hours_per_day = cfg.get("hours_per_day", 24)

        annual_hours = hours_per_day * 365
        worker_cost = clusters * workers * worker_rate * annual_hours
        driver_cost = clusters * 1 * driver_rate * annual_hours  # 1 driver per cluster
        stream_total = worker_cost + driver_cost

        results[stream] = {
            "clusters": clusters,
            "workers_per_cluster": workers,
            "hours_per_day": hours_per_day,
            "worker_cost": round(worker_cost, 2),
            "driver_cost": round(driver_cost, 2),
            "total": round(stream_total, 2),
        }
        total_vm_cost += stream_total

    return {
        "streams": results,
        "total": round(total_vm_cost, 2),
        "worker_instance": worker_type,
        "driver_instance": driver_type,
        "discount_type": vm_discount_type,
        "worker_rate": worker_rate,
        "driver_rate": driver_rate,
    }


def get_dbsql_vm_cost(warehouse_size: str, cloud: str, catalog: str, schema: str) -> dict:
    """Get DBSQL warehouse VM cost from the lookup table."""
    tbl = qualified_table("tco_lookup_dbsql_sizes", catalog, schema)
    df = execute_query(f"""
        SELECT size_name, worker_count, dbu_per_hour, vcpus, vm_cost_per_hour
        FROM {tbl}
        WHERE size_name = '{warehouse_size}' AND cloud = '{cloud}'
        LIMIT 1
    """)
    if df.empty:
        return {"worker_count": 0, "dbu_per_hour": 0, "vcpus": 0, "vm_cost_per_hour": 0}
    return df.iloc[0].to_dict()
