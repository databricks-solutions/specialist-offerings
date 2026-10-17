"""Storage cost model.

Estimates Delta Lake storage cost from HDFS metrics captured by the profiler.
Accounts for replication factor savings (HDFS 3x → Delta 1x) and compression.
Supports both flat-rate and tiered storage pricing.
"""

import logging
import pandas as pd
from utils.db_connector import execute_query, qualified_table

logger = logging.getLogger(__name__)


def get_hdfs_capacity(catalog: str, schema: str) -> dict:
    """Extract HDFS capacity and usage from CM metrics.

    Looks in cm_hdfs_usage for capacity/used metrics.
    Falls back to cm_timeseries if the dedicated table doesn't exist.
    """
    hdfs_tbl = f"{catalog}.{schema}.cm_hdfs_usage"

    try:
        df = execute_query(f"""
            SELECT
                MAX(CASE WHEN metric_name LIKE '%capacity%'
                         AND metric_name NOT LIKE '%used%'
                    THEN value END) / 1e9 AS hdfs_capacity_gb,
                MAX(CASE WHEN metric_name LIKE '%capacity_used%'
                    THEN value END) / 1e9 AS hdfs_used_gb
            FROM {hdfs_tbl}
        """)
    except Exception:
        # Fallback: try cm_timeseries with HDFS metrics
        ts_tbl = f"{catalog}.{schema}.cm_timeseries"
        df = execute_query(f"""
            SELECT
                MAX(CASE WHEN metric_name LIKE '%dfs_capacity%'
                         AND metric_name NOT LIKE '%used%'
                    THEN mean_value END) / 1e9 AS hdfs_capacity_gb,
                MAX(CASE WHEN metric_name LIKE '%dfs_capacity_used%'
                    THEN mean_value END) / 1e9 AS hdfs_used_gb
            FROM {ts_tbl}
        """)

    if df.empty or pd.isna(df.iloc[0]["hdfs_capacity_gb"]):
        return {"hdfs_capacity_gb": 0, "hdfs_used_gb": 0}

    return {
        "hdfs_capacity_gb": round(float(df.iloc[0]["hdfs_capacity_gb"] or 0), 2),
        "hdfs_used_gb": round(float(df.iloc[0]["hdfs_used_gb"] or 0), 2),
    }


def estimate_storage_cost(
    catalog: str,
    schema: str,
    hdfs_repl_factor: int = 3,
    delta_compression: float = 0.5,
    cost_per_gb_month: float = 0.023,
) -> dict:
    """Estimate annual Delta Lake storage cost from HDFS metrics.

    Calculation:
    1. HDFS used GB / replication factor = logical data size
    2. Logical data × compression ratio = Delta storage size
    3. Delta storage × monthly rate × 12 = annual cost

    Returns dict with all intermediate values for transparency.
    """
    hdfs = get_hdfs_capacity(catalog, schema)
    hdfs_used = hdfs["hdfs_used_gb"]
    hdfs_capacity = hdfs["hdfs_capacity_gb"]

    if hdfs_used == 0:
        return {
            "hdfs_capacity_gb": hdfs_capacity,
            "hdfs_used_gb": 0,
            "logical_data_gb": 0,
            "delta_storage_gb": 0,
            "monthly_cost": 0,
            "annual_cost": 0,
            "repl_savings_gb": 0,
            "compression_savings_gb": 0,
        }

    logical_data_gb = hdfs_used / hdfs_repl_factor
    delta_storage_gb = logical_data_gb * delta_compression
    monthly_cost = delta_storage_gb * cost_per_gb_month
    annual_cost = monthly_cost * 12

    repl_savings_gb = hdfs_used - logical_data_gb
    compression_savings_gb = logical_data_gb - delta_storage_gb

    return {
        "hdfs_capacity_gb": round(hdfs_capacity, 2),
        "hdfs_used_gb": round(hdfs_used, 2),
        "logical_data_gb": round(logical_data_gb, 2),
        "delta_storage_gb": round(delta_storage_gb, 2),
        "monthly_cost": round(monthly_cost, 2),
        "annual_cost": round(annual_cost, 2),
        "repl_savings_gb": round(repl_savings_gb, 2),
        "compression_savings_gb": round(compression_savings_gb, 2),
    }


def _get_tier_price(cloud: str, tier: str, volume_tb: float,
                    catalog: str, schema: str) -> float:
    """Look up per-GB price from tco_lookup_storage_tiers for a volume."""
    tbl = qualified_table("tco_lookup_storage_tiers", catalog, schema)
    try:
        df = execute_query(f"""
            SELECT price_per_gb
            FROM {tbl}
            WHERE cloud = '{cloud}' AND tier_name = '{tier}'
              AND volume_min_tb <= {volume_tb} AND volume_max_tb > {volume_tb}
            LIMIT 1
        """)
        if not df.empty:
            return float(df.iloc[0]["price_per_gb"])
    except Exception:
        pass
    # Fallback to defaults
    defaults = {"hot": 0.023, "cold": 0.0125, "archive": 0.004}
    return defaults.get(tier, 0.023)


def estimate_tiered_storage_cost(
    catalog: str,
    schema: str,
    cloud: str,
    hdfs_repl_factor: int = 3,
    delta_compression: float = 0.5,
    hot_pct: float = 70.0,
    cold_pct: float = 20.0,
    archive_pct: float = 10.0,
    storage_discount_pct: float = 0.0,
) -> dict:
    """Estimate annual Delta storage cost with tiered pricing.

    Splits logical data into hot/cold/archive tiers and applies
    per-tier pricing from the lookup table.
    """
    hdfs = get_hdfs_capacity(catalog, schema)
    hdfs_used = hdfs["hdfs_used_gb"]
    hdfs_capacity = hdfs["hdfs_capacity_gb"]

    if hdfs_used == 0:
        return {
            "hdfs_capacity_gb": hdfs_capacity,
            "hdfs_used_gb": 0,
            "logical_data_gb": 0,
            "delta_storage_gb": 0,
            "tiers": {},
            "monthly_cost": 0,
            "annual_cost": 0,
        }

    logical_data_gb = hdfs_used / hdfs_repl_factor
    delta_storage_gb = logical_data_gb * delta_compression
    volume_tb = delta_storage_gb / 1024

    discount = storage_discount_pct / 100

    tiers = {}
    total_monthly = 0.0
    for tier, pct in [("hot", hot_pct), ("cold", cold_pct), ("archive", archive_pct)]:
        tier_gb = delta_storage_gb * (pct / 100)
        price = _get_tier_price(cloud, tier, volume_tb, catalog, schema)
        monthly = tier_gb * price * (1 - discount)
        tiers[tier] = {
            "gb": round(tier_gb, 2),
            "pct": pct,
            "price_per_gb": price,
            "monthly_cost": round(monthly, 2),
        }
        total_monthly += monthly

    return {
        "hdfs_capacity_gb": round(hdfs_capacity, 2),
        "hdfs_used_gb": round(hdfs_used, 2),
        "logical_data_gb": round(logical_data_gb, 2),
        "delta_storage_gb": round(delta_storage_gb, 2),
        "tiers": tiers,
        "monthly_cost": round(total_monthly, 2),
        "annual_cost": round(total_monthly * 12, 2),
        "repl_savings_gb": round(hdfs_used - logical_data_gb, 2),
        "compression_savings_gb": round(logical_data_gb - delta_storage_gb, 2),
    }
