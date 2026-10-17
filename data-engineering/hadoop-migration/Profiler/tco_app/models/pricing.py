"""Pricing snapshot logic.

Queries system.billing.list_prices for current prices and snapshots them
to tco_pricing_snapshot for reproducibility.
"""

import uuid
from datetime import datetime
import pandas as pd
from utils.db_connector import execute_query, execute_statement, qualified_table


def get_current_prices(cloud: str) -> pd.DataFrame:
    """Fetch current list prices from system.billing.list_prices for a cloud."""
    return execute_query(f"""
        SELECT
            sku_name,
            cloud,
            pricing.default AS list_price,
            pricing.effective_list.default AS effective_price,
            price_start_time,
            currency_code
        FROM system.billing.list_prices
        WHERE price_end_time IS NULL
          AND cloud = '{cloud}'
        ORDER BY sku_name
    """)


def create_pricing_snapshot(cloud: str, catalog: str, schema: str) -> str:
    """Snapshot current prices to tco_pricing_snapshot. Returns snapshot_id."""
    snapshot_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    tbl = qualified_table("tco_pricing_snapshot", catalog, schema)

    execute_statement(f"""
        INSERT INTO {tbl}
        (snapshot_id, snapshot_at, sku_name, cloud, list_price, effective_price,
         price_start_time, currency_code)
        SELECT
            '{snapshot_id}',
            '{now}',
            sku_name,
            cloud,
            pricing.default,
            pricing.effective_list.default,
            price_start_time,
            currency_code
        FROM system.billing.list_prices
        WHERE price_end_time IS NULL
          AND cloud = '{cloud}'
    """)
    return snapshot_id


def get_snapshot_prices(snapshot_id: str, catalog: str, schema: str) -> pd.DataFrame:
    """Retrieve prices from a specific snapshot."""
    tbl = qualified_table("tco_pricing_snapshot", catalog, schema)
    return execute_query(f"""
        SELECT sku_name, cloud, list_price, effective_price,
               price_start_time, currency_code
        FROM {tbl}
        WHERE snapshot_id = '{snapshot_id}'
        ORDER BY sku_name
    """)


def list_snapshots(catalog: str, schema: str) -> pd.DataFrame:
    """List all pricing snapshots."""
    tbl = qualified_table("tco_pricing_snapshot", catalog, schema)
    return execute_query(f"""
        SELECT snapshot_id, snapshot_at, cloud, COUNT(*) AS sku_count
        FROM {tbl}
        GROUP BY snapshot_id, snapshot_at, cloud
        ORDER BY snapshot_at DESC
    """)


def get_price_for_sku(snapshot_id: str, sku_name: str, cloud: str,
                      catalog: str, schema: str) -> dict:
    """Get a single SKU's price from a snapshot."""
    tbl = qualified_table("tco_pricing_snapshot", catalog, schema)
    df = execute_query(f"""
        SELECT list_price, effective_price
        FROM {tbl}
        WHERE snapshot_id = '{snapshot_id}'
          AND sku_name = '{sku_name}'
          AND cloud = '{cloud}'
        LIMIT 1
    """)
    if df.empty:
        return {"list_price": 0.0, "effective_price": 0.0}
    return df.iloc[0].to_dict()


def get_price_history(sku_name: str, cloud: str) -> pd.DataFrame:
    """Get historical prices for a SKU from system.billing.list_prices."""
    return execute_query(f"""
        SELECT
            sku_name,
            cloud,
            pricing.default AS list_price,
            pricing.effective_list.default AS effective_price,
            price_start_time,
            price_end_time
        FROM system.billing.list_prices
        WHERE sku_name = '{sku_name}'
          AND cloud = '{cloud}'
        ORDER BY price_start_time DESC
    """)
