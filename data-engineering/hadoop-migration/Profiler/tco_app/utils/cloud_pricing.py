"""Live cloud VM pricing fetcher.

Fetches real-time VM instance pricing from cloud provider APIs.
Results stored in tco_lookup_vm_instances and audit-logged to tco_vm_price_history.

- Azure: Public Retail Prices API (no auth required)
- AWS: boto3 Pricing API (requires AWS credentials)
- GCP: Cloud Billing Catalog API (requires API key)
"""

import uuid
import hashlib
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Instance types we care about for TCO (keep in sync with seed_lookups.sql)
TARGET_INSTANCES = {
    "AWS": [
        "m6id.xlarge", "m6id.2xlarge", "m6id.4xlarge", "m6id.8xlarge",
    ],
    "AZURE": [
        "Standard_E4ds_v4", "Standard_E8ds_v4", "Standard_E16ds_v4",
    ],
    "GCP": [
        "n2-highmem-4", "n2-highmem-8", "n2-highmem-16",
    ],
}

DEFAULT_REGIONS = {
    "AWS": "us-east-1",
    "AZURE": "eastus",
    "GCP": "us-central1",
}


def fetch_azure_vm_prices(region: str = "eastus") -> list[dict]:
    """Fetch VM prices from Azure Retail Prices API (no auth required)."""
    import requests

    results = []
    base_url = "https://prices.azure.com/api/retail/prices"

    # Query per-SKU with server-side OData filter for fast, precise results
    for sku_name in TARGET_INSTANCES["AZURE"]:
        params = {
            "$filter": (
                f"serviceName eq 'Virtual Machines' "
                f"and armRegionName eq '{region}' "
                f"and priceType eq 'Consumption' "
                f"and armSkuName eq '{sku_name}'"
            ),
            "api-version": "2023-01-01-preview",
        }

        try:
            resp = requests.get(base_url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            for item in data.get("Items", []):
                if "Windows" in item.get("productName", ""):
                    continue
                meter = item.get("meterName", "").lower()
                # Classify price type from meter name
                if "spot" in meter:
                    price_type = "spot"
                elif "low priority" in meter:
                    continue  # skip low priority, use spot instead
                else:
                    price_type = "on_demand"

                results.append({
                    "cloud": "AZURE",
                    "region": region,
                    "instance_type": item.get("armSkuName", sku_name),
                    "price_type": price_type,
                    "price_per_hour": item.get("retailPrice", 0),
                    "currency": item.get("currencyCode", "USD"),
                })
        except Exception as e:
            logger.warning("Azure price fetch for %s failed: %s", sku_name, e)

    logger.info("Azure: fetched %d price records for %d SKUs", len(results), len(TARGET_INSTANCES["AZURE"]))
    return results


def fetch_aws_ec2_prices(region: str = "us-east-1") -> list[dict]:
    """Fetch EC2 prices from AWS Pricing API (requires boto3 + credentials)."""
    try:
        import boto3
    except ImportError:
        logger.warning("boto3 not installed — skipping AWS price fetch")
        return []

    results = []
    target_types = TARGET_INSTANCES["AWS"]

    try:
        # Pricing API only available in us-east-1
        client = boto3.client("pricing", region_name="us-east-1")
        paginator = client.get_paginator("get_products")

        for instance_type in target_types:
            pages = paginator.paginate(
                ServiceCode="AmazonEC2",
                Filters=[
                    {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                    {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                    {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                    {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
                    {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
                    {"Type": "TERM_MATCH", "Field": "location", "Value": _aws_region_name(region)},
                ],
                MaxResults=10,
            )

            for page in pages:
                for price_item in page["PriceList"]:
                    data = json.loads(price_item) if isinstance(price_item, str) else price_item
                    for term_type, terms in data.get("terms", {}).items():
                        for term in terms.values():
                            for dim in term.get("priceDimensions", {}).values():
                                price = float(dim.get("pricePerUnit", {}).get("USD", 0))
                                if price > 0:
                                    pt = "on_demand" if term_type == "OnDemand" else "reserved"
                                    results.append({
                                        "cloud": "AWS",
                                        "region": region,
                                        "instance_type": instance_type,
                                        "price_type": pt,
                                        "price_per_hour": price,
                                        "currency": "USD",
                                    })
    except Exception as e:
        logger.warning("AWS pricing fetch failed: %s", e)
        return []

    logger.info("AWS: fetched %d price records", len(results))
    return results


def fetch_gcp_vm_prices() -> list[dict]:
    """Fetch GCP VM prices from Cloud Billing Catalog API (requires API key)."""
    import os
    import requests

    api_key = os.getenv("GCP_BILLING_API_KEY", "")
    if not api_key:
        logger.warning("GCP_BILLING_API_KEY not set — skipping GCP price fetch")
        return []

    results = []
    compute_service_id = "6F81-5844-456A"
    url = f"https://cloudbilling.googleapis.com/v1/services/{compute_service_id}/skus"
    params = {"key": api_key}

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        skus = resp.json().get("skus", [])

        target_types = [t.lower() for t in TARGET_INSTANCES["GCP"]]
        for sku in skus:
            desc = sku.get("description", "").lower()
            if any(t.replace("-", "") in desc.replace("-", "") for t in target_types):
                for tier in sku.get("pricingInfo", []):
                    for rate in tier.get("pricingExpression", {}).get("tieredRates", []):
                        price = float(rate.get("unitPrice", {}).get("nanos", 0)) / 1e9
                        if price > 0:
                            results.append({
                                "cloud": "GCP",
                                "region": "us-central1",
                                "instance_type": _extract_gcp_instance(desc),
                                "price_type": "on_demand",
                                "price_per_hour": price,
                                "currency": "USD",
                            })
    except Exception as e:
        logger.warning("GCP pricing fetch failed: %s", e)
        return []

    logger.info("GCP: fetched %d price records", len(results))
    return results


def refresh_vm_prices(cloud: str, catalog: str, schema: str) -> dict:
    """Fetch live prices for a cloud, audit-log them, and update lookup table.

    Returns dict with fetch_id, record_count, and status.
    """
    from utils.db_connector import execute_statement, qualified_table

    region = DEFAULT_REGIONS.get(cloud, "us-east-1")

    # Step 1: Fetch from cloud API
    if cloud == "AZURE":
        prices = fetch_azure_vm_prices(region)
    elif cloud == "AWS":
        prices = fetch_aws_ec2_prices(region)
    elif cloud == "GCP":
        prices = fetch_gcp_vm_prices()
    else:
        return {"status": "error", "message": f"Unknown cloud: {cloud}"}

    if not prices:
        return {"status": "no_data", "message": f"No prices fetched for {cloud}. Using seed data."}

    # Step 2: Write audit log (immutable)
    fetch_id = str(uuid.uuid4())
    fetch_time = datetime.utcnow().isoformat()
    raw_hash = hashlib.sha256(json.dumps(prices, sort_keys=True).encode()).hexdigest()

    history_tbl = qualified_table("tco_vm_price_history", catalog, schema)
    for p in prices:
        execute_statement(f"""
            INSERT INTO {history_tbl}
            (fetch_id, fetch_time, cloud, region, instance_type, price_type,
             price_per_hour, currency, api_source, raw_response_hash)
            VALUES (
                '{fetch_id}', '{fetch_time}', '{p["cloud"]}', '{p["region"]}',
                '{p["instance_type"]}', '{p["price_type"]}',
                {p["price_per_hour"]}, '{p["currency"]}',
                '{_api_source(cloud)}', '{raw_hash}'
            )
        """)

    # Step 3: Upsert lookup table — group prices by instance_type
    lookup_tbl = qualified_table("tco_lookup_vm_instances", catalog, schema)

    # Build per-instance price map: {instance_type: {on_demand: X, spot: Y}}
    price_map = {}
    for p in prices:
        inst = p["instance_type"]
        if inst not in price_map:
            price_map[inst] = {"cloud": p["cloud"], "region": p["region"]}
        price_map[inst][p["price_type"]] = p["price_per_hour"]

    for inst, pm in price_map.items():
        od = pm.get("on_demand", 0)
        spot = pm.get("spot", od * 0.60)
        reserved = od * 0.66  # estimated 1yr reserved

        # Preserve vcpus/memory_gb/category from existing seed data
        execute_statement(f"""
            MERGE INTO {lookup_tbl} AS t
            USING (SELECT '{pm["cloud"]}' AS cloud, '{inst}' AS instance_type) AS s
            ON t.cloud = s.cloud AND t.instance_type = s.instance_type
            WHEN MATCHED THEN UPDATE SET
                on_demand_price = {od},
                reserved_price = {reserved},
                spot_price = {spot},
                region = '{pm["region"]}',
                last_refreshed = '{fetch_time}'
            WHEN NOT MATCHED THEN INSERT
                (cloud, instance_type, vcpus, memory_gb, on_demand_price,
                 reserved_price, spot_price, region, category, last_refreshed)
            VALUES ('{pm["cloud"]}', '{inst}', 0, 0.0, {od}, {reserved}, {spot},
                    '{pm["region"]}', 'worker', '{fetch_time}')
        """)

    logger.info("Refreshed %d VM instances for %s (fetch_id=%s)", len(price_map), cloud, fetch_id[:8])
    return {
        "status": "ok",
        "fetch_id": fetch_id,
        "record_count": len(prices),
        "updated_instances": len(price_map),
    }


def _aws_region_name(region_code: str) -> str:
    """Convert AWS region code to human-readable name for Pricing API."""
    mapping = {
        "us-east-1": "US East (N. Virginia)",
        "us-east-2": "US East (Ohio)",
        "us-west-1": "US West (N. California)",
        "us-west-2": "US West (Oregon)",
        "eu-west-1": "EU (Ireland)",
        "eu-central-1": "EU (Frankfurt)",
        "ap-southeast-1": "Asia Pacific (Singapore)",
    }
    return mapping.get(region_code, region_code)


def _extract_gcp_instance(description: str) -> str:
    """Extract GCP instance type name from SKU description."""
    for t in TARGET_INSTANCES["GCP"]:
        if t.replace("-", "").lower() in description.replace("-", "").lower():
            return t
    return "unknown"


def _api_source(cloud: str) -> str:
    """Return API source string for audit log."""
    return {"AWS": "aws_pricing", "AZURE": "azure_retail", "GCP": "gcp_billing"}.get(cloud, "unknown")
