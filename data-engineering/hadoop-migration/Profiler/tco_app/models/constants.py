"""TCO Calculator — Constants and lookup values.

Sourced from the Hadoop TCO spreadsheet's "Lookups- Other" sheet.
These are fallback defaults; the app prefers database lookup tables.
"""

# Performance gains by engine type (fraction of vCores saved)
PERFORMANCE_GAINS = {
    "classic": 0.30,
    "photon": 0.75,
    "serverless": 0.75,
}

# DBSQL utilization by warehouse type
DBSQL_UTILIZATION = {
    "classic": 0.70,
    "pro": 0.70,
    "serverless": 1.00,
}

# VM pricing discount by purchase type (fraction off on-demand)
VM_DISCOUNTS = {
    "on_demand": 0.00,
    "reserved": 0.34,
    "spot": 0.40,
}

# Migration services T-shirt sizing ($)
MIGRATION_TSHIRT = {
    "small": 500_000,
    "medium": 850_000,
    "large": 1_750_000,
    "custom": 0,
}

# Default assumption values for new assumption sets
DEFAULT_ASSUMPTIONS = {
    "hadoop_vendor_type": "Licensed",
    "hadoop_node_count": 100,
    "hadoop_vcores_per_node": 32,
    "hadoop_utilization_pct": 30.0,
    "hadoop_license_per_node": 11_200.0,
    "hadoop_license_discount": 0.0,
    "hadoop_support_pct": 25.0,
    "hadoop_hardware_per_node": 1_000.0,
    "hadoop_datacenter_per_node": 5_000.0,
    "hadoop_admin_count": 6,
    "hadoop_admin_salary": 180_000.0,
    "dev_test_uplift": 0.20,
    "hyperthreading_factor": 2.0,
    "photon_perf_gain": 0.75,
    "etl_pct": 40.0,
    "interactive_pct": 30.0,
    "bisql_pct": 30.0,
    "vm_discount_type": "on_demand",
    "worker_instance_type": "m6id.2xlarge",
    "driver_instance_type": "m6id.xlarge",
    "dbsql_warehouse_size": "Medium",
    "dbsql_type": "pro",
    "dbsql_utilization": 0.70,
    "storage_discount_pct": 0.0,
    "hot_storage_pct": 70.0,
    "cold_storage_pct": 20.0,
    "archive_storage_pct": 10.0,
    "dbx_support_pct": 25.0,
    "dbx_admin_overhead_pct": 30.0,
    "migration_tshirt": "medium",
    "migration_custom_cost": 0.0,
    "ecif_credit": 0.0,
    "migration_duration_quarters": 8,
}

# DBU rates by SKU category + engine from spreadsheet
# Format: (jobs_classic, jobs_photon, all_purpose_classic, all_purpose_photon)
DBU_RATES = {
    "AWS": {"jobs_classic": 0.15, "jobs_photon": 0.20, "all_purpose_classic": 0.40, "all_purpose_photon": 0.55},
    "AZURE": {"jobs_classic": 0.15, "jobs_photon": 0.20, "all_purpose_classic": 0.40, "all_purpose_photon": 0.55},
    "GCP": {"jobs_classic": 0.15, "jobs_photon": 0.20, "all_purpose_classic": 0.40, "all_purpose_photon": 0.55},
}

# Hours per year for annualization
HOURS_PER_YEAR = 8_760
HOURS_PER_DAY = 24
QUARTERS_PER_YEAR = 4

# Instance catalog: Databricks-supported instance types with specs
# Format: {cloud: [(instance_type, vcpus, memory_gb), ...]}
INSTANCE_CATALOG = {
    "AWS": [
        # M6i — General purpose (Intel)
        ("m6i.xlarge", 4, 16), ("m6i.2xlarge", 8, 32), ("m6i.4xlarge", 16, 64),
        ("m6i.8xlarge", 32, 128), ("m6i.12xlarge", 48, 192), ("m6i.16xlarge", 64, 256),
        # M6id — General purpose + local NVMe
        ("m6id.xlarge", 4, 16), ("m6id.2xlarge", 8, 32), ("m6id.4xlarge", 16, 64),
        ("m6id.8xlarge", 32, 128), ("m6id.12xlarge", 48, 192), ("m6id.16xlarge", 64, 256),
        # M5d — General purpose + local NVMe (prev gen)
        ("m5d.xlarge", 4, 16), ("m5d.2xlarge", 8, 32), ("m5d.4xlarge", 16, 64),
        ("m5d.8xlarge", 32, 128), ("m5d.12xlarge", 48, 192),
        # R6i — Memory optimized (Intel)
        ("r6i.xlarge", 4, 32), ("r6i.2xlarge", 8, 64), ("r6i.4xlarge", 16, 128),
        ("r6i.8xlarge", 32, 256), ("r6i.12xlarge", 48, 384),
        # R6id — Memory optimized + local NVMe
        ("r6id.xlarge", 4, 32), ("r6id.2xlarge", 8, 64), ("r6id.4xlarge", 16, 128),
        ("r6id.8xlarge", 32, 256), ("r6id.12xlarge", 48, 384),
        # R5d — Memory optimized + local NVMe (prev gen)
        ("r5d.xlarge", 4, 32), ("r5d.2xlarge", 8, 64), ("r5d.4xlarge", 16, 128),
        ("r5d.8xlarge", 32, 256), ("r5d.12xlarge", 48, 384),
        # C6i — Compute optimized (Intel)
        ("c6i.xlarge", 4, 8), ("c6i.2xlarge", 8, 16), ("c6i.4xlarge", 16, 32),
        ("c6i.8xlarge", 32, 64), ("c6i.12xlarge", 48, 96),
        # C6id — Compute optimized + local NVMe
        ("c6id.xlarge", 4, 8), ("c6id.2xlarge", 8, 16), ("c6id.4xlarge", 16, 32),
        ("c6id.8xlarge", 32, 64), ("c6id.12xlarge", 48, 96),
        # I3 — Storage optimized
        ("i3.xlarge", 4, 30.5), ("i3.2xlarge", 8, 61), ("i3.4xlarge", 16, 122),
        ("i3.8xlarge", 32, 244), ("i3.16xlarge", 64, 488),
        # I4i — Storage optimized (current gen)
        ("i4i.xlarge", 4, 32), ("i4i.2xlarge", 8, 64), ("i4i.4xlarge", 16, 128),
        ("i4i.8xlarge", 32, 256), ("i4i.16xlarge", 64, 512),
        # G5 — GPU (ML workloads)
        ("g5.xlarge", 4, 16), ("g5.2xlarge", 8, 32), ("g5.4xlarge", 16, 64),
        ("g5.8xlarge", 32, 128), ("g5.16xlarge", 64, 256),
        # P3 — GPU (training)
        ("p3.2xlarge", 8, 61), ("p3.8xlarge", 32, 244), ("p3.16xlarge", 64, 488),
    ],
    "AZURE": [
        # Edsv4 — Memory optimized + local disk
        ("Standard_E4ds_v4", 4, 32), ("Standard_E8ds_v4", 8, 64),
        ("Standard_E16ds_v4", 16, 128), ("Standard_E32ds_v4", 32, 256),
        ("Standard_E48ds_v4", 48, 384), ("Standard_E64ds_v4", 64, 504),
        # Edsv5 — Memory optimized + local disk (current gen)
        ("Standard_E4ds_v5", 4, 32), ("Standard_E8ds_v5", 8, 64),
        ("Standard_E16ds_v5", 16, 128), ("Standard_E32ds_v5", 32, 256),
        ("Standard_E48ds_v5", 48, 384), ("Standard_E64ds_v5", 64, 512),
        # Ddsv4 — General purpose + local disk
        ("Standard_D4ds_v4", 4, 16), ("Standard_D8ds_v4", 8, 32),
        ("Standard_D16ds_v4", 16, 64), ("Standard_D32ds_v4", 32, 128),
        ("Standard_D48ds_v4", 48, 192), ("Standard_D64ds_v4", 64, 256),
        # Ddsv5 — General purpose + local disk (current gen)
        ("Standard_D4ds_v5", 4, 16), ("Standard_D8ds_v5", 8, 32),
        ("Standard_D16ds_v5", 16, 64), ("Standard_D32ds_v5", 32, 128),
        ("Standard_D48ds_v5", 48, 192), ("Standard_D64ds_v5", 64, 256),
        # Fdsv2 — Compute optimized
        ("Standard_F4s_v2", 4, 8), ("Standard_F8s_v2", 8, 16),
        ("Standard_F16s_v2", 16, 32), ("Standard_F32s_v2", 32, 64),
        ("Standard_F48s_v2", 48, 96), ("Standard_F64s_v2", 64, 128),
        # Lsv2 — Storage optimized
        ("Standard_L8s_v2", 8, 64), ("Standard_L16s_v2", 16, 128),
        ("Standard_L32s_v2", 32, 256), ("Standard_L48s_v2", 48, 384),
        # NCv3 — GPU (training)
        ("Standard_NC6s_v3", 6, 112), ("Standard_NC12s_v3", 12, 224),
        ("Standard_NC24s_v3", 24, 448),
    ],
    "GCP": [
        # N2-standard — General purpose
        ("n2-standard-4", 4, 16), ("n2-standard-8", 8, 32),
        ("n2-standard-16", 16, 64), ("n2-standard-32", 32, 128),
        ("n2-standard-48", 48, 192), ("n2-standard-64", 64, 256),
        # N2-highmem — Memory optimized
        ("n2-highmem-4", 4, 32), ("n2-highmem-8", 8, 64),
        ("n2-highmem-16", 16, 128), ("n2-highmem-32", 32, 256),
        ("n2-highmem-48", 48, 384), ("n2-highmem-64", 64, 512),
        # N2-highcpu — Compute optimized
        ("n2-highcpu-4", 4, 4), ("n2-highcpu-8", 8, 8),
        ("n2-highcpu-16", 16, 16), ("n2-highcpu-32", 32, 32),
        ("n2-highcpu-48", 48, 48), ("n2-highcpu-64", 64, 64),
        # N2d-standard — AMD general purpose
        ("n2d-standard-4", 4, 16), ("n2d-standard-8", 8, 32),
        ("n2d-standard-16", 16, 64), ("n2d-standard-32", 32, 128),
        # N2d-highmem — AMD memory optimized
        ("n2d-highmem-4", 4, 32), ("n2d-highmem-8", 8, 64),
        ("n2d-highmem-16", 16, 128), ("n2d-highmem-32", 32, 256),
        # A2 — GPU (training)
        ("a2-highgpu-1g", 12, 85), ("a2-highgpu-2g", 24, 170),
        ("a2-highgpu-4g", 48, 340),
    ],
}

def get_instance_specs(instance_type: str, cloud: str) -> dict:
    """Look up vCPUs and memory for an instance type. Returns dict with vcpus, memory_gb."""
    for name, vcpus, mem_gb in INSTANCE_CATALOG.get(cloud, []):
        if name == instance_type:
            return {"vcpus": vcpus, "memory_gb": mem_gb}
    return {"vcpus": 8, "memory_gb": 64}  # fallback
