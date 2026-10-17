-- TCO Calculator — Seed Lookup Tables
-- Populates VM instances, DBSQL sizes, and storage tiers from spreadsheet baseline.
-- Safe to re-run: each INSERT is idempotent (tables are checked before seeding).

-- ============================================================
-- 1. VM Instance Pricing (seed values from spreadsheet)
-- ============================================================

INSERT INTO tco_lookup_vm_instances
(cloud, instance_type, vcpus, memory_gb, on_demand_price, reserved_price, spot_price, region, category, last_refreshed)
SELECT c.* FROM (
    SELECT 'AWS' AS cloud, 'm6id.2xlarge' AS instance_type, 8 AS vcpus, 32.0 AS memory_gb, 0.5016 AS on_demand_price, 0.3311 AS reserved_price, 0.3010 AS spot_price, 'us-east-1' AS region, 'worker' AS category, current_timestamp() AS last_refreshed
    UNION ALL SELECT 'AWS', 'm6id.4xlarge', 16, 64.0, 1.0032, 0.6621, 0.6019, 'us-east-1', 'worker', current_timestamp()
    UNION ALL SELECT 'AWS', 'm6id.8xlarge', 32, 128.0, 2.0064, 1.3242, 1.2038, 'us-east-1', 'worker', current_timestamp()
    UNION ALL SELECT 'AWS', 'm6id.xlarge', 4, 16.0, 0.2508, 0.1656, 0.1505, 'us-east-1', 'driver', current_timestamp()
    UNION ALL SELECT 'AZURE', 'Standard_E8ds_v4', 8, 64.0, 0.576, 0.3802, 0.3456, 'eastus', 'worker', current_timestamp()
    UNION ALL SELECT 'AZURE', 'Standard_E16ds_v4', 16, 128.0, 1.152, 0.7603, 0.6912, 'eastus', 'worker', current_timestamp()
    UNION ALL SELECT 'AZURE', 'Standard_E4ds_v4', 4, 32.0, 0.288, 0.1901, 0.1728, 'eastus', 'driver', current_timestamp()
    UNION ALL SELECT 'GCP', 'n2-highmem-8', 8, 64.0, 0.5266, 0.3476, 0.1580, 'us-central1', 'worker', current_timestamp()
    UNION ALL SELECT 'GCP', 'n2-highmem-16', 16, 128.0, 1.0532, 0.6951, 0.3160, 'us-central1', 'worker', current_timestamp()
    UNION ALL SELECT 'GCP', 'n2-highmem-4', 4, 32.0, 0.2633, 0.1738, 0.0790, 'us-central1', 'driver', current_timestamp()
) c;

-- ============================================================
-- 2. DBSQL Warehouse Sizes (from "Lookups- Databrick SQL" sheet)
-- ============================================================

INSERT INTO tco_lookup_dbsql_sizes
(size_name, worker_count, dbu_per_hour, vcpus, cloud, vm_cost_per_hour)
SELECT c.* FROM (
    -- AWS sizes
    SELECT '2X-Small' AS size_name, 1 AS worker_count, 2.0 AS dbu_per_hour, 8 AS vcpus, 'AWS' AS cloud, 0.50 AS vm_cost_per_hour
    UNION ALL SELECT 'X-Small', 2, 4.0, 16, 'AWS', 1.00
    UNION ALL SELECT 'Small', 4, 8.0, 32, 'AWS', 2.00
    UNION ALL SELECT 'Medium', 8, 16.0, 64, 'AWS', 4.01
    UNION ALL SELECT 'Large', 16, 32.0, 128, 'AWS', 8.02
    UNION ALL SELECT 'X-Large', 32, 64.0, 256, 'AWS', 16.04
    UNION ALL SELECT '2X-Large', 64, 128.0, 512, 'AWS', 32.08
    UNION ALL SELECT '3X-Large', 128, 256.0, 1024, 'AWS', 64.16
    UNION ALL SELECT '4X-Large', 256, 512.0, 2048, 'AWS', 128.32
    -- Azure sizes
    UNION ALL SELECT '2X-Small', 1, 2.0, 8, 'AZURE', 0.58
    UNION ALL SELECT 'X-Small', 2, 4.0, 16, 'AZURE', 1.15
    UNION ALL SELECT 'Small', 4, 8.0, 32, 'AZURE', 2.30
    UNION ALL SELECT 'Medium', 8, 16.0, 64, 'AZURE', 4.61
    UNION ALL SELECT 'Large', 16, 32.0, 128, 'AZURE', 9.22
    UNION ALL SELECT 'X-Large', 32, 64.0, 256, 'AZURE', 18.43
    UNION ALL SELECT '2X-Large', 64, 128.0, 512, 'AZURE', 36.86
    UNION ALL SELECT '3X-Large', 128, 256.0, 1024, 'AZURE', 73.73
    UNION ALL SELECT '4X-Large', 256, 512.0, 2048, 'AZURE', 147.46
    -- GCP sizes
    UNION ALL SELECT '2X-Small', 1, 2.0, 8, 'GCP', 0.53
    UNION ALL SELECT 'X-Small', 2, 4.0, 16, 'GCP', 1.05
    UNION ALL SELECT 'Small', 4, 8.0, 32, 'GCP', 2.11
    UNION ALL SELECT 'Medium', 8, 16.0, 64, 'GCP', 4.21
    UNION ALL SELECT 'Large', 16, 32.0, 128, 'GCP', 8.43
    UNION ALL SELECT 'X-Large', 32, 64.0, 256, 'GCP', 16.85
    UNION ALL SELECT '2X-Large', 64, 128.0, 512, 'GCP', 33.70
    UNION ALL SELECT '3X-Large', 128, 256.0, 1024, 'GCP', 67.41
    UNION ALL SELECT '4X-Large', 256, 512.0, 2048, 'GCP', 134.82
) c;

-- ============================================================
-- 3. Storage Tiers (from "Lookups- Other" sheet)
-- ============================================================

INSERT INTO tco_lookup_storage_tiers
(cloud, tier_name, volume_min_tb, volume_max_tb, price_per_gb)
SELECT c.* FROM (
    -- AWS S3 tiers
    SELECT 'AWS' AS cloud, 'hot' AS tier_name, 0.0 AS volume_min_tb, 50.0 AS volume_max_tb, 0.023 AS price_per_gb
    UNION ALL SELECT 'AWS', 'hot', 50.0, 500.0, 0.022
    UNION ALL SELECT 'AWS', 'hot', 500.0, 999999.0, 0.021
    UNION ALL SELECT 'AWS', 'cold', 0.0, 999999.0, 0.0125
    UNION ALL SELECT 'AWS', 'archive', 0.0, 999999.0, 0.004
    -- Azure Blob tiers
    UNION ALL SELECT 'AZURE', 'hot', 0.0, 50.0, 0.018
    UNION ALL SELECT 'AZURE', 'hot', 50.0, 500.0, 0.0173
    UNION ALL SELECT 'AZURE', 'hot', 500.0, 999999.0, 0.0166
    UNION ALL SELECT 'AZURE', 'cold', 0.0, 999999.0, 0.01
    UNION ALL SELECT 'AZURE', 'archive', 0.0, 999999.0, 0.002
    -- GCP GCS tiers
    UNION ALL SELECT 'GCP', 'hot', 0.0, 999999.0, 0.020
    UNION ALL SELECT 'GCP', 'cold', 0.0, 999999.0, 0.010
    UNION ALL SELECT 'GCP', 'archive', 0.0, 999999.0, 0.004
) c;
