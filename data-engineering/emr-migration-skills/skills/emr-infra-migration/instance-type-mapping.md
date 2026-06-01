# EMR Instance Type to Databricks Mapping

## Overview

All EC2 instance types available in EMR are also available in Databricks on AWS. The key differences are:
- **Photon-enabled instances**: Use Photon runtime for 2-8x acceleration on supported workloads (scan, aggregation, join-heavy)
- **Graviton instances**: ARM-based instances (e.g., m6g, r6g, c6g) are supported and often 20-30% cheaper
- **Spot instances**: Databricks supports spot with automatic fallback to on-demand

## Top 20 Instance Type Mapping

### General Purpose (M-series)

| EMR Instance | vCPU | Mem (GB) | On-Demand $/hr | Databricks Rec. | Notes |
|---|---|---|---|---|---|
| m5.xlarge | 4 | 16 | ~$0.192 | m5.xlarge | Direct match. Consider m6i.xlarge for 15% better perf. |
| m5.2xlarge | 8 | 32 | ~$0.384 | m5.2xlarge | Most common general-purpose worker. |
| m5.4xlarge | 16 | 64 | ~$0.768 | m5.4xlarge | Good for medium-large jobs. |
| m5.8xlarge | 32 | 128 | ~$1.536 | m5.8xlarge | Consider fewer, larger workers vs more smaller ones. |
| m5.12xlarge | 48 | 192 | ~$2.304 | m5.12xlarge | Very large — often better to use more m5.4xlarge. |
| m6i.xlarge | 4 | 16 | ~$0.192 | m6i.xlarge | Newer generation, better price/perf. |
| m6g.xlarge | 4 | 16 | ~$0.154 | m6g.xlarge | Graviton (ARM). ~20% cheaper. Verify library compat. |

### Memory Optimized (R-series)

| EMR Instance | vCPU | Mem (GB) | On-Demand $/hr | Databricks Rec. | Notes |
|---|---|---|---|---|---|
| r5.xlarge | 4 | 32 | ~$0.252 | r5.xlarge | Memory-heavy workloads (large joins, caching). |
| r5.2xlarge | 8 | 64 | ~$0.504 | r5.2xlarge | Common for memory-intensive Spark jobs. |
| r5.4xlarge | 16 | 128 | ~$1.008 | r5.4xlarge | Heavy caching, large broadcast joins. |
| r5.8xlarge | 32 | 256 | ~$2.016 | r5.8xlarge | Very memory-intensive; consider partitioning data instead. |
| r6i.xlarge | 4 | 32 | ~$0.252 | r6i.xlarge | Newer R-series, same price, better perf. |
| r6g.xlarge | 4 | 32 | ~$0.201 | r6g.xlarge | Graviton. ~20% cheaper. |

### Compute Optimized (C-series)

| EMR Instance | vCPU | Mem (GB) | On-Demand $/hr | Databricks Rec. | Notes |
|---|---|---|---|---|---|
| c5.2xlarge | 8 | 16 | ~$0.340 | c5.2xlarge | CPU-bound transforms, ML feature engineering. |
| c5.4xlarge | 16 | 32 | ~$0.680 | c5.4xlarge | Heavy computation, low memory needs. |
| c5.9xlarge | 36 | 72 | ~$1.530 | c5.9xlarge | Very CPU-heavy workloads. |
| c6i.2xlarge | 8 | 16 | ~$0.340 | c6i.2xlarge | Newer C-series. |

### Storage Optimized (I-series)

| EMR Instance | vCPU | Mem (GB) | On-Demand $/hr | Databricks Rec. | Notes |
|---|---|---|---|---|---|
| i3.xlarge | 4 | 30.5 | ~$0.312 | i3.xlarge | NVMe SSD — excellent for Delta cache. |
| i3.2xlarge | 8 | 61 | ~$0.624 | i3.2xlarge | Best for scan-heavy workloads with caching. |
| i3.4xlarge | 16 | 122 | ~$1.248 | i3.4xlarge | Large Delta cache footprint. |

## Cost Comparison Notes

### Databricks Pricing Model

Databricks cost = **EC2 cost** + **DBU cost**

- EC2 cost is the same as EMR (you pay AWS directly for VMs)
- DBU cost depends on the tier:
  - Jobs Compute: ~$0.15/DBU (batch workloads)
  - All-Purpose Compute: ~$0.40/DBU (interactive)
  - Serverless: ~$0.07/DBU (per-second billing, no idle cost)

### EMR Pricing Model

EMR cost = **EC2 cost** + **EMR surcharge** (~20-25% of EC2 cost)

### Comparison Example: m5.2xlarge cluster with 4 workers, 2-hour job

| Platform | EC2/VM Cost | Platform Cost | Total | Notes |
|---|---|---|---|---|
| EMR | $0.384 x 4 x 2 = $3.07 | ~$0.77 (EMR surcharge) | ~$3.84 | |
| Databricks Jobs | $0.384 x 4 x 2 = $3.07 | ~$1.20 (DBU cost) | ~$4.27 | With Photon, may complete in 1 hr |
| Databricks Serverless | N/A | ~$2.80 (all-inclusive DBU) | ~$2.80 | No idle time, instant startup |

## Spot Instance Considerations

### EMR Spot

- Instance fleets with allocation strategy (lowest-price, capacity-optimized)
- Task instance groups for spot, core for on-demand
- Spot interruption handling varies

### Databricks Spot

```yaml
aws_attributes:
  availability: "SPOT_WITH_FALLBACK"    # Spot with on-demand fallback
  # Options: SPOT, ON_DEMAND, SPOT_WITH_FALLBACK
  zone_id: "auto"                        # Let Databricks choose AZ
  spot_bid_price_percent: 100            # Bid up to on-demand price
  first_on_demand: 1                     # Keep driver on-demand
```

**Best practice**: Use `SPOT_WITH_FALLBACK` with `first_on_demand: 1` to keep the driver node on-demand while workers use spot.

## Graviton (ARM) Considerations

Graviton instances (m6g, r6g, c6g, m7g) offer ~20% cost savings and good performance. Considerations:

- **Python workloads**: Generally compatible — pure Python and PySpark work fine
- **JVM workloads**: Databricks Runtime includes ARM-compatible JVM
- **Native libraries**: Some C extensions or JNI libraries may need ARM builds
- **Photon**: Supports Graviton instances
- **Recommendation**: Test with Graviton; fall back to x86 if library compatibility issues arise

## Right-Sizing with Photon

Photon accelerates SQL and DataFrame operations. With Photon enabled, you often need fewer or smaller instances:

| Without Photon | With Photon | Typical Speedup |
|---|---|---|
| 8x m5.2xlarge (2 hours) | 4x m5.2xlarge (45 min) | 2-4x for scan/agg |
| 4x r5.4xlarge (3 hours) | 4x r5.2xlarge (1 hour) | Can downsize memory |
| 8x i3.2xlarge (1 hour) | 4x i3.2xlarge (30 min) | Fewer workers needed |

**When to use Photon:**
- SQL-heavy workloads (scans, filters, aggregations, joins)
- Parquet/Delta reads and writes
- ETL pipelines with DataFrame operations

**When Photon may not help:**
- UDF-heavy workloads (Python UDFs run outside Photon)
- ML training (use ML Runtime instead)
- Streaming micro-batches with very small data volumes
