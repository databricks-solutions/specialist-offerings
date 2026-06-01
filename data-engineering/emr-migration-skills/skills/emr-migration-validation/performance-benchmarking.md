# Performance Benchmarking Methodology

## Step 1: Establish EMR Baseline

Record execution metrics for each workload on EMR before migration.

### Metrics to Capture

| Metric | How to Capture on EMR | How to Capture on Databricks |
|---|---|---|
| Wall clock time | EMR Step duration (console/API) | Job run duration (Workflows UI) |
| CPU utilization | CloudWatch metrics | Ganglia / Spark UI / Cluster metrics |
| Peak memory | CloudWatch / YARN ResourceManager | Spark UI > Executors tab |
| Shuffle data | Spark UI > Stages > Shuffle Read/Write | Spark UI > Stages |
| I/O bytes read | Spark UI > SQL > scan metrics | Spark UI > SQL > scan metrics |
| I/O bytes written | Spark UI > SQL > write metrics | Spark UI > SQL > write metrics |
| Number of tasks | Spark UI > Jobs | Spark UI > Jobs |
| GC time | Spark UI > Executors > GC Time | Spark UI > Executors > GC Time |

### Baseline Template

```markdown
## Workload: [Name]
- **EMR Cluster**: [Cluster ID] ([Instance types] x [count])
- **EMR Version**: emr-[version]
- **Spark Version**: [version]
- **Input Size**: [size in GB]
- **Output Size**: [size in GB]

### EMR Metrics (average of 3 runs)
| Metric | Run 1 | Run 2 | Run 3 | Average |
|---|---|---|---|---|
| Duration (min) | | | | |
| CPU Utilization (%) | | | | |
| Peak Memory (GB) | | | | |
| Shuffle Read (GB) | | | | |
| Shuffle Write (GB) | | | | |
| Input Bytes (GB) | | | | |
| Output Bytes (GB) | | | | |
| Total Tasks | | | | |
| GC Time (sec) | | | | |
```

## Step 2: Run on Databricks with Equivalent Cluster

Configure a Databricks cluster that approximates the EMR cluster:

### Cluster Sizing Equivalence

| EMR Instance | Databricks Equivalent | Notes |
|---|---|---|
| m5.xlarge (4 vCPU, 16 GB) | Standard_D4s_v3 (Azure) / m5.xlarge (AWS) | Similar specs |
| m5.2xlarge (8 vCPU, 32 GB) | Standard_D8s_v3 / m5.2xlarge | Similar specs |
| r5.xlarge (4 vCPU, 32 GB) | Standard_E4s_v3 / r5.xlarge | Memory-optimized |
| r5.2xlarge (8 vCPU, 64 GB) | Standard_E8s_v3 / r5.2xlarge | Memory-optimized |
| i3.xlarge (4 vCPU, 30.5 GB) | Standard_L4s / i3.xlarge | Storage-optimized |

**Important**: For a fair comparison, set min/max workers to the same fixed count as EMR (disable autoscaling initially).

## Step 3: Compare Metrics

### Comparison Template

```markdown
## Workload: [Name]

| Metric | EMR (avg) | Databricks (avg) | Ratio (DBX/EMR) | Status |
|---|---|---|---|---|
| Duration (min) | | | | OK if < 2.0x |
| CPU Utilization (%) | | | | INFO |
| Peak Memory (GB) | | | | INFO |
| Shuffle Read (GB) | | | | OK if similar |
| Shuffle Write (GB) | | | | OK if similar |
| Input Bytes (GB) | | | | OK if similar |
| Output Bytes (GB) | | | | OK if similar |
| Total Tasks | | | | INFO |
| GC Time (sec) | | | | OK if < 2.0x |
```

## Step 4: Cost-Normalize

Compare the cost per run on both platforms.

```python
def calculate_cost_comparison(
    emr_instance_type, emr_instance_count, emr_duration_hours,
    dbx_instance_type, dbx_instance_count, dbx_duration_hours,
    emr_price_per_hour, dbx_price_per_dbu, dbx_dbu_per_hour
):
    """Calculate cost comparison between EMR and Databricks."""
    
    # EMR cost = EC2 cost + EMR surcharge
    emr_cost = emr_instance_count * emr_duration_hours * emr_price_per_hour
    
    # Databricks cost = VM cost + DBU cost
    dbx_vm_cost = dbx_instance_count * dbx_duration_hours * emr_price_per_hour  # Same VM pricing
    dbx_dbu_cost = dbx_instance_count * dbx_duration_hours * dbx_dbu_per_hour * dbx_price_per_dbu
    dbx_total_cost = dbx_vm_cost + dbx_dbu_cost
    
    print(f"EMR Cost: ${emr_cost:.2f}")
    print(f"Databricks Cost: ${dbx_total_cost:.2f} (VM: ${dbx_vm_cost:.2f} + DBU: ${dbx_dbu_cost:.2f})")
    print(f"Cost Ratio: {dbx_total_cost/emr_cost:.2f}x")
    
    return {
        "emr_cost": emr_cost,
        "dbx_cost": dbx_total_cost,
        "ratio": dbx_total_cost / emr_cost
    }
```

## Step 5: Optimization Opportunities on Databricks

If Databricks is slower or more expensive, try these optimizations before concluding:

### 1. Enable Photon

Photon is a vectorized query engine that accelerates Spark SQL and DataFrame operations. Enable it via cluster configuration.

```json
{
  "runtime_engine": "PHOTON"
}
```

**Expected improvement**: 2-8x for scan-heavy, aggregation-heavy, and join-heavy workloads.

### 2. Use Delta Caching

Enable local SSD caching for frequently read Delta tables.

```python
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.databricks.io.cache.maxDiskUsage", "50g")
spark.conf.set("spark.databricks.io.cache.maxMetaDataCache", "1g")
```

### 3. Tune Autoscaling

For variable workloads, enable autoscaling to reduce cost during low-activity periods.

```json
{
  "autoscale": {
    "min_workers": 2,
    "max_workers": 10
  }
}
```

### 4. Use Serverless for Intermittent Workloads

For jobs that run infrequently, serverless compute eliminates idle cluster costs.

### 5. Optimize File Layout

```sql
-- Z-order for common filter columns
OPTIMIZE catalog.schema.table ZORDER BY (date, customer_id);

-- Enable auto-optimization
ALTER TABLE catalog.schema.table SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

### 6. Predictive Optimization

Enable Unity Catalog predictive optimization for automatic OPTIMIZE and VACUUM.

```sql
ALTER TABLE catalog.schema.table SET TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true'
);
```

## Tips for Fair Comparison

1. **Same data**: Use identical input datasets for both platforms
2. **Same cluster size**: Disable autoscaling and match worker count
3. **Warm cache**: Run each workload 3 times, discard the first (cold) run
4. **Same Spark version**: Match Spark versions as closely as possible
5. **Measure total time**: Include data read, processing, and write time
6. **Account for Delta overhead**: If converting Parquet to Delta, the first comparison may show Delta overhead (but subsequent reads will be faster)
7. **Normalize for DBR optimizations**: Databricks Runtime includes optimizations not in open-source Spark -- this is a feature, not an unfair advantage
