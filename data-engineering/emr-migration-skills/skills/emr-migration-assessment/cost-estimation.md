# Cost Estimation -- EMR to Databricks

A framework for comparing current EMR costs against projected Databricks costs, enabling informed migration decisions.

## EMR Cost Components

EMR costs break down into five categories:

| Component | Description | How to Extract |
|-----------|-------------|----------------|
| **EC2 Instance Hours** | Compute cost for master, core, and task nodes | Cost Explorer: `UsageType` contains `BoxUsage` |
| **EMR Surcharge** | AWS markup for managed EMR service (typically 25% of EC2) | Cost Explorer: `UsageType` contains `ElasticMapReduce` |
| **EBS Volumes** | Storage attached to cluster nodes | Cost Explorer: `UsageType` contains `EBS` |
| **S3 Storage** | Data lake storage (shared with other services) | Cost Explorer with S3 filter, allocate by prefix |
| **Data Transfer** | Cross-AZ, cross-region, and internet egress | Cost Explorer: `UsageType` contains `DataTransfer` |

### Extracting EMR Costs from Cost Explorer

```python
import boto3
from datetime import datetime, timedelta

ce = boto3.client("ce", region_name="us-east-1")

def get_emr_cost_breakdown(months_back=3):
    """Get detailed EMR cost breakdown by usage type."""
    end = datetime.utcnow().strftime("%Y-%m-01")
    start_dt = datetime.utcnow() - timedelta(days=months_back * 30)
    start = start_dt.strftime("%Y-%m-01")

    response = ce.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="MONTHLY",
        Metrics=["BlendedCost", "UnblendedCost", "UsageQuantity"],
        Filter={
            "Dimensions": {
                "Key": "SERVICE",
                "Values": ["Amazon Elastic MapReduce"],
            }
        },
        GroupBy=[
            {"Type": "DIMENSION", "Key": "USAGE_TYPE"},
        ],
    )
    return response["ResultsByTime"]


def get_emr_cost_by_cluster_tag(tag_key="Name", months_back=3):
    """Get EMR costs grouped by cluster tag to identify per-cluster spend."""
    end = datetime.utcnow().strftime("%Y-%m-01")
    start_dt = datetime.utcnow() - timedelta(days=months_back * 30)
    start = start_dt.strftime("%Y-%m-01")

    response = ce.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="MONTHLY",
        Metrics=["BlendedCost"],
        Filter={
            "Dimensions": {
                "Key": "SERVICE",
                "Values": ["Amazon Elastic MapReduce"],
            }
        },
        GroupBy=[
            {"Type": "TAG", "Key": tag_key},
        ],
    )
    return response["ResultsByTime"]
```

### Extracting Associated EC2 Costs

EMR EC2 costs are billed under the EC2 service. To capture them:

```python
def get_emr_ec2_costs(months_back=3):
    """Get EC2 costs for EMR instances using resource tags."""
    end = datetime.utcnow().strftime("%Y-%m-01")
    start_dt = datetime.utcnow() - timedelta(days=months_back * 30)
    start = start_dt.strftime("%Y-%m-01")

    response = ce.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="MONTHLY",
        Metrics=["BlendedCost"],
        Filter={
            "And": [
                {
                    "Dimensions": {
                        "Key": "SERVICE",
                        "Values": ["Amazon Elastic Compute Cloud - Compute"],
                    }
                },
                {
                    "Tags": {
                        "Key": "aws:elasticmapreduce:job-flow-id",
                        "MatchOptions": ["STARTS_WITH"],
                        "Values": ["j-"],
                    }
                },
            ]
        },
        GroupBy=[{"Type": "DIMENSION", "Key": "INSTANCE_TYPE"}],
    )
    return response["ResultsByTime"]
```

## Databricks Cost Components

| Component | Description | Pricing Model |
|-----------|-------------|---------------|
| **DBU Consumption** | Databricks Units based on workload type and instance | Per-DBU rate varies by tier (Jobs, All-Purpose, SQL) |
| **Cloud Infrastructure** | EC2 instances (pass-through from AWS) | Standard AWS EC2 pricing |
| **Storage** | S3 / DBFS (same data lake, no migration needed) | Standard S3 pricing |
| **Data Transfer** | Same as AWS data transfer costs | Standard AWS rates |

## Instance Type to DBU Mapping

Databricks DBU rates depend on the instance type and workload tier. Below is the mapping formula:

```
DBUs per hour = instance_dbu_rate * number_of_instances * hours_running
Databricks cost = DBUs * per_dbu_price
Cloud cost = EC2 on-demand price * number_of_instances * hours_running
Total Databricks cost = Databricks cost + Cloud cost
```

### Common Instance DBU Rates (Jobs Compute, AWS)

| Instance Type | vCPUs | Memory (GB) | DBU/hr (Jobs) | DBU/hr (All-Purpose) |
|--------------|-------|-------------|---------------|---------------------|
| m5.xlarge | 4 | 16 | 1.0 | 1.5 |
| m5.2xlarge | 8 | 32 | 2.0 | 3.0 |
| m5.4xlarge | 16 | 64 | 4.0 | 6.0 |
| r5.xlarge | 4 | 32 | 1.0 | 1.5 |
| r5.2xlarge | 8 | 64 | 2.0 | 3.0 |
| c5.xlarge | 4 | 8 | 0.75 | 1.0 |
| c5.2xlarge | 8 | 16 | 1.5 | 2.0 |
| i3.xlarge | 4 | 30.5 | 1.0 | 1.5 |
| i3.2xlarge | 8 | 61 | 2.0 | 3.0 |

> **Note:** Actual DBU rates should be confirmed against the latest Databricks pricing page or your contract terms.

### DBU Price Tiers (approximate, list price)

| Tier | Per-DBU Price (USD) |
|------|-------------------|
| Jobs Compute | $0.15 |
| Jobs Compute Lite (Serverless) | $0.07 |
| All-Purpose Compute | $0.40 |
| Delta Live Tables (Core) | $0.20 |
| Delta Live Tables (Pro) | $0.25 |
| Delta Live Tables (Advanced) | $0.36 |
| SQL (Classic) | $0.22 |
| SQL (Pro) | $0.55 |
| SQL (Serverless) | $0.70 |

## Spot vs On-Demand Considerations

### EMR Spot Usage
- EMR core nodes on Spot: risk of data loss if terminated (HDFS replication helps)
- EMR task nodes on Spot: safe, no data stored locally
- Typical discount: 60-90% off on-demand

### Databricks Spot Usage
- Databricks supports Spot instances for worker nodes
- Driver node should be on-demand for reliability
- Spot fallback to on-demand is configurable
- Typical discount: similar 60-90% off on-demand

### Mapping Spot Strategy

```python
def map_spot_strategy(emr_instance_groups):
    """
    Map EMR Spot/On-Demand mix to recommended Databricks configuration.
    """
    recommendations = []
    for ig in emr_instance_groups:
        rec = {
            "emr_group": ig["name"],
            "emr_type": ig["instance_group_type"],
            "emr_market": ig["market"],
            "instance_type": ig["instance_type"],
            "count": ig["requested_count"],
        }
        if ig["instance_group_type"] == "MASTER":
            rec["databricks_recommendation"] = "Driver node: ON_DEMAND"
        elif ig["market"] == "SPOT":
            rec["databricks_recommendation"] = (
                f"Worker: SPOT with on-demand fallback, "
                f"spot_bid_max_price = -1 (auto)"
            )
        else:
            rec["databricks_recommendation"] = "Worker: ON_DEMAND"
        recommendations.append(rec)
    return recommendations
```

## Reserved Capacity / Committed Use

### EMR (AWS)
- **Reserved Instances**: 1-year or 3-year EC2 RIs apply to EMR nodes
- **Savings Plans**: Compute Savings Plans cover EMR EC2 usage
- Check for existing RI/SP coverage that would transfer to Databricks EC2 usage

### Databricks
- **Committed Use Discounts (CUD)**: Pre-purchase DBUs at discounted rates
- **Enterprise Agreement**: Custom pricing for large commitments
- Typical discount: 20-40% for 1-year commit, 40-60% for 3-year

```python
def check_reserved_capacity():
    """Check for existing AWS Reserved Instances and Savings Plans."""
    ec2 = boto3.client("ec2", region_name="us-east-1")

    # Reserved Instances
    ris = ec2.describe_reserved_instances(
        Filters=[{"Name": "state", "Values": ["active"]}]
    )

    # Savings Plans
    sp = boto3.client("savingsplans")
    plans = sp.describe_savings_plans(
        states=["active"]
    )

    return {
        "reserved_instances": [
            {
                "instance_type": ri["InstanceType"],
                "count": ri["InstanceCount"],
                "end": str(ri["End"]),
                "offering_type": ri["OfferingType"],
            }
            for ri in ris["ReservedInstances"]
        ],
        "savings_plans": [
            {
                "type": sp_item["SavingsPlanType"],
                "commitment": sp_item["Commitment"],
                "end": sp_item["End"],
            }
            for sp_item in plans["SavingsPlans"]
        ],
    }
```

## TCO Comparison Template

Use this table to compare total cost of ownership:

| Cost Category | EMR (Monthly) | Databricks (Monthly) | Savings | Notes |
|--------------|---------------|---------------------|---------|-------|
| Compute (EC2) | $ | $ | | Instance types may differ |
| EMR Surcharge / DBU Cost | $ | $ | | EMR ~25% of EC2; DBU varies |
| EBS Storage | $ | $ | | Databricks uses root vol only |
| S3 Storage | $ | $ | | Same across platforms |
| Data Transfer | $ | $ | | Same across platforms |
| Spot Savings | -$ | -$ | | Both support Spot |
| RI/SP/CUD Discounts | -$ | -$ | | May need new commitments |
| **Total** | **$** | **$** | **$** | |

## Worked Example

**Scenario**: 5x m5.xlarge EMR cluster running 8 hours/day, 22 days/month

### EMR Cost

```
EC2 cost:
  Master (1x m5.xlarge): 1 * $0.192/hr * 8hr * 22d = $33.79
  Core   (4x m5.xlarge): 4 * $0.192/hr * 8hr * 22d = $135.17
  Subtotal EC2: $168.96

EMR surcharge (~25%):
  $168.96 * 0.25 = $42.24

EBS (50GB gp3 per node @ $0.08/GB):
  5 * 50 * $0.08 = $20.00

Total EMR monthly: $231.20
```

### Databricks Cost (Jobs Compute)

```
DBU consumption:
  Driver (1x m5.xlarge): 1.0 DBU/hr * 8hr * 22d = 176 DBU
  Workers (4x m5.xlarge): 4 * 1.0 DBU/hr * 8hr * 22d = 704 DBU
  Total DBUs: 880
  DBU cost: 880 * $0.15 = $132.00

Cloud infrastructure (EC2, same instances):
  5 * $0.192/hr * 8hr * 22d = $168.96

EBS (root volume only, 100GB gp3 per node):
  5 * 100 * $0.08 = $40.00

Total Databricks monthly: $340.96
```

### Comparison

| | EMR | Databricks | Delta |
|---|---|---|---|
| Compute + Platform | $211.20 | $300.96 | +$89.76 |
| Storage | $20.00 | $40.00 | +$20.00 |
| **Total** | **$231.20** | **$340.96** | **+$109.76** |

> **Important caveats:**
> - This is list-price comparison only. Databricks CUD and enterprise discounts can reduce the DBU cost by 20-60%.
> - Databricks Photon engine and Delta Lake optimizations often reduce job runtime by 2-5x, meaning fewer compute hours.
> - Serverless Jobs Compute at $0.07/DBU can be significantly cheaper for bursty workloads.
> - The real savings often come from reduced operational overhead, faster development, and fewer failed jobs.
> - Always benchmark actual workloads on both platforms before finalizing cost projections.

## Cost Estimation Script

```python
def estimate_databricks_cost(emr_clusters, dbu_rate=0.15):
    """
    Given a list of EMR cluster inventories, estimate equivalent Databricks cost.
    """
    DBU_RATES = {
        "m5.xlarge": 1.0, "m5.2xlarge": 2.0, "m5.4xlarge": 4.0,
        "r5.xlarge": 1.0, "r5.2xlarge": 2.0, "r5.4xlarge": 4.0,
        "c5.xlarge": 0.75, "c5.2xlarge": 1.5, "c5.4xlarge": 3.0,
        "i3.xlarge": 1.0, "i3.2xlarge": 2.0,
    }

    estimates = []
    for cluster in emr_clusters:
        total_dbus = 0
        for ig in cluster.get("instance_groups", []):
            itype = ig["instance_type"]
            count = ig.get("running_count") or ig.get("requested_count", 0)
            dbu_per_instance = DBU_RATES.get(itype, 1.0)  # default to 1.0
            total_dbus += dbu_per_instance * count

        # Estimate hours from normalized instance hours or uptime
        hours = cluster.get("estimated_daily_hours", 24)
        days = cluster.get("days_per_month", 30)
        monthly_dbus = total_dbus * hours * days
        monthly_dbu_cost = monthly_dbus * dbu_rate

        estimates.append({
            "cluster_name": cluster.get("name"),
            "monthly_dbus": monthly_dbus,
            "monthly_dbu_cost": monthly_dbu_cost,
        })

    return estimates
```
