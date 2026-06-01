# YARN to Databricks Compute Model

## Overview

EMR runs Apache Spark on YARN (Yet Another Resource Negotiator), a cluster resource manager from Hadoop. Databricks does not use YARN. Instead, Databricks has its own resource management layer that handles executor allocation, memory management, and autoscaling directly. Understanding the differences is essential for migrating configurations and debugging performance issues.

## Architecture Comparison

### EMR with YARN

```
EMR Cluster
├── Master Node
│   ├── YARN ResourceManager
│   ├── Spark Driver (Application Master in cluster mode)
│   └── Hive Metastore (Glue or local)
├── Core Nodes
│   ├── YARN NodeManager
│   ├── Spark Executors (YARN containers)
│   └── HDFS DataNode
└── Task Nodes
    ├── YARN NodeManager
    └── Spark Executors (YARN containers)
```

### Databricks

```
Databricks Cluster
├── Driver Node
│   ├── Spark Driver
│   ├── Databricks Agent
│   └── Notebook/Job orchestration
└── Worker Nodes
    ├── Spark Executor(s)
    ├── Databricks Agent
    └── Local SSD (Delta cache)
```

**Key difference**: No YARN layer. Databricks manages executors directly. Each worker node typically runs one executor that uses all available resources on that node.

## Concept Mapping

### Resource Allocation

| YARN Concept | Databricks Equivalent | Notes |
|---|---|---|
| YARN Container | Worker node | Each worker is approximately one large container |
| Container vCores | Worker vCPUs | Entire node's CPUs available |
| Container memory | Worker memory | Entire node's memory available |
| NodeManager | Databricks Agent | Manages the worker process |
| ResourceManager | Databricks Cluster Manager | Allocates workers, handles autoscaling |
| Application Master | Spark Driver | Manages the Spark application |
| YARN queue | Cluster policy | Governance and resource limits |

### Scheduling and Queues

| YARN Feature | Databricks Equivalent | Notes |
|---|---|---|
| Capacity Scheduler | Cluster policies | Define allowed instance types, sizes, configurations |
| Fair Scheduler | Internal (no config) | Databricks schedules fairly across concurrent queries |
| YARN queue hierarchy | Not applicable | Each job gets its own cluster; no queue sharing |
| Queue capacity (%) | Not applicable | Cluster policies control max cluster size |
| Queue max-capacity | Cluster policy `max_workers` | Maximum cluster size |
| User limits per queue | Cluster policy permissions | Restrict who can use which policies |
| Queue preemption | Not applicable | Each cluster is isolated |

### Application Priority

| YARN Feature | Databricks Equivalent | Notes |
|---|---|---|
| Application priority | Job priority (Fair scheduling) | SQL Warehouse fair scheduling for concurrent queries |
| Queue priority | Not applicable | Each job gets dedicated resources |
| Preemption across apps | Not applicable | Jobs run on separate clusters |

### Resource Preemption to Autoscaling

YARN preempts containers to reallocate resources between applications. Databricks autoscaling adds/removes entire worker nodes.

```yaml
# YARN: Preemption and dynamic reallocation within fixed cluster
# yarn.scheduler.capacity.root.preemption=true
# yarn.scheduler.capacity.root.ordering-policy=fair

# Databricks: Autoscaling adds/removes workers
autoscale:
  min_workers: 2    # Always have at least 2 workers
  max_workers: 20   # Scale up to 20 when load increases
```

### Node Labels to Cluster Pools

YARN node labels partition a cluster so specific applications run on specific nodes. Databricks uses cluster pools for similar purposes.

```bash
# Create a cluster pool for GPU workloads
databricks cluster-pools create --json '{
  "instance_pool_name": "gpu-pool",
  "node_type_id": "p3.2xlarge",
  "min_idle_instances": 0,
  "max_capacity": 10,
  "idle_instance_autotermination_minutes": 15
}'
```

## Configuration Migration Guide

### YARN-site.xml Properties to Databricks

| YARN Property | Action | Databricks Equivalent |
|---|---|---|
| `yarn.nodemanager.resource.memory-mb` | Remove | Determined by node type |
| `yarn.nodemanager.resource.cpu-vcores` | Remove | Determined by node type |
| `yarn.scheduler.minimum-allocation-mb` | Remove | Entire node allocated |
| `yarn.scheduler.maximum-allocation-mb` | Remove | Entire node allocated |
| `yarn.scheduler.minimum-allocation-vcores` | Remove | Entire node allocated |
| `yarn.scheduler.maximum-allocation-vcores` | Remove | Entire node allocated |
| `yarn.nodemanager.vmem-check-enabled` | Remove | Not applicable |
| `yarn.nodemanager.pmem-check-enabled` | Remove | Not applicable |
| `yarn.nodemanager.vmem-pmem-ratio` | Remove | Not applicable |
| `yarn.nodemanager.local-dirs` | Remove | Databricks manages local storage |
| `yarn.nodemanager.log-dirs` | Remove | Databricks manages logging |
| `yarn.log-aggregation-enable` | Remove | Databricks manages log delivery |
| `yarn.resourcemanager.max-completed-applications` | Remove | Not applicable |

### Capacity Scheduler to Cluster Policies

**EMR Capacity Scheduler:**
```xml
<property>
  <name>yarn.scheduler.capacity.root.queues</name>
  <value>default,etl,interactive</value>
</property>
<property>
  <name>yarn.scheduler.capacity.root.etl.capacity</name>
  <value>60</value>
</property>
<property>
  <name>yarn.scheduler.capacity.root.interactive.capacity</name>
  <value>40</value>
</property>
<property>
  <name>yarn.scheduler.capacity.root.etl.maximum-capacity</name>
  <value>80</value>
</property>
```

**Databricks Cluster Policies:**
```json
{
  "name": "ETL Policy",
  "definition": {
    "node_type_id": {
      "type": "allowlist",
      "values": ["m5.2xlarge", "m5.4xlarge"]
    },
    "autoscale.min_workers": {
      "type": "range",
      "minValue": 2,
      "maxValue": 4
    },
    "autoscale.max_workers": {
      "type": "range",
      "minValue": 4,
      "maxValue": 20
    },
    "custom_tags.team": {
      "type": "fixed",
      "value": "etl"
    }
  }
}
```

```json
{
  "name": "Interactive Policy",
  "definition": {
    "node_type_id": {
      "type": "allowlist",
      "values": ["m5.xlarge", "m5.2xlarge"]
    },
    "autoscale.max_workers": {
      "type": "range",
      "minValue": 1,
      "maxValue": 10
    },
    "autotermination_minutes": {
      "type": "range",
      "minValue": 10,
      "maxValue": 120
    }
  }
}
```

## Debugging Differences

### YARN Container Failures to Databricks Worker Issues

| YARN Error | Databricks Equivalent | Investigation |
|---|---|---|
| Container killed by YARN for exceeding memory | Worker OOM (killed by OS) | Check Spark UI > Executors > Peak Memory; increase node size or tune `spark.executor.memory` |
| Container preempted | Not applicable (dedicated cluster) | N/A |
| NodeManager unhealthy | Worker lost | Check cluster event log; may be spot interruption |
| ApplicationMaster restart | Driver restart | Check driver logs; increase `spark.driver.memory` if OOM |
| Too many failed containers | Task failures | Check Spark UI > Stages > Failed Tasks |

### Monitoring Comparison

| YARN Monitoring | Databricks Monitoring |
|---|---|
| YARN ResourceManager UI (8088) | Databricks Cluster UI > Spark UI |
| YARN application logs | Driver logs and worker logs in Cluster UI |
| YARN node health | Cluster event log |
| YARN queue utilization | System tables: `system.compute.clusters` |
| YARN container metrics | Spark UI > Executors tab |

## Key Takeaway

The main mental model shift: **YARN shares a fixed cluster among multiple applications**. **Databricks gives each job its own cluster that scales independently**. This eliminates queue contention, preemption, and complex capacity planning. Instead, governance is handled through cluster policies and budgets.
