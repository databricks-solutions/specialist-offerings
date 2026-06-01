# Dependency Mapping -- Discovering EMR Job Dependencies

Before migrating EMR workloads, you must understand the full dependency graph: which jobs depend on which, what data flows between them, and what external systems are involved. Migrating a job in isolation can break upstream/downstream processes.

## Job Chain Analysis

### Step Functions State Machines

Many EMR pipelines are orchestrated by AWS Step Functions:

```python
import boto3
import json
import re

sfn = boto3.client("stepfunctions", region_name="us-east-1")

def find_emr_step_functions():
    """Find all Step Functions state machines that reference EMR."""
    emr_state_machines = []
    paginator = sfn.get_paginator("list_state_machines")

    for page in paginator.paginate():
        for sm in page["stateMachines"]:
            definition = sfn.describe_state_machine(
                stateMachineArn=sm["stateMachineArn"]
            )
            defn_str = definition["definition"]

            # Check if the state machine references EMR
            if any(keyword in defn_str for keyword in [
                "elasticmapreduce", "EmrAddStep", "EmrCreateCluster",
                "emr", "RunJobFlow", "AddJobFlowSteps"
            ]):
                defn_json = json.loads(defn_str)
                emr_state_machines.append({
                    "name": sm["name"],
                    "arn": sm["stateMachineArn"],
                    "definition": defn_json,
                    "states_with_emr": _extract_emr_states(defn_json),
                })
    return emr_state_machines


def _extract_emr_states(definition):
    """Extract states that interact with EMR from a state machine definition."""
    emr_states = []
    states = definition.get("States", {})
    for state_name, state_def in states.items():
        resource = state_def.get("Resource", "")
        if "elasticmapreduce" in resource.lower() or "emr" in resource.lower():
            emr_states.append({
                "state_name": state_name,
                "type": state_def.get("Type"),
                "resource": resource,
                "parameters": state_def.get("Parameters", {}),
                "next": state_def.get("Next"),
            })
    return emr_states
```

### Airflow DAGs

If using MWAA (Managed Workflows for Apache Airflow) or self-hosted Airflow:

```python
import os

def scan_airflow_dags_for_emr(dag_directory):
    """
    Scan Airflow DAG files to find EMR-related operators and their dependencies.
    Works for local DAG directories or S3-downloaded DAG folders.
    """
    emr_dags = []

    for root, dirs, files in os.walk(dag_directory):
        for filename in files:
            if not filename.endswith(".py"):
                continue
            filepath = os.path.join(root, filename)
            with open(filepath, "r") as f:
                content = f.read()

            # Check for EMR operator imports
            emr_indicators = [
                "EmrAddStepsOperator",
                "EmrCreateJobFlowOperator",
                "EmrTerminateJobFlowOperator",
                "EmrStepSensor",
                "EmrJobFlowSensor",
                "emr_conn_id",
                "EmrServerlessStartJobOperator",
            ]

            if any(indicator in content for indicator in emr_indicators):
                # Extract DAG ID
                dag_id_match = re.search(r'dag_id\s*=\s*["\']([^"\']+)["\']', content)
                dag_id = dag_id_match.group(1) if dag_id_match else filename

                # Extract task dependencies (>> operator)
                dependencies = re.findall(
                    r'(\w+)\s*>>\s*(\w+)', content
                )

                # Extract schedule
                schedule_match = re.search(
                    r'schedule_interval\s*=\s*["\']([^"\']+)["\']', content
                )
                schedule = schedule_match.group(1) if schedule_match else None

                emr_dags.append({
                    "dag_id": dag_id,
                    "file": filepath,
                    "schedule": schedule,
                    "task_dependencies": dependencies,
                    "emr_operators_found": [
                        ind for ind in emr_indicators if ind in content
                    ],
                })

    return emr_dags
```

### Cron Schedules (EC2 or On-Cluster)

```python
def extract_cron_jobs_from_emr(cluster_id, key_file, master_dns):
    """
    SSH into EMR master node to extract crontab entries.
    Requires SSH key and security group allowing port 22.
    """
    # For automated extraction, use SSM Run Command for better security:
    ssm = boto3.client("ssm", region_name="us-east-1")
    response = ssm.send_command(
        InstanceIds=[get_master_instance_id(cluster_id)],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": ["crontab -l", "cat /etc/crontab"]},
    )
    return response


def get_master_instance_id(cluster_id):
    """Get the EC2 instance ID of the EMR master node."""
    emr = boto3.client("emr", region_name="us-east-1")
    instances = emr.list_instances(
        ClusterId=cluster_id,
        InstanceGroupTypes=["MASTER"],
    )
    if instances["Instances"]:
        return instances["Instances"][0]["Ec2InstanceId"]
    return None
```

### EventBridge (CloudWatch Events) Rules

```python
def find_emr_eventbridge_rules():
    """Find EventBridge rules that trigger or are triggered by EMR events."""
    events = boto3.client("events", region_name="us-east-1")
    emr_rules = []

    paginator = events.get_paginator("list_rules")
    for page in paginator.paginate():
        for rule in page["Rules"]:
            pattern = rule.get("EventPattern", "")
            if "elasticmapreduce" in pattern.lower() or "emr" in pattern.lower():
                targets = events.list_targets_by_rule(Rule=rule["Name"])
                emr_rules.append({
                    "name": rule["Name"],
                    "schedule": rule.get("ScheduleExpression"),
                    "event_pattern": json.loads(pattern) if pattern else None,
                    "state": rule["State"],
                    "targets": targets.get("Targets", []),
                })
    return emr_rules
```

## Data Lineage -- S3 Path Analysis

The most reliable way to trace data flow is by analyzing S3 paths used as inputs and outputs across jobs.

```python
def extract_s3_paths_from_steps(cluster_id):
    """
    Parse EMR step arguments to find S3 input/output paths.
    This builds a map of which steps produce and consume which S3 locations.
    """
    emr = boto3.client("emr", region_name="us-east-1")
    paginator = emr.get_paginator("list_steps")
    step_io = []

    for page in paginator.paginate(ClusterId=cluster_id):
        for step in page["Steps"]:
            args = step["Config"].get("Args", [])
            args_str = " ".join(args)

            # Extract S3 paths
            s3_paths = re.findall(r's3[a-z]*://[^\s,\'"]+', args_str)

            # Heuristic: classify as input vs output
            inputs = []
            outputs = []
            for i, arg in enumerate(args):
                if re.match(r's3[a-z]*://', arg):
                    # Check preceding argument for hints
                    prev = args[i - 1] if i > 0 else ""
                    if prev in ("--input", "--src", "-i", "--source"):
                        inputs.append(arg)
                    elif prev in ("--output", "--dst", "-o", "--destination", "--target"):
                        outputs.append(arg)
                    else:
                        # Default: last S3 path is usually output
                        inputs.append(arg)

            # For spark-submit, the last S3 path is often the output
            if len(s3_paths) >= 2 and not outputs:
                outputs = [s3_paths[-1]]
                inputs = s3_paths[:-1]

            step_io.append({
                "step_name": step["Name"],
                "step_id": step["Id"],
                "s3_paths": s3_paths,
                "probable_inputs": inputs,
                "probable_outputs": outputs,
            })

    return step_io


def build_data_lineage_graph(all_step_io):
    """
    Build a lineage graph where edges represent data flow between steps.
    Two steps are connected if one writes to an S3 path the other reads from.
    """
    lineage = []
    output_map = {}  # s3_path -> step that writes it

    for step in all_step_io:
        for path in step["probable_outputs"]:
            # Normalize path (remove trailing slashes, partition suffixes)
            normalized = path.rstrip("/").split("/partition=")[0]
            output_map[normalized] = step["step_name"]

    for step in all_step_io:
        for path in step["probable_inputs"]:
            normalized = path.rstrip("/").split("/partition=")[0]
            if normalized in output_map:
                producer = output_map[normalized]
                if producer != step["step_name"]:
                    lineage.append({
                        "producer": producer,
                        "consumer": step["step_name"],
                        "s3_path": normalized,
                    })

    return lineage
```

## Glue Catalog Dependencies

```python
def map_table_dependencies(glue_catalog, step_io_by_cluster):
    """
    Cross-reference Glue table locations with S3 paths used by EMR steps
    to determine which jobs read/write which catalog tables.
    """
    glue = boto3.client("glue", region_name="us-east-1")
    table_location_map = {}  # s3_location -> (database, table)

    for db_name, tables in glue_catalog.items():
        for table in tables:
            location = table.get("location", "")
            if location:
                normalized = location.rstrip("/")
                table_location_map[normalized] = (db_name, table["name"])

    dependencies = []
    for cluster_id, steps in step_io_by_cluster.items():
        for step in steps:
            for path in step.get("s3_paths", []):
                normalized = path.rstrip("/")
                # Check if path matches or is a subdirectory of a table location
                for table_loc, (db, tbl) in table_location_map.items():
                    if normalized.startswith(table_loc) or table_loc.startswith(normalized):
                        dependencies.append({
                            "cluster_id": cluster_id,
                            "step_name": step["step_name"],
                            "database": db,
                            "table": tbl,
                            "s3_path": path,
                            "access_type": (
                                "WRITE" if path in step.get("probable_outputs", [])
                                else "READ"
                            ),
                        })

    return dependencies
```

## External System Integrations

EMR jobs often connect to external databases, APIs, and message queues. These must be identified before migration.

```python
def scan_for_external_connections(cluster_id):
    """
    Analyze EMR step arguments and bootstrap scripts for external system references.
    """
    emr_client = boto3.client("emr", region_name="us-east-1")
    external_systems = []

    # Patterns that indicate external system connections
    patterns = {
        "jdbc": re.compile(r'jdbc:[a-z]+://[^\s"\']+'),
        "mongo": re.compile(r'mongodb(\+srv)?://[^\s"\']+'),
        "redis": re.compile(r'redis://[^\s"\']+'),
        "kafka": re.compile(r'(bootstrap\.servers|kafka\.broker)[=:]\s*[^\s"\']+'),
        "kinesis": re.compile(r'kinesis://[^\s"\']+|streamName[=:]\s*[^\s"\']+'),
        "dynamodb": re.compile(r'dynamodb://[^\s"\']+|tableName[=:]\s*[^\s"\']+'),
        "sqs": re.compile(r'sqs://[^\s"\']+|queueUrl[=:]\s*[^\s"\']+'),
        "api_endpoint": re.compile(r'https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/api/'),
    }

    # Scan step arguments
    paginator = emr_client.get_paginator("list_steps")
    for page in paginator.paginate(ClusterId=cluster_id):
        for step in page["Steps"]:
            args_str = " ".join(step["Config"].get("Args", []))
            for system_type, pattern in patterns.items():
                matches = pattern.findall(args_str)
                if matches:
                    external_systems.append({
                        "step_name": step["Name"],
                        "system_type": system_type,
                        "references": matches,
                    })

    # Scan bootstrap actions
    bootstraps = emr_client.list_bootstrap_actions(ClusterId=cluster_id)
    for action in bootstraps.get("BootstrapActions", []):
        args_str = " ".join(action.get("Args", []))
        for system_type, pattern in patterns.items():
            matches = pattern.findall(args_str)
            if matches:
                external_systems.append({
                    "source": f"bootstrap:{action['Name']}",
                    "system_type": system_type,
                    "references": matches,
                })

    return external_systems
```

## Generating a Dependency Graph

### Mermaid Diagram

```python
def generate_mermaid_diagram(lineage, table_deps, external_systems):
    """Generate a Mermaid flowchart from the dependency data."""
    lines = ["graph LR"]

    # Data lineage edges (step-to-step via S3)
    for edge in lineage:
        producer = edge["producer"].replace(" ", "_")
        consumer = edge["consumer"].replace(" ", "_")
        lines.append(f"    {producer} -->|S3| {consumer}")

    # Table dependencies
    for dep in table_deps:
        step = dep["step_name"].replace(" ", "_")
        table = f"{dep['database']}__{dep['table']}"
        if dep["access_type"] == "WRITE":
            lines.append(f"    {step} -->|writes| {table}[({dep['table']})]")
        else:
            lines.append(f"    {table}[({dep['table']})] -->|reads| {step}")

    # External systems
    for ext in external_systems:
        step = ext.get("step_name", ext.get("source", "unknown")).replace(" ", "_")
        system = ext["system_type"]
        lines.append(f"    {step} <-->|{system}| ext_{system}{{{system}}}")

    return "\n".join(lines)
```

### Graphviz DOT Format

```python
def generate_dot_graph(lineage, table_deps, external_systems):
    """Generate a Graphviz DOT file for rendering with dot/neato."""
    lines = [
        "digraph emr_dependencies {",
        '    rankdir=LR;',
        '    node [shape=box, style=filled, fillcolor="#E8F0FE"];',
    ]

    # Steps as boxes
    all_steps = set()
    for edge in lineage:
        all_steps.add(edge["producer"])
        all_steps.add(edge["consumer"])
    for step in all_steps:
        safe = step.replace(" ", "_").replace("-", "_")
        lines.append(f'    {safe} [label="{step}"];')

    # Tables as cylinders
    lines.append('    node [shape=cylinder, fillcolor="#FFF3E0"];')
    tables = set()
    for dep in table_deps:
        table_id = f"{dep['database']}__{dep['table']}"
        if table_id not in tables:
            tables.add(table_id)
            lines.append(f'    {table_id} [label="{dep["database"]}.{dep["table"]}"];')

    # External systems as diamonds
    lines.append('    node [shape=diamond, fillcolor="#FCE4EC"];')
    ext_ids = set()
    for ext in external_systems:
        ext_id = f"ext_{ext['system_type']}"
        if ext_id not in ext_ids:
            ext_ids.add(ext_id)
            lines.append(f'    {ext_id} [label="{ext["system_type"]}"];')

    # Edges
    for edge in lineage:
        p = edge["producer"].replace(" ", "_").replace("-", "_")
        c = edge["consumer"].replace(" ", "_").replace("-", "_")
        lines.append(f'    {p} -> {c} [label="S3"];')

    for dep in table_deps:
        step = dep["step_name"].replace(" ", "_").replace("-", "_")
        table_id = f"{dep['database']}__{dep['table']}"
        if dep["access_type"] == "WRITE":
            lines.append(f'    {step} -> {table_id};')
        else:
            lines.append(f'    {table_id} -> {step};')

    for ext in external_systems:
        step = ext.get("step_name", ext.get("source", "unknown"))
        step = step.replace(" ", "_").replace("-", "_")
        ext_id = f"ext_{ext['system_type']}"
        lines.append(f'    {step} -> {ext_id} [dir=both];')

    lines.append("}")
    return "\n".join(lines)
```

## Dependency Documentation Template

Use this template to document dependencies for each EMR workload:

```markdown
## Workload: [Name]

### Cluster
- Cluster ID: j-XXXXXXXXXXXXX
- Schedule: Daily at 02:00 UTC / On-demand / Always-on

### Upstream Dependencies
| Source | Type | Description |
|--------|------|-------------|
| s3://bucket/raw/events/ | S3 (data) | Raw event data landing zone |
| glue_db.customers | Glue Table | Customer dimension table |
| RDS postgres://prod-db/orders | JDBC | Order transaction data |

### Downstream Dependents
| Target | Type | Description |
|--------|------|-------------|
| s3://bucket/curated/daily_summary/ | S3 (data) | Aggregated daily metrics |
| glue_db.daily_summary | Glue Table | Registered in catalog |
| Redshift analytics.dashboard_metrics | JDBC | Dashboard source table |

### Orchestration
- **Orchestrator**: Step Functions / Airflow / Cron
- **State Machine/DAG**: arn:aws:states:... / daily_etl_dag
- **Trigger**: EventBridge rule / S3 event / Schedule
- **SLA**: Must complete by 06:00 UTC

### External Systems
| System | Protocol | Purpose |
|--------|----------|---------|
| PostgreSQL (RDS) | JDBC | Read order data |
| Kafka (MSK) | kafka:// | Publish completion events |
| REST API (internal) | HTTPS | Enrich with metadata |

### Migration Notes
- [ ] JDBC connections need Databricks secret scope setup
- [ ] Kafka producer must be reconfigured for Databricks cluster IPs
- [ ] Downstream Redshift load must be updated to read from new location
```
