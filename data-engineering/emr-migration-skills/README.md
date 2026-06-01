# EMR to Databricks Migration Toolkit

Claude Code skills, helper scripts, and reference data for migrating Amazon EMR workloads to Databricks.

## Claude Code Skills

11 custom skills in `~/.claude/skills/` that guide the full migration lifecycle:

| Skill | Purpose |
|-------|---------|
| `/emr-migration-orchestrator` | Master entry point — routes to the right skill based on migration phase |
| `/emr-migration-assessment` | Analyze existing EMR clusters, jobs, costs, and dependencies |
| `/emr-spark-code-migration` | Convert PySpark/Scala Spark code from EMR to Databricks Runtime |
| `/emr-hive-to-unity-catalog` | Migrate Hive/Glue Data Catalog to Unity Catalog |
| `/emr-steps-to-workflows` | Convert EMR Steps and job flows to Databricks Workflows |
| `/emr-infra-migration` | Migrate infrastructure — instance types, S3, IAM, networking |
| `/emr-config-migration` | Convert Spark/YARN configs to Databricks cluster configs |
| `/emr-bootstrap-to-init-scripts` | Convert bootstrap actions to Databricks init scripts |
| `/emr-notebook-migration` | Convert Zeppelin/Jupyter/EMR Studio notebooks |
| `/emr-streaming-migration` | Migrate Spark Streaming workloads |
| `/emr-migration-validation` | Validate migration — data comparison, benchmarks, regression tests |

## Helper Scripts

| Script | Description |
|--------|-------------|
| `scripts/assess_emr_cluster.py` | Enumerate EMR clusters, configs, steps via boto3 → JSON report |
| `scripts/export_glue_catalog.py` | Export Glue databases/tables → Unity Catalog DDL |
| `scripts/map_instance_types.py` | EC2 instance → Databricks node type mapping with cost comparison |
| `scripts/convert_emr_steps.py` | EMR steps → Databricks workflow YAML (DABs format) |
| `scripts/spark_config_diff.py` | Compare EMR spark config vs Databricks Runtime defaults |
| `scripts/notebook_converter.py` | Zeppelin/Jupyter → Databricks notebook format |
| `scripts/validate_migration.py` | Data comparison queries between EMR and Databricks |

## Mapping Data

| File | Description |
|------|-------------|
| `mappings/instance_type_mapping.json` | EC2 instance types → Databricks node types + cost |
| `mappings/library_mapping.json` | EMR bundled library versions → DBR runtime equivalents |
| `mappings/spark_config_mapping.json` | Spark config key translation (keep/modify/remove/replace) |
| `mappings/emr_release_mapping.json` | EMR release → DBR runtime → Spark version matrix |

## Templates

| File | Description |
|------|-------------|
| `templates/databricks_workflow.yml` | Template Databricks workflow (DABs format) |
| `templates/cluster_policy.json` | Template cluster policy derived from EMR config |
| `templates/init_script_template.sh` | Template init script for common bootstrap patterns |

## Installation

### Prerequisites

- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) installed and authenticated
- Python 3.10+
- AWS CLI configured with appropriate credentials
- Databricks CLI configured with target workspace profile

### Install Skills

Clone the repo and install the skills into your Claude Code environment:

```bash
# Clone the repo
git clone https://github.com/kishore-mannava-db/poc-emr-claude.git
cd poc-emr-claude

# Install all skills into Claude Code
for skill_dir in skills/*/; do
  skill_name=$(basename "$skill_dir")
  cp -r "$skill_dir" ~/.claude/skills/"$skill_name"
done
```

### Install Python Dependencies

The helper scripts require `boto3` and `databricks-sdk`:

```bash
pip install boto3 databricks-sdk
```

### Verify Installation

```bash
# Check skills are available
ls ~/.claude/skills/emr-*

# Test a helper script
python scripts/assess_emr_cluster.py --help
```

## Usage

Start any migration engagement with:
```
/emr-migration-orchestrator
```

This will guide you through the 5-phase workflow: **Assess → Plan → Migrate → Validate → Cutover**.
