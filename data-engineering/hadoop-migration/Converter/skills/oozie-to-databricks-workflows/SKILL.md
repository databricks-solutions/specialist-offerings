---
name: oozie-to-databricks-workflows
description: "Convert Oozie workflows and coordinators to Databricks Workflows. Triggers on: convert Oozie, migrate Oozie workflow, Oozie to Databricks, workflow.xml to Databricks Jobs, Oozie coordinator migration"
version: 1.0.0
---

# Oozie to Databricks Workflows Converter

Convert Apache Oozie workflow and coordinator definitions to Databricks Workflows (Jobs API).

## When to Use

- Converting Oozie workflow.xml files to Databricks multi-task Jobs
- Migrating Oozie coordinators to Databricks Job schedules/triggers
- Converting fork/join parallelism to Databricks parallel tasks
- Mapping Oozie action types to Databricks task types
- Converting Oozie decision nodes to `run_if` conditions

## Instructions

When given Oozie XML to convert:

1. **Read references** for detailed rules:
   - `references/DAG_MAPPING.md` — DAG structure mapping (fork/join, decision)
   - `references/ACTION_TYPE_MAPPING.md` — Each action type → Databricks task type
   - `references/COORDINATOR_MIGRATION.md` — Coordinators → triggers
   - `references/EXAMPLES.md` — Full before/after examples

2. **Parse workflow.xml** and:
   a. Identify the DAG structure (start → actions → fork/join → end)
   b. Map each `<action>` to a Databricks task type
   c. Convert `<fork>`/`<join>` to parallel task dependencies
   d. Convert `<decision>` nodes to `run_if` conditions
   e. Map error handling (`<error to>`) to task failure behavior

3. **For coordinators**:
   a. Convert frequency to cron expression
   b. Map dataset dependencies to file-arrival triggers or cron
   c. Convert parameterization (EL expressions → job parameters)

4. **Output** Databricks Job JSON definition + migration notes
