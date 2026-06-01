---
name: emr-migration-orchestrator
description: "Master orchestrator for EMR to Databricks migration. Use when: (1) 'migrate from EMR', (2) 'EMR to Databricks', (3) 'migration plan', (4) 'migration assessment', (5) ANY task mentioning EMR and Databricks migration together. ALWAYS invoke this skill first for migration planning — it delegates to specialized skills."
---

# EMR to Databricks Migration Orchestrator

## Overview

This skill orchestrates the complete migration from Amazon EMR to Databricks using a structured 5-phase workflow:

| Phase | Name | Purpose |
|-------|------|---------|
| 1 | **Assess** | Inventory EMR clusters, jobs, data catalog, costs, and dependencies |
| 2 | **Plan** | Create migration plan, prioritize workloads, identify risks, define success criteria |
| 3 | **Migrate** | Convert code, migrate catalog, set up infrastructure, convert jobs/workflows |
| 4 | **Validate** | Data comparison, performance benchmarking, regression testing |
| 5 | **Cutover** | Parallel run, routing changes, monitoring, decommission EMR |

Each phase has explicit entry criteria, exit criteria, deliverables, and phase-gating rules. You must complete one phase before advancing to the next.

## Quick Start

**Start here:** Run `/emr-migration-assessment` to begin the assessment of your EMR environment. This is always the first step regardless of your migration goals.

Once assessment is complete, the orchestrator will guide you through the remaining phases and delegate to the appropriate specialized skills.

## Decision Tree

Based on what you want to accomplish, invoke the appropriate skill:

### "I don't know where to start"
Start with `/emr-migration-assessment`. It will inventory your environment and produce a migration roadmap.

### "I know what I need to do"
See the routing table below:

| What you want to do | Skill to invoke |
|---------------------|----------------|
| Assess my EMR environment | `/emr-migration-assessment` |
| Migrate Spark/PySpark code | `/emr-spark-code-migration` |
| Migrate Hive metastore or Glue catalog | `/emr-hive-to-unity-catalog` |
| Convert EMR steps/job flows to workflows | `/emr-steps-to-workflows` |
| Set up Databricks infrastructure (VPC, clusters, workspace) | `/emr-infra-migration` |
| Convert Spark/YARN/Hadoop configs | `/emr-config-migration` |
| Convert bootstrap actions to init scripts | `/emr-bootstrap-to-init-scripts` |
| Migrate EMR notebooks | `/emr-notebook-migration` |
| Migrate Spark Structured Streaming or Kafka workloads | `/emr-streaming-migration` |
| Validate migrated workloads | `/emr-migration-validation` |

For the full decision flowchart, see `decision-tree.md`.

## Phase Gating Criteria

Phases are sequential. You MUST satisfy the exit criteria of one phase before entering the next.

### Phase 1 (Assess) -> Phase 2 (Plan)
- Assessment report generated covering compute, jobs, data, code, security, networking, cost, and operations
- Dependency map created showing inter-job and cross-system dependencies
- Cost comparison (EMR vs Databricks) drafted
- Assessment report reviewed and approved by stakeholders

### Phase 2 (Plan) -> Phase 3 (Migrate)
- Migration plan document finalized with workload priority matrix
- Risk register created with mitigations for each identified risk
- Success criteria defined for each workload (SLAs, data accuracy, performance thresholds)
- Plan approved by customer/stakeholders

### Phase 3 (Migrate) -> Phase 4 (Validate)
- All in-scope workloads migrated to Databricks in a non-production environment
- Code converted and passing basic smoke tests
- Unity Catalog configured with migrated schemas and tables
- Databricks workflows created for all scheduled jobs
- Init scripts tested on cluster startup

### Phase 4 (Validate) -> Phase 5 (Cutover)
- Data comparison reports show row counts and checksums match within tolerance
- Performance benchmarks meet or exceed defined success criteria
- All regression tests pass
- Validation report signed off by data engineering and QA teams

### Phase 5 (Cutover) -> Complete
- Parallel run period completed successfully (minimum 1 week recommended)
- DNS/routing switched to Databricks endpoints
- Monitoring dashboards operational and alerting configured
- EMR decommission timeline agreed upon and scheduled
- EMR clusters terminated and resources cleaned up

## Related Skills

| Skill | Description | Invocation |
|-------|-------------|------------|
| **EMR Migration Assessment** | Inventory EMR clusters, jobs, data, costs; produce assessment report and dependency map | `/emr-migration-assessment` |
| **EMR Spark Code Migration** | Convert PySpark/Scala Spark code from EMR patterns to Databricks patterns (DBFS, Delta, UC) | `/emr-spark-code-migration` |
| **EMR Hive to Unity Catalog** | Migrate Hive metastore or AWS Glue Data Catalog to Databricks Unity Catalog | `/emr-hive-to-unity-catalog` |
| **EMR Steps to Workflows** | Convert EMR Steps, job flows, and Step Functions orchestration to Databricks Workflows | `/emr-steps-to-workflows` |
| **EMR Infrastructure Migration** | Set up Databricks workspace, VPC peering, cluster policies, instance pools to replace EMR infra | `/emr-infra-migration` |
| **EMR Config Migration** | Convert Spark, YARN, Hadoop, and Hive configurations to Databricks cluster/job configs | `/emr-config-migration` |
| **EMR Bootstrap to Init Scripts** | Convert EMR bootstrap actions to Databricks cluster init scripts | `/emr-bootstrap-to-init-scripts` |
| **EMR Notebook Migration** | Migrate EMR Studio, Zeppelin, or Jupyter notebooks to Databricks notebooks | `/emr-notebook-migration` |
| **EMR Streaming Migration** | Migrate Spark Structured Streaming, Kafka, and Kinesis workloads to Databricks | `/emr-streaming-migration` |
| **EMR Migration Validation** | Validate migrated workloads with data comparison, performance benchmarks, and regression tests | `/emr-migration-validation` |

## Critical Rules

1. **Always assess before migrating.** Never begin code conversion or infrastructure setup without completing Phase 1 (Assess). Skipping assessment leads to missed dependencies, incorrect sizing, and rework.

2. **Always validate after migrating.** Never proceed to cutover without completing Phase 4 (Validate). Every migrated workload must have data accuracy verification and performance benchmarking.

3. **Respect phase gates.** Do not skip phases. If a customer insists on jumping ahead, document the risks explicitly and obtain written acknowledgement.

4. **Delegate to specialized skills.** This orchestrator routes work to the appropriate specialized skill. Do not attempt to perform specialized migration tasks (code conversion, catalog migration, etc.) directly from this skill.

5. **Track progress.** Maintain a migration tracker showing each workload, its current phase, and its status. Update it after every skill invocation.
