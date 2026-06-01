# EMR to Databricks Migration Phases

## Phase 1 — Assess

**Purpose:** Understand the full scope of the EMR environment before making any migration decisions.

**Entry Criteria:**
- Customer engagement has started
- Access to AWS account with EMR clusters (read-only IAM role at minimum)
- Access to AWS Cost Explorer or billing data

**Activities:**
1. Inventory all EMR clusters (transient and long-running)
2. Catalog all EMR steps, job flows, and scheduled jobs
3. Map the AWS Glue Data Catalog or Hive metastore (databases, tables, partitions)
4. Document data sources and sinks (S3 buckets, RDS, DynamoDB, Redshift, Kafka)
5. Profile code assets: PySpark, Scala, Hive SQL, Pig, Presto queries
6. Identify all JAR dependencies, custom libraries, and bootstrap actions
7. Map inter-job dependencies and cross-system integrations
8. Capture security posture: IAM roles, Lake Formation policies, encryption, Kerberos
9. Document networking: VPCs, subnets, security groups, S3 endpoints, cross-account access
10. Collect cost data: EMR spend, EC2, EBS, S3 storage, data transfer

**Deliverables:**
- **Assessment Report**: Comprehensive inventory of compute, jobs, data, code, security, networking, cost, and operations
- **Dependency Map**: Visual and tabular map of inter-job dependencies and external system integrations
- **Cost Comparison**: Side-by-side EMR vs Databricks estimated cost (compute, storage, platform fees)

**Exit Criteria:**
- Assessment report generated and covers all categories in the assessment checklist
- Dependency map reviewed — no unresolved "unknown" dependencies
- Cost comparison drafted with assumptions documented
- Assessment report approved by stakeholders

**Primary Skill:** `/emr-migration-assessment`

---

## Phase 2 — Plan

**Purpose:** Create a prioritized, risk-aware migration plan with clear success criteria.

**Entry Criteria:**
- Phase 1 (Assess) exit criteria met
- Assessment report approved

**Activities:**
1. Prioritize workloads using a scoring matrix (business criticality, complexity, dependencies, risk)
2. Group workloads into migration waves (typically 3-5 waves)
3. Define success criteria per workload: data accuracy thresholds, performance SLAs, uptime requirements
4. Identify risks and create a risk register with likelihood, impact, and mitigations
5. Design the target Databricks architecture: workspace topology, Unity Catalog structure, cluster policies
6. Plan the networking setup: VPC peering, Private Link, DNS configuration
7. Define the testing strategy: unit tests, integration tests, data validation, performance benchmarks
8. Create the migration timeline with milestones and dependencies
9. Assign roles and responsibilities (migration team, customer SMEs, Databricks support)
10. Define communication plan and escalation procedures

**Deliverables:**
- **Migration Plan Document**: End-to-end plan covering scope, timeline, architecture, waves, and team
- **Workload Priority Matrix**: Scored and ranked list of all workloads with wave assignments
- **Risk Register**: All identified risks with likelihood, impact, owner, and mitigation strategy
- **Target Architecture Diagram**: Databricks workspace topology, networking, catalog structure

**Exit Criteria:**
- Migration plan reviewed and approved by customer
- Workload priority matrix agreed upon — wave 1 workloads identified
- Risk register reviewed — all high-impact risks have mitigation plans
- Target architecture validated by Databricks solutions architect

**Primary Skills:** This phase is primarily planning work. Use `/emr-infra-migration` to design target architecture.

---

## Phase 3 — Migrate

**Purpose:** Execute the migration — convert code, migrate catalog, set up infrastructure, and create workflows.

**Entry Criteria:**
- Phase 2 (Plan) exit criteria met
- Migration plan approved by customer
- Databricks workspace provisioned (at least non-production)

**Activities:**
1. Set up Databricks infrastructure: workspace config, VPC peering, cluster policies, instance pools
2. Migrate AWS Glue Data Catalog / Hive metastore to Unity Catalog
3. Convert Spark/PySpark code from EMR patterns to Databricks patterns
4. Convert Hive SQL scripts to Databricks SQL or Spark SQL
5. Convert EMR bootstrap actions to Databricks init scripts
6. Convert EMR Steps and job flows to Databricks Workflows
7. Migrate EMR Studio / Zeppelin / Jupyter notebooks to Databricks notebooks
8. Migrate streaming workloads (Structured Streaming, Kafka consumers)
9. Convert Spark/YARN/Hadoop configurations to Databricks cluster configs
10. Set up monitoring, logging, and alerting in Databricks
11. Run smoke tests for each migrated workload

**Deliverables:**
- **Migrated Code**: All PySpark, Scala, SQL code converted and committed to version control
- **Unity Catalog**: Databases, schemas, tables, and access policies migrated
- **Databricks Workflows**: All scheduled jobs created with proper dependencies and alerting
- **Init Scripts**: Bootstrap actions converted and tested on cluster startup
- **Migrated Notebooks**: All notebooks converted to Databricks format
- **Streaming Jobs**: Structured Streaming jobs running on Databricks
- **Configuration**: Cluster policies, instance pools, Spark configs applied

**Exit Criteria:**
- All in-scope workloads (current wave) migrated to Databricks non-production environment
- Code converted and committed to version control
- Unity Catalog populated with migrated schemas and tables
- Databricks Workflows created and passing basic smoke tests
- Init scripts tested on cluster startup without errors
- Streaming jobs consuming and producing data correctly

**Primary Skills:**
| Task | Skill |
|------|-------|
| Infrastructure setup | `/emr-infra-migration` |
| Catalog migration | `/emr-hive-to-unity-catalog` |
| Spark code conversion | `/emr-spark-code-migration` |
| Job/step conversion | `/emr-steps-to-workflows` |
| Bootstrap actions | `/emr-bootstrap-to-init-scripts` |
| Notebook migration | `/emr-notebook-migration` |
| Streaming workloads | `/emr-streaming-migration` |
| Config conversion | `/emr-config-migration` |

---

## Phase 4 — Validate

**Purpose:** Verify that migrated workloads produce correct results and meet performance requirements.

**Entry Criteria:**
- Phase 3 (Migrate) exit criteria met for current wave
- All workloads running in Databricks non-production environment
- Validation test plan defined (from Phase 2)

**Activities:**
1. Run data comparison: row counts, checksums, column-level spot checks between EMR output and Databricks output
2. Execute performance benchmarks: job duration, resource utilization, cost per run
3. Run regression tests: compare business logic outputs (aggregations, transformations, ML model outputs)
4. Validate data freshness and latency for streaming workloads
5. Test failure scenarios: cluster autoscaling, job retries, network partitions
6. Validate security: access controls, encryption, audit logging
7. Test end-to-end data pipelines from source to consumption layer
8. Compare costs: actual Databricks run cost vs EMR run cost
9. Document any discrepancies and remediation actions
10. Obtain sign-off from data engineering, QA, and business stakeholders

**Deliverables:**
- **Validation Report**: Data accuracy, completeness, and consistency results per workload
- **Benchmark Results**: Performance comparison (EMR vs Databricks) with job durations, costs, resource usage
- **Regression Test Results**: Pass/fail status for all business logic tests
- **Discrepancy Log**: Any differences found with root cause and remediation status

**Exit Criteria:**
- Data comparison shows row counts and checksums match within defined tolerance (e.g., 99.99%)
- Performance benchmarks meet or exceed success criteria from Phase 2
- All regression tests pass
- No open P1/P2 discrepancies
- Validation report signed off by data engineering and QA teams

**Primary Skill:** `/emr-migration-validation`

---

## Phase 5 — Cutover

**Purpose:** Transition production traffic from EMR to Databricks and decommission EMR resources.

**Entry Criteria:**
- Phase 4 (Validate) exit criteria met
- Production Databricks workspace provisioned and configured identically to non-prod
- Cutover plan approved by customer (including rollback plan)
- On-call team briefed and monitoring dashboards ready

**Activities:**
1. Deploy validated workloads to Databricks production environment
2. Start parallel run: both EMR and Databricks processing the same data simultaneously
3. Compare parallel run outputs daily for the agreed parallel run period (typically 1-2 weeks)
4. Switch DNS/routing for consumers to point to Databricks outputs
5. Redirect data producers to write to Databricks-managed locations (or ensure S3 paths are shared)
6. Monitor Databricks workloads: job success rates, latencies, costs, error rates
7. Execute rollback plan if critical issues are found (switch back to EMR)
8. After successful parallel run, stop EMR jobs
9. Create EMR decommission timeline: stop clusters, remove IAM roles, clean up S3, terminate resources
10. Archive EMR configurations and code for reference
11. Conduct migration retrospective

**Deliverables:**
- **Cutover Plan**: Step-by-step cutover runbook with rollback procedures
- **Parallel Run Report**: Daily comparison results during parallel run period
- **Monitoring Dashboard**: Databricks job health, SLA compliance, cost tracking
- **EMR Decommission Timeline**: Phased plan to terminate EMR resources with dates and owners
- **Migration Retrospective**: Lessons learned, what went well, what to improve

**Exit Criteria:**
- Parallel run completed with no critical discrepancies
- All consumers reading from Databricks outputs
- Monitoring dashboards operational with alerting configured
- EMR jobs stopped and clusters terminated
- EMR decommission timeline executed (or scheduled with committed dates)
- Migration retrospective completed and documented
