# EMR Pre-Migration Assessment Checklist

Use this checklist to ensure complete coverage during Phase 1 (Assess). Every item should be documented in the assessment report before proceeding to Phase 2 (Plan).

---

## Compute

- [ ] Total number of EMR clusters (active and recently terminated)
- [ ] Cluster types: transient (per-job) vs long-running (persistent)
- [ ] Instance types for master, core, and task nodes
- [ ] Spot vs On-Demand vs Reserved Instance mix per cluster
- [ ] Autoscaling configurations: min/max nodes, scaling policies, metrics used
- [ ] Cluster utilization metrics: average CPU, memory, HDFS usage over last 30/60/90 days
- [ ] EMR release versions in use (emr-6.x, emr-7.x, etc.)
- [ ] Applications installed per cluster (Spark, Hive, Presto, HBase, Flink, etc.)
- [ ] Average cluster lifetime (for transient clusters)
- [ ] Peak concurrency: maximum clusters running simultaneously

## Jobs

- [ ] Total number of EMR Steps across all clusters
- [ ] Job flow types: Spark submit, Hive script, Custom JAR, Streaming, Pig, Presto
- [ ] Scheduling mechanism: cron, Apache Airflow, AWS Step Functions, AWS Data Pipeline, MWAA, custom
- [ ] Job execution frequency: hourly, daily, weekly, event-driven
- [ ] Inter-job dependencies: which jobs depend on output of other jobs
- [ ] Cross-system dependencies: jobs triggered by external events (S3 events, SNS, SQS, API calls)
- [ ] SLA requirements per job: max allowed runtime, data freshness guarantees
- [ ] Job failure rates and retry policies
- [ ] Average and p95 job durations
- [ ] Number of concurrent jobs per cluster

## Data

- [ ] AWS Glue Data Catalog: number of databases and tables
- [ ] Hive metastore (if self-managed): version, database, number of databases/tables
- [ ] Data formats in use: Parquet, ORC, CSV, JSON, Avro, Delta Lake, Apache Iceberg, Hudi
- [ ] Total data storage size (S3, HDFS, EBS)
- [ ] S3 bucket inventory: buckets used for input, output, and intermediate data
- [ ] Partitioning strategies: date-based, hash-based, hybrid
- [ ] Table sizes: distribution of table sizes (identify the largest tables)
- [ ] Data retention policies: how long is data kept, lifecycle rules on S3
- [ ] Data sources: RDS, DynamoDB, Redshift, Kinesis, Kafka, JDBC, APIs
- [ ] Data sinks: S3, Redshift, RDS, Elasticsearch, downstream consumers
- [ ] Data freshness requirements: real-time, near-real-time, batch

## Code

- [ ] Languages in use: PySpark, Scala Spark, Java Spark, Hive SQL, Pig Latin, Presto SQL, SparkR
- [ ] Number of scripts/applications per language
- [ ] Custom libraries and internal packages (Python wheels, JARs, eggs)
- [ ] Third-party JAR dependencies: list all non-standard JARs deployed to clusters
- [ ] Notebook formats: EMR Studio (Jupyter), Zeppelin notebooks
- [ ] Number of notebooks and their primary purpose (exploration, ETL, reporting)
- [ ] Version control: are scripts in Git, S3, or unmanaged?
- [ ] Code complexity: simple ETL, complex ML pipelines, graph processing, etc.
- [ ] Hadoop API usage: direct HDFS API calls, MapReduce jobs, custom InputFormats
- [ ] AWS SDK usage in Spark code: direct S3 client calls, Glue API calls, DynamoDB access

## Security

- [ ] IAM roles: EMR service role, EC2 instance profile, autoscaling role
- [ ] IAM policies: what permissions do EMR clusters have (S3, Glue, KMS, DynamoDB, etc.)
- [ ] AWS Lake Formation policies: table-level and column-level access controls
- [ ] Encryption at rest: S3 SSE (SSE-S3, SSE-KMS, SSE-C), EBS encryption, LUKS
- [ ] Encryption in transit: TLS for Spark shuffle, HTTPS for S3 access
- [ ] Kerberos: is Kerberos enabled for cluster authentication
- [ ] LDAP/AD integration for user authentication
- [ ] Security configurations: EMR security configuration name and settings
- [ ] Audit logging: CloudTrail for API calls, S3 access logging
- [ ] Secrets management: AWS Secrets Manager, Parameter Store, or hardcoded credentials
- [ ] Cross-account access patterns: which accounts access which resources

## Networking

- [ ] VPC configuration: VPC ID, CIDR range, region, availability zones
- [ ] Subnet configuration: public vs private subnets for EMR
- [ ] NAT Gateway: is one in place for private subnet internet access
- [ ] S3 VPC Endpoint (Gateway endpoint): is it configured
- [ ] Other VPC Endpoints: Glue, KMS, STS, CloudWatch, DynamoDB
- [ ] Security groups: inbound/outbound rules for master and core nodes
- [ ] DNS configuration: private hosted zones, custom DNS resolution
- [ ] VPC peering or Transit Gateway connections
- [ ] Direct Connect or VPN to on-premises
- [ ] Cross-region data transfer patterns
- [ ] Public accessibility: are any EMR clusters publicly accessible

## Cost

- [ ] Monthly EMR spend (last 3-6 months trend)
- [ ] EMR cost breakdown: EC2 instances, EMR premium, EBS volumes
- [ ] S3 storage costs associated with EMR workloads
- [ ] Data transfer costs: inter-AZ, inter-region, internet egress
- [ ] Reserved Instance or Savings Plan coverage for EMR instances
- [ ] Spot Instance savings and interruption rates
- [ ] Cost per job (if tagging is in place)
- [ ] Cost trends: is spend growing, stable, or declining
- [ ] Underutilized resources: clusters running idle, oversized instances
- [ ] Glue Data Catalog costs (API calls, storage)

## Operations

- [ ] Monitoring: CloudWatch metrics collected, custom dashboards
- [ ] Logging: Spark event logs, YARN logs, step logs — where are they stored (S3, CloudWatch Logs)
- [ ] Alerting: CloudWatch Alarms, SNS topics, PagerDuty, Opsgenie integration
- [ ] On-call procedures: who responds to EMR cluster/job failures
- [ ] Runbooks: documented procedures for common failure scenarios
- [ ] Deployment process: how are new jobs and code changes deployed to EMR
- [ ] CI/CD: Jenkins, CodePipeline, GitHub Actions, or manual deployment
- [ ] Change management: approval process for production changes
- [ ] Disaster recovery: multi-region, backup strategies, RPO/RTO requirements
- [ ] Support: AWS Business/Enterprise Support, third-party managed services

---

## Assessment Report Template

After completing the checklist, compile the findings into an assessment report with the following sections:

1. **Executive Summary**: High-level overview of the EMR environment and migration readiness
2. **Compute Inventory**: Cluster details, instance types, utilization
3. **Job Inventory**: All jobs with scheduling, dependencies, and SLAs
4. **Data Inventory**: Catalog, storage, formats, sizes, lineage
5. **Code Inventory**: Languages, scripts, libraries, notebooks
6. **Security Posture**: IAM, encryption, network security, audit
7. **Network Architecture**: VPC topology, connectivity, endpoints
8. **Cost Analysis**: Current EMR spend with Databricks cost estimate
9. **Dependency Map**: Inter-job and cross-system dependency diagram
10. **Risk Summary**: Key risks identified during assessment
11. **Recommendation**: Proceed / proceed with conditions / defer migration
