---
name: emr-infra-migration
description: "Migrate EMR infrastructure to Databricks — instance types, S3 access, IAM roles, networking. Use when: (1) 'EMR instance type mapping', (2) 'S3 access from Databricks', (3) 'IAM roles for Databricks', (4) 'VPC configuration for Databricks', (5) 'EMR security to Databricks', (6) any infrastructure-level migration planning."
---

# EMR Infrastructure Migration to Databricks

## Overview

EMR infrastructure consists of EC2 instances, S3 storage, IAM roles, VPC networking, and encryption configuration. Databricks abstracts much of this but still requires equivalent infrastructure setup. The key difference: Databricks manages the compute plane (cluster orchestration, Spark runtime, autoscaling) while you control the data plane (storage, networking, identity).

## Key Infrastructure Differences

| Component | EMR | Databricks |
|---|---|---|
| Compute | EC2 instances you select and manage | Databricks selects/manages VMs; you configure instance types and policies |
| Storage | S3 with EMRFS / HDFS | S3 via Unity Catalog external locations (no HDFS) |
| Identity | IAM roles attached to EC2 instances | Storage credentials + instance profiles via Unity Catalog |
| Networking | Your VPC, your subnets | Customer-managed VPC or Databricks-managed VPC |
| Metastore | Hive Metastore (Glue or local) | Unity Catalog (replaces HMS) |
| Encryption | EMR security configurations | Cluster policies + workspace-level encryption |
| Autoscaling | EMR managed scaling or custom policies | Databricks autoscaling or Serverless |

## Quick Instance Type Mapping (Top 10)

| EMR Instance | vCPU | Mem (GB) | Databricks Use Case |
|---|---|---|---|
| m5.xlarge | 4 | 16 | General purpose — small jobs |
| m5.2xlarge | 8 | 32 | General purpose — medium jobs |
| m5.4xlarge | 16 | 64 | General purpose — large jobs |
| r5.xlarge | 4 | 32 | Memory-heavy joins/caching |
| r5.2xlarge | 8 | 64 | Memory-heavy joins/caching |
| r5.4xlarge | 16 | 128 | Memory-heavy joins/caching |
| c5.2xlarge | 8 | 16 | CPU-heavy transforms |
| c5.4xlarge | 16 | 32 | CPU-heavy transforms |
| i3.xlarge | 4 | 30.5 | Storage-optimized (Delta cache) |
| i3.2xlarge | 8 | 61 | Storage-optimized (Delta cache) |

All EMR instance types are available in Databricks on AWS. See `instance-type-mapping.md` for the full mapping with cost comparison and Photon/Graviton considerations.

## S3 Access Migration Summary

EMR accesses S3 via EMRFS with IAM instance profiles. Databricks accesses S3 via Unity Catalog external locations with storage credentials.

**Migration steps:**
1. Create an IAM role for the Databricks storage credential
2. Register the storage credential in Unity Catalog
3. Create external locations for each S3 path
4. Grant access to users/groups
5. Update code to use Unity Catalog paths or external locations

See `s3-to-external-locations.md` for step-by-step with Terraform and CLI examples.

## IAM Migration Summary

| EMR Role | Databricks Equivalent |
|---|---|
| EMR service role (`EMR_DefaultRole`) | Databricks cross-account role (workspace deployment) |
| EC2 instance profile (`EMR_EC2_DefaultRole`) | Storage credentials + instance profiles in Unity Catalog |
| EMR autoscaling role | Not needed (Databricks manages autoscaling internally) |
| S3 access policies on instance profile | Storage credential IAM role with S3 access |
| KMS key policies | Workspace encryption configuration |

See `iam-to-unity-catalog.md` for IAM policy templates and migration steps.

## Networking Summary

| EMR Pattern | Databricks Equivalent |
|---|---|
| Public subnet with internet access | Databricks-managed VPC (default) |
| Private subnet with NAT gateway | Customer-managed VPC with NAT |
| VPC peering to other services | VPC peering or PrivateLink |
| Security groups for EMR | Security groups for Databricks data plane |
| S3 VPC endpoint | S3 gateway endpoint (same) |
| STS VPC endpoint | STS endpoint for cross-account access |

See `networking-patterns.md` for detailed VPC architecture patterns.

## Encryption

| EMR Encryption | Databricks Equivalent |
|---|---|
| At-rest: S3 SSE-S3/SSE-KMS | Same S3 encryption (unchanged) |
| At-rest: EBS encryption | EBS encryption via cluster policy or workspace config |
| In-transit: TLS | TLS enabled by default |
| In-transit: Spark RPC encryption | Enabled by default in Databricks Runtime |

## Related Skills

- **emr-migration-assessment** — assess EMR workloads before migration
- **emr-config-migration** — convert Spark/YARN configurations
- **emr-steps-to-workflows** — convert EMR Steps to Databricks Workflows
