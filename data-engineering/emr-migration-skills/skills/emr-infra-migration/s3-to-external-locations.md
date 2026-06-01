# Migrating S3 Access: EMR EMRFS to Databricks Unity Catalog External Locations

## Overview

EMR accesses S3 through EMRFS using IAM instance profiles attached to EC2 instances. Every node in the cluster has the same S3 permissions. Databricks uses Unity Catalog with **storage credentials** and **external locations** for fine-grained S3 access control, decoupled from compute.

### Architecture Comparison

```
EMR:
  EC2 Instance Profile -> IAM Role -> S3 Bucket Policy -> S3

Databricks:
  Unity Catalog -> Storage Credential -> IAM Role -> S3
               -> External Location -> specific S3 path
               -> GRANT to users/groups
```

## Step-by-Step Migration

### Step 1: Create IAM Role for Storage Credential

This role is assumed by Databricks to access S3 on behalf of users.

**IAM Trust Policy** (allows Databricks to assume the role):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<your-databricks-account-id>"
        }
      }
    }
  ]
}
```

**IAM Permission Policy** (S3 access):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::my-data-bucket",
        "arn:aws:s3:::my-data-bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "sts:AssumeRole"
      ],
      "Resource": [
        "arn:aws:iam::<account-id>:role/databricks-storage-credential"
      ]
    }
  ]
}
```

### Step 2: Create Storage Credential in Unity Catalog

**Databricks CLI:**

```bash
databricks unity-catalog storage-credentials create \
  --name "s3-data-lake-credential" \
  --aws-iam-role '{"role_arn": "arn:aws:iam::<account-id>:role/databricks-storage-credential"}'
```

**Terraform:**

```hcl
resource "databricks_storage_credential" "s3_data_lake" {
  name = "s3-data-lake-credential"
  aws_iam_role {
    role_arn = aws_iam_role.databricks_storage_credential.arn
  }
  comment = "Storage credential for migrated EMR data lake"
}
```

### Step 3: Create External Locations

Create one external location per S3 path that needs distinct access control.

**Databricks CLI:**

```bash
# Raw data location
databricks unity-catalog external-locations create \
  --name "raw-data" \
  --url "s3://my-data-bucket/raw/" \
  --credential-name "s3-data-lake-credential"

# Processed data location
databricks unity-catalog external-locations create \
  --name "processed-data" \
  --url "s3://my-data-bucket/processed/" \
  --credential-name "s3-data-lake-credential"
```

**Terraform:**

```hcl
resource "databricks_external_location" "raw_data" {
  name            = "raw-data"
  url             = "s3://my-data-bucket/raw/"
  credential_name = databricks_storage_credential.s3_data_lake.name
  comment         = "Raw data from EMR ingestion pipelines"
}

resource "databricks_external_location" "processed_data" {
  name            = "processed-data"
  url             = "s3://my-data-bucket/processed/"
  credential_name = databricks_storage_credential.s3_data_lake.name
  comment         = "Processed data from EMR ETL pipelines"
}
```

### Step 4: Grant Access to Users and Groups

```sql
-- Grant read access to data engineers
GRANT READ FILES ON EXTERNAL LOCATION `raw-data` TO `data-engineers`;

-- Grant read/write access to ETL service principal
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `processed-data` TO `etl-service-principal`;

-- Grant create external table
GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `processed-data` TO `data-engineers`;
```

**Terraform:**

```hcl
resource "databricks_grants" "raw_data_grants" {
  external_location = databricks_external_location.raw_data.id
  grant {
    principal  = "data-engineers"
    privileges = ["READ_FILES"]
  }
}

resource "databricks_grants" "processed_data_grants" {
  external_location = databricks_external_location.processed_data.id
  grant {
    principal  = "data-engineers"
    privileges = ["READ_FILES", "WRITE_FILES", "CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "etl-service-principal"
    privileges = ["READ_FILES", "WRITE_FILES"]
  }
}
```

### Step 5: Update Code

**Before (EMR — direct S3 paths):**

```python
# EMR code using S3 paths directly (EMRFS handles access via instance profile)
df = spark.read.parquet("s3://my-data-bucket/raw/events/")
df.write.parquet("s3://my-data-bucket/processed/events/")

# Or with explicit credentials (bad practice, but common)
spark.conf.set("fs.s3a.access.key", "AKIA...")
spark.conf.set("fs.s3a.secret.key", "...")
df = spark.read.parquet("s3a://my-data-bucket/raw/events/")
```

**After (Databricks — Unity Catalog):**

```python
# Option 1: Use external locations (S3 paths still work, UC handles auth)
df = spark.read.parquet("s3://my-data-bucket/raw/events/")
df.write.parquet("s3://my-data-bucket/processed/events/")
# Works if external locations are configured — no code change needed!

# Option 2: Use Unity Catalog managed tables (recommended for new tables)
df = spark.read.table("catalog.schema.raw_events")
df.write.saveAsTable("catalog.schema.processed_events")

# Option 3: Use external tables backed by S3
# CREATE EXTERNAL TABLE catalog.schema.raw_events
# USING DELTA LOCATION 's3://my-data-bucket/raw/events/';
df = spark.read.table("catalog.schema.raw_events")
```

## Common Migration Patterns

### Pattern 1: Shared S3 Bucket (EMR and Databricks coexist)

During migration, both EMR and Databricks read/write the same S3 locations.

```
S3 Bucket
  /raw/          <- EMR writes, Databricks reads (external location)
  /processed/    <- Both write during migration
  /archive/      <- Databricks manages post-migration
```

Ensure the Databricks storage credential IAM role has the same S3 permissions as the EMR instance profile.

### Pattern 2: S3 Path Migration (new bucket or path structure)

```python
# Migrate data from EMR bucket to Databricks-managed location
dbutils.fs.cp(
    "s3://emr-data-bucket/processed/",
    "s3://databricks-data-bucket/processed/",
    recurse=True
)
```

### Pattern 3: EMRFS Consistent View Replacement

EMR's EMRFS consistent view (using DynamoDB) is not needed with Databricks. Delta Lake provides ACID transactions and consistent reads natively.

```python
# EMR: EMRFS consistent view config (remove this)
# spark.conf.set("fs.s3.consistent", "true")
# spark.conf.set("fs.s3.consistent.metadata.tableName", "EmrFSMetadata")

# Databricks: Delta provides consistency automatically
df.write.format("delta").save("s3://bucket/table/")
```

## Validation

After setup, validate access from a Databricks notebook:

```python
# List files in external location
dbutils.fs.ls("s3://my-data-bucket/raw/")

# Read data
df = spark.read.parquet("s3://my-data-bucket/raw/events/")
df.count()

# Write data (if write access granted)
df.limit(10).write.mode("overwrite").parquet("s3://my-data-bucket/processed/test/")

# Verify external location in Unity Catalog
display(spark.sql("SHOW EXTERNAL LOCATIONS"))
```

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Access Denied` on S3 path | External location not created or no grant | Create external location; grant READ_FILES |
| `No matching storage credential` | Storage credential IAM role cannot access the bucket | Check IAM policy on the storage credential role |
| `External location overlaps` | Two external locations cover the same path | Use more specific paths or remove the duplicate |
| `Unable to assume role` | Trust policy incorrect | Verify the Databricks AWS account ID and external ID |
