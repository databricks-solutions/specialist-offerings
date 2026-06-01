# EMR IAM Roles to Databricks Unity Catalog Security

## Overview

EMR uses several IAM roles for cluster operations and data access. Databricks replaces most of these with Unity Catalog governance and a cross-account IAM role for workspace deployment. This guide maps each EMR IAM role to its Databricks equivalent.

## Role Mapping

### 1. EMR Service Role (EMR_DefaultRole) -> Databricks Cross-Account Role

**EMR Service Role** allows EMR to provision EC2 instances, manage scaling, and access CloudWatch.

**Databricks Cross-Account Role** allows Databricks to manage compute resources in your AWS account.

**EMR Service Role permissions (typical):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:RunInstances", "ec2:TerminateInstances",
        "ec2:CreateSecurityGroup", "ec2:DeleteSecurityGroup",
        "ec2:AuthorizeSecurityGroupIngress", "ec2:AuthorizeSecurityGroupEgress",
        "ec2:DescribeInstances", "ec2:DescribeSecurityGroups",
        "ec2:DescribeSubnets", "ec2:DescribeVpcs",
        "cloudwatch:PutMetricData",
        "s3:GetObject", "s3:ListBucket"
      ],
      "Resource": "*"
    }
  ]
}
```

**Databricks Cross-Account Role (template):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:RunInstances", "ec2:TerminateInstances",
        "ec2:CreateTags", "ec2:DescribeInstances",
        "ec2:DescribeVolumes", "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups", "ec2:DescribeVpcs",
        "ec2:CreateVolume", "ec2:DeleteVolume",
        "ec2:AttachVolume", "ec2:DetachVolume"
      ],
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "ec2:ResourceTag/Vendor": "Databricks"
        }
      }
    }
  ]
}
```

**Trust policy for Databricks:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::414351767826:root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<databricks-account-id>"
        }
      }
    }
  ]
}
```

### 2. EMR EC2 Instance Profile (EMR_EC2_DefaultRole) -> Storage Credentials + Instance Profile

The EMR EC2 instance profile provides S3 access, Glue catalog access, and KMS permissions to all nodes in the cluster.

**Databricks splits this into:**

| EMR Instance Profile Permission | Databricks Equivalent |
|---|---|
| S3 read/write | Unity Catalog storage credential + external location |
| Glue Data Catalog | Unity Catalog (replaces Glue as metastore) |
| KMS encrypt/decrypt | Workspace encryption config or cluster policy |
| CloudWatch Logs | Built-in Databricks logging |
| DynamoDB (EMRFS consistency) | Not needed (Delta Lake provides ACID) |
| SQS/SNS (notifications) | Databricks notifications (email, webhook) |

**EMR EC2 Instance Profile (typical):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
        "s3:ListBucket", "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::data-lake-bucket",
        "arn:aws:s3:::data-lake-bucket/*",
        "arn:aws:s3:::emr-logs-bucket",
        "arn:aws:s3:::emr-logs-bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetTable", "glue:GetPartitions",
        "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"
      ],
      "Resource": "arn:aws:kms:us-east-1:<account-id>:key/<key-id>"
    }
  ]
}
```

**Databricks Storage Credential IAM Role (replaces S3 access):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
        "s3:ListBucket", "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::data-lake-bucket",
        "arn:aws:s3:::data-lake-bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "arn:aws:iam::<account-id>:role/databricks-storage-credential"
    }
  ]
}
```

### 3. EMR Autoscaling Role -> Not Needed

EMR has a separate autoscaling role (`EMR_AutoScaling_DefaultRole`) that allows the autoscaling service to add/remove instances. Databricks manages autoscaling internally — no separate IAM role required.

### 4. EMR Security Configuration -> Cluster Policies + Workspace Config

**EMR Security Configuration:**
```json
{
  "EncryptionConfiguration": {
    "AtRestEncryptionConfiguration": {
      "S3EncryptionConfiguration": {
        "EncryptionMode": "SSE-KMS",
        "AwsKmsKey": "arn:aws:kms:us-east-1:<account>:key/<key-id>"
      },
      "LocalDiskEncryptionConfiguration": {
        "EncryptionKeyProviderType": "AwsKms",
        "AwsKmsKey": "arn:aws:kms:us-east-1:<account>:key/<key-id>"
      }
    },
    "InTransitEncryptionConfiguration": {
      "TLSCertificateConfiguration": {
        "CertificateProviderType": "PEM"
      }
    }
  }
}
```

**Databricks equivalents:**
- S3 encryption: Unchanged (S3 bucket policy handles this)
- EBS encryption: Configure in cluster policy or workspace settings
- In-transit TLS: Enabled by default in Databricks

## Terraform: Complete IAM Setup

```hcl
# Cross-account role for Databricks workspace
resource "aws_iam_role" "databricks_cross_account" {
  name = "databricks-cross-account-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::414351767826:root"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_account_id
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "databricks_cross_account" {
  name = "databricks-cross-account-policy"
  role = aws_iam_role.databricks_cross_account.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ec2:RunInstances", "ec2:TerminateInstances",
          "ec2:CreateTags", "ec2:DescribeInstances",
          "ec2:DescribeVolumes", "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups", "ec2:DescribeVpcs",
          "ec2:CreateVolume", "ec2:DeleteVolume",
          "ec2:AttachVolume", "ec2:DetachVolume"
        ]
        Resource = "*"
      }
    ]
  })
}

# Storage credential IAM role (replaces EMR EC2 instance profile for S3 access)
resource "aws_iam_role" "databricks_storage_credential" {
  name = "databricks-unity-catalog-storage"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_account_id
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "databricks_storage_credential" {
  name = "databricks-storage-policy"
  role = aws_iam_role.databricks_storage_credential.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:ListBucket", "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${var.data_bucket}",
          "arn:aws:s3:::${var.data_bucket}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = "sts:AssumeRole"
        Resource = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/databricks-unity-catalog-storage"
      }
    ]
  })
}

# Register in Unity Catalog
resource "databricks_storage_credential" "data_lake" {
  name = "data-lake-credential"
  aws_iam_role {
    role_arn = aws_iam_role.databricks_storage_credential.arn
  }
}

resource "databricks_external_location" "data_lake" {
  name            = "data-lake"
  url             = "s3://${var.data_bucket}/"
  credential_name = databricks_storage_credential.data_lake.name
}
```

## Migration Checklist

- [ ] Inventory all IAM roles used by EMR clusters
- [ ] Map S3 bucket access from EMR instance profiles
- [ ] Create Databricks cross-account role
- [ ] Create storage credential IAM role with equivalent S3 access
- [ ] Register storage credential in Unity Catalog
- [ ] Create external locations for each S3 path
- [ ] Grant access to appropriate users/groups/service principals
- [ ] Remove Glue catalog IAM permissions (Unity Catalog replaces Glue)
- [ ] Remove DynamoDB permissions (EMRFS consistency not needed)
- [ ] Remove EMR-specific IAM roles after migration is complete
- [ ] Validate S3 access from Databricks notebooks
