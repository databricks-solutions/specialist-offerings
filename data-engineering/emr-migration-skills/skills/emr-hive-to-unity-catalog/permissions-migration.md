# Permissions Migration: AWS IAM / Lake Formation to Unity Catalog

## Overview

Migrating permissions is one of the most critical and error-prone aspects of moving from EMR to Databricks. This guide covers mapping AWS-native access controls to Unity Catalog's grant model.

## AWS IAM-Based Access to Unity Catalog Storage Credentials

### EMR Instance Profile to Storage Credential

In EMR, clusters assume an IAM instance profile that grants access to S3 data. In Unity Catalog, this maps to a **storage credential**.

```sql
-- Create a storage credential backed by an IAM role
CREATE STORAGE CREDENTIAL emr_data_access
WITH (AWS_IAM_ROLE = 'arn:aws:iam::123456789012:role/databricks-uc-s3-access')
COMMENT 'IAM role for accessing EMR data in S3';
```

The IAM role must have a trust policy allowing Databricks to assume it:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-XXXXX"
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

### S3 Bucket Policies to External Locations

Each S3 path that UC needs to access must be registered as an external location.

```sql
-- Register S3 paths as external locations
CREATE EXTERNAL LOCATION emr_raw_data
  URL 's3://emr-data-bucket/raw/'
  WITH (STORAGE CREDENTIAL emr_data_access)
  COMMENT 'Raw data from EMR pipelines';

CREATE EXTERNAL LOCATION emr_processed_data
  URL 's3://emr-data-bucket/processed/'
  WITH (STORAGE CREDENTIAL emr_data_access)
  COMMENT 'Processed data from EMR pipelines';

-- Grant access to specific groups
GRANT READ FILES ON EXTERNAL LOCATION emr_raw_data TO `data-engineers`;
GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION emr_raw_data TO `data-engineers`;
```

### Cross-Account IAM Roles to Storage Credentials

If EMR accessed data in a different AWS account via cross-account IAM roles:

```sql
-- Storage credential that assumes a cross-account role
CREATE STORAGE CREDENTIAL cross_account_data
WITH (AWS_IAM_ROLE = 'arn:aws:iam::987654321098:role/cross-account-databricks-access')
COMMENT 'Cross-account access to partner data bucket';

CREATE EXTERNAL LOCATION partner_data
  URL 's3://partner-data-bucket/shared/'
  WITH (STORAGE CREDENTIAL cross_account_data);
```

## AWS Lake Formation to Unity Catalog Grants

### Database-Level Permissions to Schema-Level Grants

```sql
-- Lake Formation: GRANT ALL ON DATABASE raw_data TO role/data-engineers
-- Unity Catalog equivalent:
GRANT USE SCHEMA ON SCHEMA uc_catalog.raw_data TO `data-engineers`;
GRANT CREATE TABLE ON SCHEMA uc_catalog.raw_data TO `data-engineers`;
GRANT SELECT ON SCHEMA uc_catalog.raw_data TO `data-engineers`;

-- Lake Formation: GRANT DESCRIBE ON DATABASE analytics TO role/analysts
-- Unity Catalog equivalent:
GRANT USE SCHEMA ON SCHEMA uc_catalog.analytics TO `analysts`;
```

### Table-Level Permissions to Table-Level Grants

```sql
-- Lake Formation: GRANT SELECT ON TABLE raw_data.events TO role/analysts
-- Unity Catalog equivalent:
GRANT SELECT ON TABLE uc_catalog.raw_data.events TO `analysts`;

-- Lake Formation: GRANT INSERT ON TABLE processed.output TO role/etl-service
-- Unity Catalog equivalent:
GRANT MODIFY ON TABLE uc_catalog.processed.output TO `etl-service`;

-- Lake Formation: GRANT ALL ON TABLE processed.output TO role/data-engineers
-- Unity Catalog equivalent:
GRANT ALL PRIVILEGES ON TABLE uc_catalog.processed.output TO `data-engineers`;
```

### Column-Level Security to Column Masks

Lake Formation supports column-level `SELECT` grants. Unity Catalog uses **column masks** for column-level security.

```sql
-- Lake Formation: GRANT SELECT ON TABLE users (name, email) TO role/limited-analysts
-- (hides salary, ssn columns)

-- Unity Catalog: Use column masks to redact sensitive columns
-- First, create masking functions
CREATE FUNCTION uc_catalog.common.mask_ssn(ssn STRING)
  RETURNS STRING
  RETURN CASE
    WHEN is_member('pii-authorized') THEN ssn
    ELSE CONCAT('***-**-', RIGHT(ssn, 4))
  END;

CREATE FUNCTION uc_catalog.common.mask_salary(salary DECIMAL(18,2))
  RETURNS DECIMAL(18,2)
  RETURN CASE
    WHEN is_member('hr-team') THEN salary
    ELSE NULL
  END;

-- Apply column masks to the table
ALTER TABLE uc_catalog.schema.users
  ALTER COLUMN ssn SET MASK uc_catalog.common.mask_ssn;

ALTER TABLE uc_catalog.schema.users
  ALTER COLUMN salary SET MASK uc_catalog.common.mask_salary;
```

### Row-Level Security to Row Filters

Lake Formation supports row-level filtering. Unity Catalog uses **row filters**.

```sql
-- Lake Formation: Row-level filter on region column
-- Only show rows where region matches user's allowed regions

-- Unity Catalog: Create a row filter function
CREATE FUNCTION uc_catalog.common.region_filter(region STRING)
  RETURNS BOOLEAN
  RETURN (
    is_member('global-access')
    OR region IN (
      SELECT allowed_region
      FROM uc_catalog.common.user_region_access
      WHERE user_name = current_user()
    )
  );

-- Apply the row filter to the table
ALTER TABLE uc_catalog.schema.sales_data
  SET ROW FILTER uc_catalog.common.region_filter ON (region);
```

### Tag-Based Access Control (ABAC)

Lake Formation tag-based access maps to Unity Catalog tags.

```sql
-- Lake Formation: Tag key=classification, value=pii -> grant to pii-authorized
-- Unity Catalog: Use tags + policies

-- Apply tags to columns
ALTER TABLE uc_catalog.schema.users
  ALTER COLUMN ssn SET TAGS ('classification' = 'pii');

ALTER TABLE uc_catalog.schema.users
  ALTER COLUMN email SET TAGS ('classification' = 'pii');

-- Tags can be used in column mask functions
CREATE FUNCTION uc_catalog.common.pii_mask(value STRING)
  RETURNS STRING
  RETURN CASE
    WHEN is_member('pii-authorized') THEN value
    ELSE '***REDACTED***'
  END;
```

## Glue Resource Policies to UC Privilege Model

Glue resource policies control cross-account catalog access. In UC, this is handled by the sharing model.

```sql
-- Share data with another Databricks account/workspace via Delta Sharing
CREATE SHARE emr_migrated_data;
ALTER SHARE emr_migrated_data ADD TABLE uc_catalog.raw_data.events;
ALTER SHARE emr_migrated_data ADD SCHEMA uc_catalog.processed;

-- Grant a recipient access to the share
CREATE RECIPIENT partner_workspace
  USING ID '<recipient-sharing-id>';

GRANT SELECT ON SHARE emr_migrated_data TO RECIPIENT partner_workspace;
```

## Migration Steps

### Step 1: Document Current Permissions

```python
import boto3
import json

def export_lake_formation_permissions(region: str = 'us-east-1') -> dict:
    """Export all Lake Formation permissions for migration planning."""
    lf = boto3.client('lakeformation', region_name=region)

    permissions = []
    paginator = lf.get_paginator('list_permissions')
    for page in paginator.paginate():
        permissions.extend(page['PrincipalResourcePermissions'])

    # Organize by resource type
    result = {
        'database_permissions': [],
        'table_permissions': [],
        'column_permissions': [],
        'data_location_permissions': [],
    }

    for perm in permissions:
        resource = perm['Resource']
        principal = perm['Principal']['DataLakePrincipalIdentifier']
        grants = perm['Permissions']
        grantable = perm.get('PermissionsWithGrantOption', [])

        entry = {
            'principal': principal,
            'permissions': grants,
            'grantable': grantable,
        }

        if 'Database' in resource:
            entry['database'] = resource['Database']['Name']
            result['database_permissions'].append(entry)
        elif 'Table' in resource:
            entry['database'] = resource['Table']['DatabaseName']
            entry['table'] = resource['Table'].get('Name', '*')
            result['table_permissions'].append(entry)
        elif 'TableWithColumns' in resource:
            entry['database'] = resource['TableWithColumns']['DatabaseName']
            entry['table'] = resource['TableWithColumns']['Name']
            entry['columns'] = resource['TableWithColumns'].get('ColumnNames', [])
            entry['excluded_columns'] = resource['TableWithColumns'].get('ColumnWildcard', {}).get('ExcludedColumnNames', [])
            result['column_permissions'].append(entry)
        elif 'DataLocation' in resource:
            entry['location'] = resource['DataLocation']['ResourceArn']
            result['data_location_permissions'].append(entry)

    return result

# Export permissions
perms = export_lake_formation_permissions()
with open('lake_formation_permissions.json', 'w') as f:
    json.dump(perms, f, indent=2)
```

### Step 2: Map to UC Grant Equivalents

```python
def map_lf_to_uc_grants(
    lf_permissions: dict,
    uc_catalog: str,
    principal_mapping: dict,
) -> list:
    """
    Map Lake Formation permissions to UC GRANT statements.

    Args:
        lf_permissions: Output from export_lake_formation_permissions()
        uc_catalog: Target UC catalog name
        principal_mapping: Map of IAM principal -> UC group name
            e.g. {'arn:aws:iam::123:role/data-eng': 'data-engineers'}
    """
    uc_grants = []

    LF_TO_UC_PERMISSION = {
        'ALL': 'ALL PRIVILEGES',
        'SELECT': 'SELECT',
        'INSERT': 'MODIFY',
        'ALTER': 'MODIFY',
        'DROP': 'MODIFY',
        'DESCRIBE': 'USE SCHEMA',  # approximate mapping
        'CREATE_TABLE': 'CREATE TABLE',
        'CREATE_DATABASE': 'CREATE SCHEMA',
    }

    # Database-level permissions
    for perm in lf_permissions.get('database_permissions', []):
        uc_principal = principal_mapping.get(perm['principal'], perm['principal'])
        schema = perm['database']

        for lf_perm in perm['permissions']:
            uc_perm = LF_TO_UC_PERMISSION.get(lf_perm, None)
            if uc_perm:
                uc_grants.append(
                    f"GRANT {uc_perm} ON SCHEMA `{uc_catalog}`.`{schema}` TO `{uc_principal}`;"
                )
        # Always grant USE SCHEMA if any permission exists
        uc_grants.append(
            f"GRANT USE SCHEMA ON SCHEMA `{uc_catalog}`.`{schema}` TO `{uc_principal}`;"
        )

    # Table-level permissions
    for perm in lf_permissions.get('table_permissions', []):
        uc_principal = principal_mapping.get(perm['principal'], perm['principal'])
        schema = perm['database']
        table = perm['table']

        for lf_perm in perm['permissions']:
            uc_perm = LF_TO_UC_PERMISSION.get(lf_perm, None)
            if uc_perm and uc_perm not in ('USE SCHEMA', 'CREATE SCHEMA', 'CREATE TABLE'):
                if table == '*':
                    uc_grants.append(
                        f"GRANT {uc_perm} ON SCHEMA `{uc_catalog}`.`{schema}` TO `{uc_principal}`;"
                    )
                else:
                    uc_grants.append(
                        f"GRANT {uc_perm} ON TABLE `{uc_catalog}`.`{schema}`.`{table}` TO `{uc_principal}`;"
                    )

    # Column-level permissions become column mask recommendations
    for perm in lf_permissions.get('column_permissions', []):
        uc_principal = principal_mapping.get(perm['principal'], perm['principal'])
        schema = perm['database']
        table = perm['table']
        columns = perm.get('columns', [])
        excluded = perm.get('excluded_columns', [])

        if excluded:
            uc_grants.append(
                f"-- Column-level security: {uc_principal} should NOT see columns "
                f"{excluded} on `{uc_catalog}`.`{schema}`.`{table}`"
            )
            uc_grants.append(
                f"-- TODO: Create column mask functions for: {', '.join(excluded)}"
            )
        elif columns:
            uc_grants.append(
                f"-- Column-level security: {uc_principal} can only see columns "
                f"{columns} on `{uc_catalog}`.`{schema}`.`{table}`"
            )
            uc_grants.append(
                f"-- TODO: Create column mask functions for all other columns"
            )

    return uc_grants
```

### Step 3: Create UC Groups Matching IAM Roles/Groups

```sql
-- Map IAM roles/groups to Databricks account groups
-- This is done via Databricks Account Console or SCIM provisioning

-- Example group creation (via Databricks Account API or UI):
-- IAM Role: arn:aws:iam::123:role/data-engineers  ->  UC Group: data-engineers
-- IAM Role: arn:aws:iam::123:role/analysts         ->  UC Group: analysts
-- IAM Role: arn:aws:iam::123:role/etl-service      ->  UC Group: etl-service
-- IAM Role: arn:aws:iam::123:role/pii-authorized   ->  UC Group: pii-authorized

-- Verify groups exist
SELECT * FROM system.access.groups WHERE display_name IN ('data-engineers', 'analysts', 'etl-service');
```

### Step 4: Apply Grants via SQL or Terraform

#### SQL Approach

```sql
-- Run the generated GRANT statements
-- (output from map_lf_to_uc_grants function)

-- Catalog-level grants
GRANT USE CATALOG ON CATALOG uc_catalog TO `data-engineers`;
GRANT USE CATALOG ON CATALOG uc_catalog TO `analysts`;
GRANT USE CATALOG ON CATALOG uc_catalog TO `etl-service`;

-- Schema-level grants
GRANT USE SCHEMA ON SCHEMA uc_catalog.raw_data TO `data-engineers`;
GRANT CREATE TABLE ON SCHEMA uc_catalog.raw_data TO `data-engineers`;
GRANT SELECT ON SCHEMA uc_catalog.raw_data TO `data-engineers`;

GRANT USE SCHEMA ON SCHEMA uc_catalog.processed TO `analysts`;
GRANT SELECT ON SCHEMA uc_catalog.processed TO `analysts`;

-- Table-level grants
GRANT SELECT ON TABLE uc_catalog.raw_data.events TO `analysts`;
GRANT ALL PRIVILEGES ON TABLE uc_catalog.processed.daily_agg TO `etl-service`;
```

#### Terraform Approach (Permissions as Code)

```hcl
# Storage credential
resource "databricks_storage_credential" "emr_data" {
  name = "emr_data_access"
  aws_iam_role {
    role_arn = "arn:aws:iam::123456789012:role/databricks-uc-s3-access"
  }
  comment = "IAM role for accessing EMR data in S3"
}

# External location
resource "databricks_external_location" "raw_data" {
  name            = "emr_raw_data"
  url             = "s3://emr-data-bucket/raw/"
  credential_name = databricks_storage_credential.emr_data.name
  comment         = "Raw data from EMR pipelines"
}

# Catalog
resource "databricks_catalog" "migrated" {
  name    = "uc_catalog"
  comment = "Migrated from EMR Glue Data Catalog"
}

# Schema
resource "databricks_schema" "raw_data" {
  catalog_name = databricks_catalog.migrated.name
  name         = "raw_data"
  comment      = "Migrated from Glue database: raw_data"
}

# Grants -- catalog level
resource "databricks_grants" "catalog_grants" {
  catalog = databricks_catalog.migrated.name

  grant {
    principal  = "data-engineers"
    privileges = ["USE_CATALOG"]
  }

  grant {
    principal  = "analysts"
    privileges = ["USE_CATALOG"]
  }

  grant {
    principal  = "etl-service"
    privileges = ["USE_CATALOG"]
  }
}

# Grants -- schema level
resource "databricks_grants" "raw_data_grants" {
  schema = "${databricks_catalog.migrated.name}.${databricks_schema.raw_data.name}"

  grant {
    principal  = "data-engineers"
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT"]
  }

  grant {
    principal  = "analysts"
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

# Grants -- external location
resource "databricks_grants" "raw_data_location_grants" {
  external_location = databricks_external_location.raw_data.id

  grant {
    principal  = "data-engineers"
    privileges = ["READ_FILES", "CREATE_EXTERNAL_TABLE"]
  }
}
```

### Step 5: Validate Access Patterns

```sql
-- Test as a specific user/group using SET OWNER or impersonation
-- Verify data-engineers can read raw_data
SELECT * FROM uc_catalog.raw_data.events LIMIT 5;

-- Verify analysts can read processed but not raw_data (if restricted)
-- Run as analyst user:
SELECT * FROM uc_catalog.processed.daily_agg LIMIT 5;  -- should succeed
SELECT * FROM uc_catalog.raw_data.events LIMIT 5;       -- should fail if no grant

-- Verify column masks are working
-- Run as non-pii-authorized user:
SELECT ssn FROM uc_catalog.schema.users LIMIT 5;
-- Should return masked values like '***-**-1234'

-- Verify row filters are working
SELECT DISTINCT region FROM uc_catalog.schema.sales_data;
-- Should only show regions the current user is authorized for

-- Check effective grants for a principal
SHOW GRANTS ON SCHEMA uc_catalog.raw_data;
SHOW GRANTS `data-engineers` ON SCHEMA uc_catalog.raw_data;
SHOW GRANTS ON TABLE uc_catalog.raw_data.events;
```

## Common Permission Migration Pitfalls

1. **Overly broad grants**: Lake Formation `ALL` on a database grants permissions on future tables too. UC `ALL PRIVILEGES` on a schema does the same -- verify this is intended.

2. **Service account access**: EMR ETL jobs using instance profiles need equivalent service principal access in UC. Create a Databricks service principal and grant it the necessary permissions.

3. **Cross-account access**: Lake Formation cross-account grants require setting up Delta Sharing or workspace-level catalog access in UC.

4. **Data location permissions**: Lake Formation `DATA_LOCATION_ACCESS` maps to external location grants in UC. Do not forget these or external table creation will fail.

5. **SUPER permission**: Lake Formation's implicit admin/super permissions have no direct UC equivalent. Use explicit `GRANT` statements for UC metastore admins.
