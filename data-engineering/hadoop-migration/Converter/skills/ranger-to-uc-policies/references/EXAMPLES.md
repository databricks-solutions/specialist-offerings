# Examples: Ranger Policies → Unity Catalog SQL

## Example 1: Simple Hive Table SELECT Grant

### Before (Ranger JSON)
```json
{
  "service": "hive",
  "name": "analytics_read_access",
  "policyType": 0,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["analytics"], "isExcludes": false},
    "table": {"values": ["orders"], "isExcludes": false},
    "column": {"values": ["*"], "isExcludes": false}
  },
  "policyItems": [
    {
      "accesses": [{"type": "select", "isAllowed": true}],
      "users": [],
      "groups": ["data_readers"],
      "delegateAdmin": false
    }
  ]
}
```

### After (Unity Catalog)
```sql
-- Source: Ranger policy "analytics_read_access"
GRANT SELECT ON TABLE main.analytics.orders TO `data_readers`;
```

## Example 2: Multi-Permission Grant with Groups

### Before (Ranger JSON)
```json
{
  "service": "hive",
  "name": "etl_write_access",
  "policyType": 0,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["staging"], "isExcludes": false},
    "table": {"values": ["*"], "isExcludes": false},
    "column": {"values": ["*"], "isExcludes": false}
  },
  "policyItems": [
    {
      "accesses": [
        {"type": "select", "isAllowed": true},
        {"type": "update", "isAllowed": true},
        {"type": "create", "isAllowed": true},
        {"type": "drop", "isAllowed": true},
        {"type": "alter", "isAllowed": true}
      ],
      "users": ["etl_service"],
      "groups": ["etl_team"],
      "delegateAdmin": false
    }
  ]
}
```

### After (Unity Catalog)
```sql
-- Source: Ranger policy "etl_write_access"
-- Wildcard table=* → grant on schema level
GRANT ALL PRIVILEGES ON SCHEMA main.staging TO `etl_service`;
GRANT ALL PRIVILEGES ON SCHEMA main.staging TO `etl_team`;
```

## Example 3: Deny Policy → REVOKE with Warning

### Before (Ranger JSON)
```json
{
  "service": "hive",
  "name": "deny_contractors_pii",
  "policyType": 0,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["hr"], "isExcludes": false},
    "table": {"values": ["employees"], "isExcludes": false},
    "column": {"values": ["*"], "isExcludes": false}
  },
  "denyPolicyItems": [
    {
      "accesses": [{"type": "select", "isAllowed": true}],
      "users": [],
      "groups": ["contractors"],
      "delegateAdmin": false
    }
  ]
}
```

### After (Unity Catalog)
```sql
-- Source: Ranger policy "deny_contractors_pii"
-- WARNING: UC uses default-deny. If 'contractors' does not have a GRANT,
-- they already cannot access this table. Only use REVOKE if a broader
-- grant (e.g., on the schema or catalog) gives them unintended access.
REVOKE SELECT ON TABLE main.hr.employees FROM `contractors`;
```

## Example 4: Delegated Admin → MANAGE Privilege

### Before (Ranger JSON)
```json
{
  "service": "hive",
  "name": "analytics_admin_delegation",
  "policyType": 0,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["analytics"], "isExcludes": false},
    "table": {"values": ["*"], "isExcludes": false},
    "column": {"values": ["*"], "isExcludes": false}
  },
  "policyItems": [
    {
      "accesses": [
        {"type": "select", "isAllowed": true},
        {"type": "update", "isAllowed": true},
        {"type": "create", "isAllowed": true},
        {"type": "drop", "isAllowed": true},
        {"type": "alter", "isAllowed": true}
      ],
      "users": [],
      "groups": ["analytics_admins"],
      "delegateAdmin": true
    }
  ]
}
```

### After (Unity Catalog)
```sql
-- Source: Ranger policy "analytics_admin_delegation"
-- delegateAdmin=true → MANAGE privilege (allows granting access to others)
GRANT MANAGE ON SCHEMA main.analytics TO `analytics_admins`;
```

## Example 5: Row-Level Filter

### Before (Ranger JSON)
```json
{
  "service": "hive",
  "name": "orders_region_filter",
  "policyType": 2,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["sales"]},
    "table": {"values": ["orders"]}
  },
  "rowFilterPolicyItems": [
    {
      "rowFilterInfo": {
        "filterExpr": "region = '{USER}'"
      },
      "accesses": [{"type": "select", "isAllowed": true}],
      "users": [],
      "groups": ["regional_managers"]
    },
    {
      "rowFilterInfo": {
        "filterExpr": "TRUE"
      },
      "accesses": [{"type": "select", "isAllowed": true}],
      "users": [],
      "groups": ["executives"]
    }
  ]
}
```

### After (Unity Catalog)
```sql
-- Source: Ranger policy "orders_region_filter"

-- Step 1: Create the row filter function
CREATE OR REPLACE FUNCTION main.sales.orders_row_filter(region STRING)
RETURNS BOOLEAN
RETURN (
  is_member('executives')  -- executives see all rows
  OR (is_member('regional_managers') AND region = current_user())
);

-- Step 2: Apply the filter
ALTER TABLE main.sales.orders SET ROW FILTER main.sales.orders_row_filter ON (region);

-- Step 3: Grant SELECT (row filter enforces visibility)
GRANT SELECT ON TABLE main.sales.orders TO `regional_managers`;
GRANT SELECT ON TABLE main.sales.orders TO `executives`;
```

## Example 6: Column Masking

### Before (Ranger JSON)
```json
{
  "service": "hive",
  "name": "mask_employee_ssn",
  "policyType": 1,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["hr"]},
    "table": {"values": ["employees"]},
    "column": {"values": ["ssn"]}
  },
  "dataMaskPolicyItems": [
    {
      "dataMaskInfo": {
        "dataMaskType": "MASK_SHOW_LAST_4"
      },
      "accesses": [{"type": "select", "isAllowed": true}],
      "users": [],
      "groups": ["hr_viewers"]
    },
    {
      "dataMaskInfo": {
        "dataMaskType": "MASK_NONE"
      },
      "accesses": [{"type": "select", "isAllowed": true}],
      "users": [],
      "groups": ["hr_admins"]
    }
  ]
}
```

### After (Unity Catalog)
```sql
-- Source: Ranger policy "mask_employee_ssn"

-- Step 1: Create the mask function
CREATE OR REPLACE FUNCTION main.hr.mask_employees_ssn(ssn STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('hr_admins') THEN ssn
  WHEN is_member('hr_viewers') THEN concat(repeat('X', greatest(length(ssn) - 4, 0)), right(ssn, 4))
  ELSE NULL  -- default: fully masked for all other groups
END;

-- Step 2: Apply the mask
ALTER TABLE main.hr.employees ALTER COLUMN ssn SET MASK main.hr.mask_employees_ssn;
```

## Example 7: Wildcard Resource Expansion

### Before (Ranger JSON)
```json
{
  "service": "hive",
  "name": "all_databases_read",
  "policyType": 0,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["*"], "isExcludes": false},
    "table": {"values": ["*"], "isExcludes": false},
    "column": {"values": ["*"], "isExcludes": false}
  },
  "policyItems": [
    {
      "accesses": [{"type": "select", "isAllowed": true}],
      "users": [],
      "groups": ["all_readers"],
      "delegateAdmin": false
    }
  ]
}
```

### After (Unity Catalog)
```sql
-- Source: Ranger policy "all_databases_read"
-- database=*, table=*, column=* → catalog-level grant
GRANT SELECT ON CATALOG main TO `all_readers`;
-- Note: This grants SELECT on ALL schemas, tables, and views in the catalog.
-- Review whether this broad access is appropriate for your UC security model.
```

## Example 8: HDFS Policy → External Location Grant

### Before (Ranger JSON)
```json
{
  "service": "hdfs",
  "name": "raw_data_access",
  "policyType": 0,
  "isEnabled": true,
  "resources": {
    "path": {"values": ["/data/raw/events"], "isRecursive": true}
  },
  "policyItems": [
    {
      "accesses": [
        {"type": "read", "isAllowed": true},
        {"type": "write", "isAllowed": true}
      ],
      "users": ["etl_service"],
      "groups": ["data_engineers"],
      "delegateAdmin": false
    }
  ]
}
```

### After (Unity Catalog)
```sql
-- Source: Ranger policy "raw_data_access"

-- Prerequisite: Create the external location (adjust URL for your cloud provider)
-- CREATE EXTERNAL LOCATION raw_events_location
--   URL 's3://my-bucket/data/raw/events'
--   WITH (STORAGE CREDENTIAL my_credential);

-- TODO: Replace 'raw_events_location' with your actual external location name
GRANT READ FILES ON EXTERNAL LOCATION `raw_events_location` TO `etl_service`;
GRANT WRITE FILES ON EXTERNAL LOCATION `raw_events_location` TO `etl_service`;
GRANT READ FILES ON EXTERNAL LOCATION `raw_events_location` TO `data_engineers`;
GRANT WRITE FILES ON EXTERNAL LOCATION `raw_events_location` TO `data_engineers`;
```

## Example 9: Column-Level Grant

### Before (Ranger JSON)
```json
{
  "service": "hive",
  "name": "limited_employee_access",
  "policyType": 0,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["hr"]},
    "table": {"values": ["employees"]},
    "column": {"values": ["name", "department", "title"]}
  },
  "policyItems": [
    {
      "accesses": [{"type": "select", "isAllowed": true}],
      "users": [],
      "groups": ["general_staff"],
      "delegateAdmin": false
    }
  ]
}
```

### After (Unity Catalog)
```sql
-- Source: Ranger policy "limited_employee_access"
-- Column-level SELECT: only name, department, title columns
GRANT SELECT (name, department, title) ON TABLE main.hr.employees TO `general_staff`;
-- Note: UC column-level grants only support SELECT. Other operations
-- (MODIFY, etc.) cannot be restricted to specific columns.
```

## Example 10: Tag-Based Policy → Expanded Resource Grants

### Before (Ranger JSON)
```json
{
  "service": "tag",
  "name": "pii_masking_policy",
  "policyType": 1,
  "isEnabled": true,
  "resources": {
    "tag": {"values": ["PII"]}
  },
  "dataMaskPolicyItems": [
    {
      "dataMaskInfo": {
        "dataMaskType": "MASK_HASH"
      },
      "accesses": [{"type": "select", "isAllowed": true}],
      "users": [],
      "groups": ["analysts"]
    },
    {
      "dataMaskInfo": {
        "dataMaskType": "MASK_NONE"
      },
      "accesses": [{"type": "select", "isAllowed": true}],
      "users": [],
      "groups": ["data_owners"]
    }
  ]
}
```

### After (Unity Catalog)
```sql
-- Source: Ranger tag-based policy "pii_masking_policy"
-- WARNING: Tag-based policy expanded to resource-based masks.
-- You must enumerate all resources tagged with 'PII' in Atlas/Ranger.
-- If new columns are tagged in the future, masks must be manually added.

-- Example: assuming PII-tagged columns are hr.employees.ssn, hr.employees.dob,
-- finance.payroll.salary

-- Mask function for string PII columns
CREATE OR REPLACE FUNCTION main.hr.mask_pii_string(col STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('data_owners') THEN col
  WHEN is_member('analysts') THEN sha2(col, 256)
  ELSE NULL
END;

-- Mask function for numeric PII columns
CREATE OR REPLACE FUNCTION main.finance.mask_pii_decimal(col DECIMAL(10,2))
RETURNS DECIMAL(10,2)
RETURN CASE
  WHEN is_member('data_owners') THEN col
  ELSE NULL
END;

-- Apply masks to all PII-tagged columns
ALTER TABLE main.hr.employees ALTER COLUMN ssn SET MASK main.hr.mask_pii_string;
ALTER TABLE main.hr.employees ALTER COLUMN dob SET MASK main.hr.mask_pii_string;
ALTER TABLE main.finance.payroll ALTER COLUMN salary SET MASK main.finance.mask_pii_decimal;
```
