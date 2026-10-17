# Row Filter & Column Mask: Ranger → Unity Catalog

## Row-Level Filters

### Ranger Row Filter Structure

In Ranger, row filters are defined in `rowFilterPolicyItems` within a policy:

```json
{
  "service": "hive",
  "name": "orders_row_filter",
  "resources": {
    "database": {"values": ["analytics"]},
    "table": {"values": ["orders"]}
  },
  "rowFilterPolicyItems": [
    {
      "rowFilterInfo": {
        "filterExpr": "region = '{USER}'"
      },
      "accesses": [{"type": "select", "isAllowed": true}],
      "users": [],
      "groups": ["regional_analysts"]
    }
  ]
}
```

### UC Row Filter Conversion

Each Ranger row filter becomes a UC function + ALTER TABLE:

```sql
-- Step 1: Create the filter function
CREATE OR REPLACE FUNCTION main.analytics.orders_row_filter(region STRING)
RETURNS BOOLEAN
RETURN (
  is_member('admin_group')  -- admins see all rows
  OR region = current_user()  -- Ranger {USER} → current_user()
);

-- Step 2: Apply the filter to the table
ALTER TABLE main.analytics.orders SET ROW FILTER main.analytics.orders_row_filter ON (region);
```

### Ranger Variable Substitution

| Ranger Variable | UC Equivalent | Example |
|----------------|---------------|---------|
| `{USER}` | `current_user()` | `region = current_user()` |
| `{OWNER}` | — | No direct equivalent; use `is_account_group_member()` |
| Group membership check | `is_member('group')` | `is_member('regional_analysts')` |

### Multi-Group Row Filters

When multiple groups have different filter expressions:

```json
{
  "rowFilterPolicyItems": [
    {
      "rowFilterInfo": {"filterExpr": "region = 'US'"},
      "groups": ["us_team"]
    },
    {
      "rowFilterInfo": {"filterExpr": "region = 'EU'"},
      "groups": ["eu_team"]
    },
    {
      "rowFilterInfo": {"filterExpr": "TRUE"},
      "groups": ["admin"]
    }
  ]
}
```

Converts to a single UC function that combines all conditions:

```sql
CREATE OR REPLACE FUNCTION main.analytics.orders_row_filter(region STRING)
RETURNS BOOLEAN
RETURN (
  (is_member('admin') AND TRUE)
  OR (is_member('us_team') AND region = 'US')
  OR (is_member('eu_team') AND region = 'EU')
);

ALTER TABLE main.analytics.orders SET ROW FILTER main.analytics.orders_row_filter ON (region);
```

## Column Masking

### Ranger Column Mask Structure

```json
{
  "service": "hive",
  "name": "mask_ssn",
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
    }
  ]
}
```

### Ranger Mask Type → UC Implementation

| Ranger Mask Type | UC Function Body | Input Type |
|-----------------|-----------------|------------|
| `MASK` | `regexp_replace(CAST(col AS STRING), '.', 'X')` | STRING |
| `MASK_SHOW_LAST_4` | `concat(repeat('X', greatest(length(col) - 4, 0)), right(col, 4))` | STRING |
| `MASK_SHOW_FIRST_4` | `concat(left(col, 4), repeat('X', greatest(length(col) - 4, 0)))` | STRING |
| `MASK_HASH` | `sha2(CAST(col AS STRING), 256)` | STRING |
| `MASK_NULL` | `NULL` | any |
| `MASK_NONE` | `col` (passthrough) | any |
| `MASK_DATE_SHOW_YEAR` | `date_trunc('YEAR', col)` | DATE/TIMESTAMP |
| `CUSTOM` | Use the custom expression directly | varies |

### UC Column Mask Conversion

Each Ranger column mask becomes a UC function + ALTER TABLE:

```sql
-- Step 1: Create the mask function
CREATE OR REPLACE FUNCTION main.hr.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('hr_admins') THEN ssn  -- unmasked for admins
  ELSE concat(repeat('X', greatest(length(ssn) - 4, 0)), right(ssn, 4))
END;

-- Step 2: Apply the mask to the column
ALTER TABLE main.hr.employees ALTER COLUMN ssn SET MASK main.hr.mask_ssn;
```

### Multi-Group Column Masks

When different groups see different mask levels:

```json
{
  "dataMaskPolicyItems": [
    {
      "dataMaskInfo": {"dataMaskType": "MASK_NONE"},
      "groups": ["hr_admins"]
    },
    {
      "dataMaskInfo": {"dataMaskType": "MASK_SHOW_LAST_4"},
      "groups": ["hr_viewers"]
    },
    {
      "dataMaskInfo": {"dataMaskType": "MASK_HASH"},
      "groups": ["auditors"]
    }
  ]
}
```

Converts to a single UC function with group-based branching:

```sql
CREATE OR REPLACE FUNCTION main.hr.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('hr_admins') THEN ssn
  WHEN is_member('hr_viewers') THEN concat(repeat('X', greatest(length(ssn) - 4, 0)), right(ssn, 4))
  WHEN is_member('auditors') THEN sha2(ssn, 256)
  ELSE NULL  -- default: fully masked
END;

ALTER TABLE main.hr.employees ALTER COLUMN ssn SET MASK main.hr.mask_ssn;
```

### Multiple Columns on Same Table

Create a separate mask function per column:

```sql
-- Mask for SSN column
CREATE OR REPLACE FUNCTION main.hr.mask_employees_ssn(ssn STRING)
RETURNS STRING
RETURN CASE WHEN is_member('hr_admins') THEN ssn ELSE concat(repeat('X', greatest(length(ssn) - 4, 0)), right(ssn, 4)) END;

-- Mask for salary column
CREATE OR REPLACE FUNCTION main.hr.mask_employees_salary(salary DECIMAL(10,2))
RETURNS DECIMAL(10,2)
RETURN CASE WHEN is_member('hr_admins') THEN salary ELSE NULL END;

-- Apply both masks
ALTER TABLE main.hr.employees ALTER COLUMN ssn SET MASK main.hr.mask_employees_ssn;
ALTER TABLE main.hr.employees ALTER COLUMN salary SET MASK main.hr.mask_employees_salary;
```

## Naming Conventions

| Object | Naming Pattern | Example |
|--------|---------------|---------|
| Row filter function | `<schema>.<table>_row_filter` | `main.analytics.orders_row_filter` |
| Column mask function | `<schema>.mask_<table>_<column>` | `main.hr.mask_employees_ssn` |
| Multi-policy function | `<schema>.<policy_name>` | `main.analytics.region_access_filter` |

## Removing Filters and Masks

If a Ranger policy is disabled or being decommissioned:

```sql
-- Remove row filter
ALTER TABLE main.analytics.orders DROP ROW FILTER;

-- Remove column mask
ALTER TABLE main.hr.employees ALTER COLUMN ssn DROP MASK;

-- Optionally drop the functions
DROP FUNCTION IF EXISTS main.analytics.orders_row_filter;
DROP FUNCTION IF EXISTS main.hr.mask_employees_ssn;
```
