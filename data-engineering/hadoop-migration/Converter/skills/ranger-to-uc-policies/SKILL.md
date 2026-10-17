---
name: ranger-to-uc-policies
description: "Convert Apache Ranger policies to Unity Catalog grants. Triggers on: convert Ranger policies, migrate Ranger to UC, Ranger to Unity Catalog, Ranger ACL migration, Ranger security to Databricks"
version: 1.0.0
---

# Ranger Policies to Unity Catalog Converter

Convert Apache Ranger access control policies to Unity Catalog GRANT/REVOKE statements, row filter UDFs, and column mask UDFs.

## When to Use

- Converting Ranger policy JSON exports to Unity Catalog SQL grants
- Migrating Hive/HDFS resource-based policies to UC privileges
- Converting Ranger row-level filters to UC row filter functions
- Converting Ranger column masking policies to UC column mask functions
- Mapping Ranger admin delegation to UC MANAGE privileges

## Instructions

When given Ranger policy JSON to convert:

1. **Read references** for detailed rules:
   - `references/PERMISSION_MAPPING.md` — Ranger permissions to UC privileges
   - `references/RESOURCE_MAPPING.md` — Ranger resources to UC securables
   - `references/ROW_FILTER_COLUMN_MASK.md` — Row filter and column mask transformations
   - `references/EDGE_CASES.md` — Limitations, unsupported patterns, gotchas
   - `references/EXAMPLES.md` — Before/after examples (Ranger JSON → UC SQL)

2. **Classify the policy type:**
   - **Resource-based (Hive)** — database, table, column, UDF policies → GRANT/REVOKE
   - **Resource-based (HDFS)** — path policies → EXTERNAL LOCATION grants
   - **Row filter** — `rowFilterPolicyItems` → CREATE FUNCTION + ALTER TABLE SET ROW FILTER
   - **Column mask** — `dataMaskPolicyItems` → CREATE FUNCTION + ALTER TABLE SET COLUMN MASK
   - **Tag-based** — expand to resource-based grants with warnings
   - **Deny policy** — convert to REVOKE + warning (UC is default-deny)

3. **Apply transformations** in this order:
   a. Parse the Ranger policy JSON structure
   b. Map Ranger resources to UC securables (prepend catalog name)
   c. Map Ranger permissions to UC privileges
   d. Map Ranger users/groups to UC principals
   e. Handle wildcards — expand `*` resources or flag for review
   f. Convert row filters and column masks to UDF definitions
   g. Convert deny policies to REVOKE statements with warning comments
   h. Flag `delegateAdmin` as MANAGE privilege

4. **Output** the converted SQL with:
   - GRANT/REVOKE statements
   - UDF definitions for row filters and column masks
   - ALTER TABLE statements for applying row filters and column masks
   - Inline comments explaining each transformation
   - `-- WARNING:` comments for items needing manual review

5. **Flag** any constructs that need manual review:
   - HBase, Kafka, YARN queue policies (no UC equivalent)
   - IP-based or time-based conditions
   - Tag-based policies (partial support)
   - Group inheritance conflicts from deny policies
   - Wildcard resources that cannot be expanded

## Supported Policy Types

| Policy Type | Status | Notes |
|-------------|--------|-------|
| Hive resource (database/table/column) | Supported | Full GRANT/REVOKE mapping |
| Hive UDF access | Supported | Maps to GRANT EXECUTE ON FUNCTION |
| HDFS path access | Supported | Maps to EXTERNAL LOCATION grants |
| Row-level filter | Supported | Maps to UC row filter functions |
| Column masking | Supported | Maps to UC column mask functions |
| Deny policies | Partial | REVOKE + warning (UC is default-deny) |
| Delegated admin | Supported | Maps to MANAGE privilege |
| Tag-based policies | Partial | Expanded to resource grants with warnings |
| HBase policies | Unsupported | Documented only |
| Kafka policies | Unsupported | Documented only |
| YARN queue policies | Unsupported | Documented only |

## Output Format

```sql
-- ============================================================
-- Unity Catalog Grants
-- Converted from: <policy-name>
-- Source: Apache Ranger policy export
-- Date: <conversion-date>
-- ============================================================

-- Grant statements
GRANT SELECT ON TABLE catalog.schema.table TO `group_name`;

-- Row filter UDFs (if applicable)
CREATE OR REPLACE FUNCTION catalog.schema.row_filter_fn(region STRING)
RETURNS BOOLEAN
RETURN region = current_user();

ALTER TABLE catalog.schema.table SET ROW FILTER catalog.schema.row_filter_fn ON (region);

-- Column mask UDFs (if applicable)
CREATE OR REPLACE FUNCTION catalog.schema.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE WHEN is_member('admin_group') THEN ssn ELSE concat('XXX-XX-', right(ssn, 4)) END;

ALTER TABLE catalog.schema.table ALTER COLUMN ssn SET MASK catalog.schema.mask_ssn;

-- WARNING: The following policies have no direct UC equivalent:
-- - HBase policy "hbase_read" → Manual review required
-- - Time-based condition on policy "restricted_hours" → Use workspace IP access lists
```
