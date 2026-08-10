# Edge Cases & Limitations: Ranger → Unity Catalog

## Unsupported Policy Types

These Ranger policy types have no Unity Catalog equivalent. Output documentation-only comments:

| Ranger Policy Type | Why Unsupported | Recommendation |
|-------------------|-----------------|----------------|
| HBase policies | No HBase in Databricks | If migrated to Delta, create new grants on the Delta tables |
| Kafka policies | UC doesn't govern Kafka | Use Kafka ACLs or cloud IAM for streaming sources |
| YARN queue policies | No YARN in Databricks | Use Databricks cluster policies and access control |
| Solr policies | No Solr in Databricks | Not applicable |
| Knox policies | Gateway-level auth | Use Databricks workspace IP access lists |
| Atlas policies | Tag governance | Use UC tags + tag-based access control (see Tag-Based below) |

### Output format for unsupported policies:

```sql
-- ============================================================
-- UNSUPPORTED: HBase policy "hbase_read_access"
-- Service: hbase
-- Resources: table=customer_data, column-family=personal
-- Principals: group=data_readers
-- ============================================================
-- This policy type has no Unity Catalog equivalent.
-- Action required: If HBase data was migrated to Delta tables,
-- create new GRANT statements for the target Delta tables.
-- ============================================================
```

## Deny Policies

Unity Catalog uses a default-deny model — principals have no access unless explicitly granted. Ranger's explicit deny is handled differently:

### Simple Deny

```json
{
  "denyPolicyItems": [
    {
      "accesses": [{"type": "select", "isAllowed": true}],
      "groups": ["contractors"]
    }
  ]
}
```

Converts to:

```sql
-- WARNING: Ranger explicit deny for group 'contractors' on catalog.schema.table
-- UC uses default-deny: if you do not GRANT access, it is implicitly denied.
-- If 'contractors' inherits access from a parent grant, use REVOKE:
REVOKE SELECT ON TABLE catalog.schema.table FROM `contractors`;
-- Review: Ensure 'contractors' does not receive access via broader grants.
```

### Deny with Exceptions

```json
{
  "denyPolicyItems": [
    {
      "accesses": [{"type": "select", "isAllowed": true}],
      "groups": ["external_users"],
      "delegateAdmin": false
    }
  ],
  "denyExceptions": [
    {
      "accesses": [{"type": "select", "isAllowed": true}],
      "users": ["trusted_partner@company.com"]
    }
  ]
}
```

Converts to:

```sql
-- WARNING: Deny with exception — requires manual review
-- Ranger denies SELECT to 'external_users' EXCEPT 'trusted_partner@company.com'
-- UC approach: Do not grant to the group, only grant to the exception user
REVOKE SELECT ON TABLE catalog.schema.table FROM `external_users`;
GRANT SELECT ON TABLE catalog.schema.table TO `trusted_partner@company.com`;
-- Review: Verify group membership and inheritance to ensure correct access.
```

## IP-Based and Time-Based Conditions

Ranger supports conditions like IP ranges and time windows. UC does not support these at the grant level.

```sql
-- WARNING: Ranger policy "office_hours_only" has a time-based condition:
--   Condition: _CTX_accessTime >= '09:00' AND _CTX_accessTime <= '17:00'
-- Unity Catalog does not support time-based grant conditions.
-- Alternative approaches:
--   1. Use workspace-level IP access lists for network-based restrictions
--   2. Implement time checks in row filter functions (partial solution)
--   3. Use cluster policies to restrict compute access by time
--   4. Use audit logs + alerts for policy compliance monitoring
```

## Tag-Based Policies

Ranger tag-based policies apply to resources tagged in Atlas. UC has limited tag support:

### Simple Tag Expansion

If you know the tagged resources, expand to resource-based grants:

```sql
-- Ranger tag-based policy: tag=PII, permission=select, group=pii_readers
-- Tagged resources: hr.employees.ssn, hr.employees.dob, finance.payroll.salary
-- Expanded to resource-based grants:
GRANT SELECT (ssn) ON TABLE main.hr.employees TO `pii_readers`;
GRANT SELECT (dob) ON TABLE main.hr.employees TO `pii_readers`;
GRANT SELECT (salary) ON TABLE main.finance.payroll TO `pii_readers`;

-- WARNING: Tag-based policy expanded to resource-based grants.
-- If new resources are tagged with 'PII' in the future, grants must be manually added.
-- Consider using UC column masks for PII columns instead of SELECT grants.
```

### Tag-Based Masking

Tag-based column masks in Ranger should be converted per-column:

```sql
-- Ranger: tag=PII → MASK_HASH for all tagged columns
-- Must be expanded per table/column:
ALTER TABLE main.hr.employees ALTER COLUMN ssn SET MASK main.hr.mask_pii_hash;
ALTER TABLE main.hr.employees ALTER COLUMN dob SET MASK main.hr.mask_pii_hash;
ALTER TABLE main.finance.payroll ALTER COLUMN salary SET MASK main.finance.mask_pii_hash;
```

## Wildcard Patterns

### Simple Wildcards (Supported)

| Pattern | Handling |
|---------|----------|
| `database=*` | Grant on catalog |
| `table=*` | Grant on schema |
| `column=*` | Grant on table |

### Regex/Glob Wildcards (Manual Review Required)

| Pattern | Handling |
|---------|----------|
| `database=prod_*` | Enumerate matching schemas manually |
| `table=tmp_*` | Enumerate matching tables manually |
| `table=*_staging` | Enumerate matching tables manually |

```sql
-- WARNING: Pattern wildcard 'table=tmp_*' cannot be directly mapped to UC.
-- UC does not support pattern-based grants.
-- Enumerate matching tables and grant individually:
-- GRANT SELECT ON TABLE main.analytics.tmp_daily TO `group`;
-- GRANT SELECT ON TABLE main.analytics.tmp_weekly TO `group`;
-- Or grant on the entire schema if appropriate:
-- GRANT SELECT ON SCHEMA main.analytics TO `group`;
```

## Recursive / Cascading Differences

### Ranger Recursive Flag

HDFS policies have `"isRecursive": true` for path-based access:

```sql
-- Ranger: path=/data/warehouse, isRecursive=true
-- UC: External locations are inherently recursive
GRANT READ FILES ON EXTERNAL LOCATION `warehouse_location` TO `data_readers`;
-- Note: This grants access to all files under the external location path
```

### Ranger Non-Recursive

```sql
-- Ranger: path=/data/landing, isRecursive=false
-- WARNING: UC external locations are always recursive.
-- There is no way to grant access to a single directory level without subdirectories.
-- Consider using separate external locations for fine-grained path control.
```

## Service-Level Policies

Ranger has service-level defaults (default policy ID 0) that apply to all resources:

```sql
-- WARNING: Ranger default policy (policy ID 0) for service 'hive'
-- grants 'all' to groups: admin, hive
-- In UC, use catalog-level grants for equivalent broad access:
GRANT ALL PRIVILEGES ON CATALOG main TO `admin`;
-- Note: The 'hive' service account typically does not need UC grants.
-- Databricks uses workspace admins and service principals instead.
```

## Multiple Services in Single Export

A Ranger policy export may contain policies from multiple services (hive, hdfs, hbase, kafka). Process each service type separately:

1. Extract and convert `hive` service policies → GRANT/REVOKE on UC objects
2. Extract and convert `hdfs` service policies → EXTERNAL LOCATION grants
3. Flag `hbase`, `kafka`, `yarn`, etc. as unsupported with documentation

## Empty or Disabled Policies

```sql
-- Ranger policy "old_access_rule" is disabled (isEnabled=false)
-- Skipping conversion. No UC equivalent needed.
```

Skip policies where `"isEnabled": false`. Document them as comments only if the user requests a complete audit trail.
