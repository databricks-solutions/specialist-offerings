# Permission Mapping: Ranger → Unity Catalog

## Core Permission Mappings

| Ranger Permission | UC Privilege | Applicable Securables | Notes |
|-------------------|-------------|----------------------|-------|
| `select` | `SELECT` | TABLE, VIEW | Direct 1:1 mapping |
| `update` | `MODIFY` | TABLE | UC MODIFY covers insert, update, delete |
| `create` (on database) | `CREATE SCHEMA` | CATALOG | Ranger database create → UC schema creation |
| `create` (on table) | `CREATE TABLE` | SCHEMA | Create table within a schema |
| `drop` | `MANAGE` | TABLE, SCHEMA | UC has no DROP-only privilege; MANAGE includes drop |
| `alter` | `MODIFY` | TABLE | ALTER is part of MODIFY in UC |
| `index` | — | — | No index concept in Delta Lake; omit with warning |
| `lock` | — | — | Delta handles concurrency natively; omit |
| `all` | `ALL PRIVILEGES` | varies | Maps to ALL PRIVILEGES on the target securable |
| `read` (HDFS) | `READ FILES` | EXTERNAL LOCATION | HDFS read → external location read |
| `write` (HDFS) | `WRITE FILES` | EXTERNAL LOCATION | HDFS write → external location write |
| `execute` (HDFS) | — | — | No equivalent in UC; omit with warning |
| `execute` (UDF) | `EXECUTE` | FUNCTION | Grant execute on a UC function |
| `delegateAdmin` | `MANAGE` | varies | Admin delegation → MANAGE on target securable |
| `repladmin` | — | — | Replication admin has no UC equivalent; omit |
| `serviceadmin` | — | — | Service admin has no UC equivalent; omit |

## Compound Permission Expansion

When Ranger grants multiple permissions in a single policy item, expand to multiple GRANT statements:

```json
{
  "accesses": [
    {"type": "select", "isAllowed": true},
    {"type": "update", "isAllowed": true},
    {"type": "create", "isAllowed": true}
  ]
}
```

Converts to:

```sql
GRANT SELECT ON TABLE catalog.schema.table TO `group_name`;
GRANT MODIFY ON TABLE catalog.schema.table TO `group_name`;
-- Note: 'create' on table-level resource is not applicable; create applies at schema level
```

Alternatively, if all core permissions are granted, consolidate:

```sql
-- If select + update + create + drop + alter are all granted:
GRANT ALL PRIVILEGES ON TABLE catalog.schema.table TO `group_name`;
```

## Principal Mapping

| Ranger Principal | UC Principal | Syntax |
|-----------------|-------------|--------|
| User | User | `` `user@domain.com` `` |
| Group | Group | `` `group_name` `` |
| Role | — | Expand role members to users/groups, or use UC group |
| `{OWNER}` | — | Implicit in UC; table owner always has full access |
| `public` | — | `GRANT ... TO ALL USERS` (use with caution) |

## Permission Precedence

Ranger evaluates in this order:
1. Deny with exceptions
2. Deny
3. Allow with exceptions
4. Allow

UC uses a simpler model — default-deny with explicit grants. When converting:

- **Allow** → `GRANT`
- **Deny** → `REVOKE` + warning comment (since UC is default-deny, explicit deny is often unnecessary)
- **Allow with exceptions** → `GRANT` to the group, then note the exception for manual review
- **Deny with exceptions** → `REVOKE` + note exceptions for manual review
