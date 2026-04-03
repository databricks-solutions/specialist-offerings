# Configurable Parameters

These parameters control how Hive DDL is converted to Unity Catalog DDL. Each can be specified via invocation args or collected interactively via `AskUserQuestion`.

## Parameters

| # | Parameter | Arg keywords | Options | Default |
|---|-----------|-------------|---------|---------|
| 1 | **Target catalog** | `catalog` | Any valid UC catalog name | `main` |
| 2 | **Target schema** | `schema` | Any valid UC schema name, or auto-derive from the Hive database name | Auto-derive from Hive DB name |
| 3 | **Table format** | `format` | `DELTA`, `ICEBERG` | `DELTA` |
| 4 | **Table naming convention** | `suffix`, `prefix` | No change / add suffix (e.g. `_ice`) / add prefix (e.g. `uc_`) | No change |
| 5 | **Clustering strategy** | `clustering` | `liquid` (convert `PARTITIONED BY` to `CLUSTER BY`) / `preserve` (keep `PARTITIONED BY` as-is) | `liquid` |

## Arg Detection Examples

The agent should parse invocation args for these patterns:

- `catalog aa_catalog` → target catalog = `aa_catalog`
- `schema bny` → target schema = `bny`
- `format iceberg` or `use Iceberg` → table format = `ICEBERG`
- `suffix _ice` → table naming = suffix `_ice`
- `prefix uc_` → table naming = prefix `uc_`
- `clustering preserve` or `keep partitions` → clustering = `preserve`

## Interactive Prompt Questions

When parameters are missing, prompt using `AskUserQuestion` with up to 4 questions (only for parameters not already specified). Combine catalog and schema into a single question.

### Question 1 — Target catalog & schema

> Which Unity Catalog namespace should the tables be created in?

| Option | Description |
|--------|-------------|
| `main` (default catalog, auto-derive schema) | Uses `main` catalog; schema name derived from Hive database name |
| Custom (specify catalog and/or schema) | You'll provide custom catalog/schema names |

If the user selects "Custom", follow up to collect the specific names.

### Question 2 — Table format

> Which table format should be used?

| Option | Description |
|--------|-------------|
| Delta (Recommended) | Standard Databricks format with full feature support |
| Iceberg | UniForm Iceberg tables; adds required TBLPROPERTIES for clustering compatibility |

### Question 3 — Table naming convention

> How should the converted tables be named?

| Option | Description |
|--------|-------------|
| Keep original names (Recommended) | Table names remain unchanged from Hive |
| Add suffix (e.g. `_ice`, `_uc`) | Append a suffix to each table name |
| Add prefix (e.g. `uc_`, `delta_`) | Prepend a prefix to each table name |

If suffix or prefix is selected, follow up to collect the specific string.

### Question 4 — Clustering strategy

> How should partitioned tables be handled?

| Option | Description |
|--------|-------------|
| Convert to Liquid Clustering (Recommended) | Converts `PARTITIONED BY` to `CLUSTER BY` for optimized data layout |
| Preserve as PARTITIONED BY | Keeps the original Hive-style partitioning scheme |

## Iceberg + Liquid Clustering Interaction

When **format = ICEBERG** and **clustering = liquid**, the converter must automatically add these TBLPROPERTIES to any table with `CLUSTER BY`:

```sql
TBLPROPERTIES (
  'delta.enableDeletionVectors' = false,
  'delta.enableRowTracking' = false
)
```

This is required because the Iceberg v2 spec does not support deletion vectors or row tracking, which are normally needed for Liquid Clustering concurrency control.
