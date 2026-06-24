# SDP Demo: Tag-Driven Expectations from a Rules Table

A Databricks Spark Declarative Pipelines (SDP) demo showing **data-quality expectations derived dynamically from a rules table, filtered by UC column tags on the target table**.

The big idea: a data steward can change DQ behavior by editing the `dq_rules` table or applying/removing UC column tags — **no pipeline code changes**.

## Architecture

```
alexn.sdp_demo
├── files (volume)            policies/ + claims/ raw JSON
├── dq_rules                  generic rules with {column} placeholder
├── bronze_policies/claims    Auto Loader streaming tables
├── silver_policies/claims    tag-derived warn-level expectations
├── silver_*_quarantine       rows violating any rule
└── gold_policy_claims_summary  materialized view
```

## Prerequisites

- Databricks CLI ≥ 0.250
- `~/.databrickscfg` DEFAULT profile authenticated to `adb-984752964297111.11.azuredatabricks.net`
- `alexn` catalog exists in that workspace
- Python ≥ 3.11 with `uv` (or another package manager that respects `pyproject.toml`)

## Setup (one-time)

```bash
cd /Users/alex.nastetsky/claude/selective/sdp_demo

# 1. Schema + volume
databricks sql query --warehouse-id <WID> --query "$(cat setup/01_create_schema_and_volume.sql)"
# or paste setup/01_create_schema_and_volume.sql into a SQL editor

# 2. Rules table
databricks sql query --warehouse-id <WID> --query "$(cat setup/02_create_rules_table.sql)"

# 3. Synthetic data into UC volume
uv run python setup/03_generate_raw_data.py
```

## Phase 1 — Pipeline without tags

```bash
databricks bundle deploy -t dev
databricks bundle run sdp_demo_pipeline -t dev
```

Verify:
```sql
SELECT count(*) FROM alexn.sdp_demo.silver_policies;             -- ~3000
SELECT count(*) FROM alexn.sdp_demo.silver_policies_quarantine;  -- 0
SELECT count(*) FROM alexn.sdp_demo.silver_claims;               -- ~15000
SELECT count(*) FROM alexn.sdp_demo.silver_claims_quarantine;    -- 0
```
The pipeline event log shows no expectations.

## Phase 2 — Apply UC column tags

```bash
databricks sql query --warehouse-id <WID> --query "$(cat setup/04_apply_column_tags.sql)"
```

Verify:
```sql
SELECT table_name, column_name, tag_name
FROM system.information_schema.column_tags
WHERE catalog_name='alexn' AND schema_name='sdp_demo'
ORDER BY table_name, column_name;
```

## Phase 3 — Re-run with full refresh

```bash
databricks bundle run sdp_demo_pipeline -t dev \
  --full-refresh bronze_policies,bronze_claims,silver_policies,silver_policies_quarantine,silver_claims,silver_claims_quarantine
```

> If your Databricks CLI bundles an older Terraform binary that fails with `openpgp: key expired`, set:
> ```
> export DATABRICKS_TF_EXEC_PATH=$(which terraform)
> export DATABRICKS_TF_VERSION=$(terraform version | head -1 | awk '{print $2}' | sed 's/v//')
> ```

Verify tags survived:
```sql
SELECT count(*) FROM system.information_schema.column_tags
WHERE catalog_name='alexn' AND schema_name='sdp_demo';   -- ≥ 16
```
If tags were lost (shouldn't happen — UC column tags persist across full refresh), re-run `setup/04_apply_column_tags.sql` (idempotent) and re-run the pipeline.

Verify expectations active:
```sql
SELECT count(*) FROM alexn.sdp_demo.silver_policies;             -- still ~3000 (warn, rows flow through)
SELECT count(*) FROM alexn.sdp_demo.silver_policies_quarantine;  -- ~hundreds
SELECT * FROM alexn.sdp_demo.silver_policies_quarantine LIMIT 20;
```
The pipeline UI now shows per-expectation pass/fail counts on each silver node.

## Phase 4 (demo) — rule-driven change without code change

- Edit a rule in `alexn.sdp_demo.dq_rules` (e.g. tighten the email regex), full-refresh, see new violations.
- Add a new tag to a column, full-refresh, the new expectation appears.

## Phase 5 (optional) — streaming behavior with tags in place

```bash
uv run python setup/03b_generate_more_raw_data.py
databricks bundle run sdp_demo_pipeline -t dev      # no full refresh
```
Auto Loader picks up the new batch; new rows flow through tag-derived expectations.

## How it works

`src/sdp_demo/transformations/_rules_helper.py:get_expectations(table_name)` runs at **module load time** (when the pipeline starts). It joins `system.information_schema.column_tags` against `alexn.sdp_demo.dq_rules`, replaces the `{column}` placeholder in each `expression_template`, and returns `(expectations_dict, quarantine_predicate)`.

The expectations dict is passed to `@dp.expect_all(...)`. The quarantine predicate is `OR`-combined NOT-of-each-expectation, so the quarantine stream filters to rows that fail at least one rule.

On the first run (no tags), the dict is empty; the decorator is skipped and the quarantine predicate is `"false"` — quarantine table is created empty. After tags are applied, the next run picks them up automatically.
