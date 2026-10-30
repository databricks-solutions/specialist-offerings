# h2mi: HDFS / Hive Metastore -> Unity Catalog managed Iceberg

`h2mi` migrates an on-prem Hadoop estate (HDFS + Hive Metastore, holding Hive tables and OSS Iceberg tables) into
Databricks Unity Catalog **managed Iceberg**: a one-time historical load, then incremental sync per table until cutover.
It runs on-prem as a PySpark application (`spark-submit` on YARN), talks to Databricks over outbound HTTPS only
(Iceberg REST catalog + SQL Statement API), and keeps its own state in append-only Iceberg tables.

| Document | What it is |
|---|---|
| [`docs/requirements.md`](docs/requirements.md) | the requirement set, area-prefixed IDs (`SCP`, `ARC`, `INC`, `VAL`, ...), lab evidence, test backlog |
| [`docs/technical_design.md`](docs/technical_design.md) | the design: decisions D1-D12, components, state model, sync modes, validation tiers, risks |

## Status: scaffold (v0.1.0.dev0)

The repository holds the design, the package skeleton laid out exactly as design §4.1, and the modules that could be
written and unit-tested without a cluster. Everything else is a placeholder whose docstring cites the design section.

| Area | State | Module |
|---|---|---|
| Config model + loader (env.yaml / table.yaml, precedence, `config_hash`, layout rules) | implemented, tested | `h2mi.config` |
| Secret references (`vault:` stub, `aws-sm:`, `env:`, `dbx-profile:`) | implemented, tested | `h2mi.secrets` |
| Type map, column classification, SerDe families | implemented, tested (lab-validated rules) | `h2mi.scanner` |
| DDL generation: target (CLUSTER BY / PARTITIONED BY, MoR props, retention), staging, grants, deny-list | implemented, tested | `h2mi.ddl.generate` |
| Planner: mode table, method choice, batch packing, download estimate + executor routing | implemented, tested | `h2mi.planner` |
| Spark conf: mandatory settings, UC / HMS catalogs, runtime credential injection | implemented, tested | `h2mi.spark.session` |
| Metastore DESCRIBE parsers | implemented, tested | `h2mi.inventory.describe` |
| Canonical row encoding, fingerprints, commit tags, Statement API wrapper | implemented, partly tested | `h2mi.validate.canonical`, `h2mi.audit.tagging`, `h2mi.dbx` |
| CLI: `version`, `config check`, offline `ddl plan --columns` | implemented, tested | `h2mi.cli` |
| Inventory (HMS client, HDFS), bridges, DDL diff/apply, engines, sync modes, state store, leases, mirror, validation tiers, restore, jobs | **placeholders** | see docstrings |

## Quickstart

```bash
make setup && source .venv/bin/activate          # venv, dev + spark + aws extras, pre-commit hooks
make check                                       # ruff, writer lint, unit tests, secret scan
h2mi version
h2mi config check --env conf/examples/env.yaml --table conf/examples/tables/*.yaml
h2mi ddl plan --env conf/examples/env.yaml --table conf/examples/tables/payments.txn_daily.yaml \
     --columns "txn_id:bigint,account_id:bigint,amount:decimal(14,2),qty:tinyint,txn_date:string"
```

`conf/examples/` shows both layers of configuration (design §6). Copy them, replace the `<placeholders>`, and keep
secrets as references. Real `env.yaml` files are git-ignored except the examples.

## Layout

```
docs/               requirements + technical design (canonical copies)
conf/examples/      env.yaml, tables/*.yaml
src/h2mi/           the package (design §4.1): cli, config, secrets, spark, inventory, scanner, ddl, planner,
                    engines, sync, validate, state, audit, dbx, util
bridges/            Hive-side UDF for UNIONTYPE bridges (java source + build script)
scripts/            scan_secrets.sh, lint_no_v1_overwrite.py
tests/              unit (CI), integration (nightly, local Spark), live (opt-in, UC sandbox), bench
```

## Rules that are enforced
- Secret **references only** in configuration (`h2mi.config` rejects values), resolved at runtime (SEC-02).
- Writers use **DataFrameWriterV2 or SQL**; the V1 overwrite path is rejected by lint and CI (D12 / ARC-09).
- Exactly one of `cluster_by` / `partitioned_by`; `cluster_by` requires keys and leads with the locality column (DDL-15, INC-17).
- Prohibited / locked table properties are refused at DDL generation (DDL-06).
- `scripts/scan_secrets.sh` keeps keys, tokens and environment identifiers out of the tree.

## Relationship to `visa_utils`
The lab probes, findings and the EC2 "on-prem" rig that produced the evidence cited in the docs live in the
`visa_utils` repository (`topics/`, `setup/onprem_lab/`). `h2mi` does not import it; the helpers it needed were copied
in and are owned here. `tests/live` can be pointed at that rig or at a real cluster.

## Before sharing outside Databricks
Replace the lab citations in `docs/` (dates and `visa_utils/...` paths) with a self-contained evidence appendix, set
`CODEOWNERS`, choose a license, and run `make check` one last time.
