# CLAUDE.md: working in h2mi

The production migration utility designed in `docs/technical_design.md` (requirements in `docs/requirements.md`).
Reference every change to a requirement ID (`INC-16`) or design item (`D11`, `§8.6`, `R12`); the docs are the contract.

## Hard rules
- **Secret references only.** `vault:` / `aws-sm:` / `env:` in config; values resolved at runtime by `h2mi.secrets`.
  Never print, log, or pass a secret on a command line (SEC-02).
- **No environment identifiers in code or examples**: hosts, IDs, buckets, principals, IPs come from `env.yaml`.
  Examples use `<placeholders>`. `scripts/scan_secrets.sh` must stay clean.
- **Writers use DataFrameWriterV2 (`writeTo`) or SQL only.** `.mode("overwrite")` is REPLACE TABLE over IRC and is
  rejected by `scripts/lint_no_v1_overwrite.py` (D12, ARC-09).
- **Upload only.** Nothing reads target data from on-prem unless `planner.executor` says the batch is under budget (D11).
- **Fail closed.** Unknown type / SerDe / mode / drift -> BlockedError (exit 4), never a guess (TYP-09, SRC-02).
- Targets are created through the Statement API, never IRC `createTable` (D9). Always explicit `format-version`.
- `make check` before finishing any change (ruff, writer lint, unit tests, secret scan).

## Layout
`src/h2mi/<module>` follows design §4.1. Pure logic (typemap, classify, ddl.generate, planner.*, validate.canonical,
inventory.describe) is unit-tested without Spark; Spark-facing code takes a session as an argument.
Modules whose docstring ends in "Not implemented in the scaffold" are placeholders: implement them against the design
section they cite and add tests in the same change.

## Lab facts to keep
- Iceberg >= 1.11 (never 1.8.x); `credential=<id>:<secret>` + `token-exchange-enabled=false`; keep batches under ~45 min
  (vended S3 credentials are not refreshed, R11).
- MoR changelog is unusable while live delete files exist -> snapshot-diff + MERGE or partition replace.
- `overwritePartitions()` works on `PARTITIONED BY` managed Iceberg targets and carries snapshot tags; filter-overwrite
  fails on `CLUSTER BY` targets. MERGE commits carry no tags (staging does).
- Lab evidence and probe scripts live in the `visa_utils` repo; this repo must not import it.
