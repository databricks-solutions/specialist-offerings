# Security

## Secrets
- Configuration carries secret **references** only (`vault:<path>#<key>`, `aws-sm:<name>`, `env:<NAME>`), never values.
  `h2mi.config` rejects anything else (SEC-02). Values are resolved at runtime by `h2mi.secrets` and handed straight to
  `spark.conf.set` / a client constructor; they are never logged, printed or written to a file.
- The SP credential reaches Spark through `attach_uc()` after the session exists, so it is absent from spark-defaults,
  the command line and the Spark UI Environment tab. `spark.redaction.regex` also covers `credential`.
- `scripts/scan_secrets.sh` runs in pre-commit and CI. It fails on keys, tokens, account IDs, workspace hosts, IPs and
  home paths. Keep the lab-specific names in the git-ignored `scripts/.scan_denylist`.

## Identities (design §9.1)
Two service principals: `ddl-admin` (DDL, GRANT, RESTORE) and one `sync` SP per domain (SELECT / MODIFY /
EXTERNAL USE SCHEMA on its schema, CAN USE on the migration warehouse, no CREATE). Storage access is through
UC-vended credentials; no standing S3 keys.

## Reporting
Open a private issue or contact the code owners. Do not post secrets or environment identifiers in issues.
