# tests/integration

Local Spark + embedded Derby HMS + HadoopCatalog standing in for the managed Iceberg target (design §11). One fixture per
SerDe / type / table shape (Hive partitioned and unpartitioned; Iceberg CoW, MoR, evolved spec), then end-to-end
`load` + `sync` cycles against the local target. Needs Java 17 and `pip install -e '.[spark,dev]'`.

Not populated in the scaffold. The template is the 2026-10-28 local end-to-end run described in
`visa_utils/topics/03_migration_approaches/onetime/README.md`.
