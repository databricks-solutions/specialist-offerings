# tests/integration

Local Spark + embedded Derby HMS + HadoopCatalog standing in for the managed Iceberg target (design §11). One fixture per
SerDe / type / table shape (Hive partitioned and unpartitioned; Iceberg CoW, MoR, evolved spec), then end-to-end
`load` + `sync` cycles against the local target. Needs Java 17 and `pip install -e '.[spark,dev]'`.

Not populated in the scaffold. The template is a 2026-10-28 local end-to-end run of a generic loader on an embedded
Derby HMS with Hive + Iceberg fixtures and a HadoopCatalog standing in for the managed target.
