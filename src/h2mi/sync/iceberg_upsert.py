"""iceberg-upsert: create_changelog_view -> net per key -> tagged staging append over IRC -> MERGE via the
Statement API by default, on-prem only under budget (INC-03, INC-08, INC-16). The MERGE source must be materialized.

Not implemented in the scaffold. Writers use DataFrameWriterV2 / SQL only (D12)."""
