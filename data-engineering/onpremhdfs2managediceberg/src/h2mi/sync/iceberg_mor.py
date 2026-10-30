"""iceberg-mor: snapshot-diff over touched partitions (manifests added in range) -> MERGE (keyed) or
overwritePartitions() on a PARTITIONED BY target (keyless); clustered keyless -> Databricks REPLACE WHERE (INC-04).

Not implemented in the scaffold. Writers use DataFrameWriterV2 / SQL only (D12)."""
