"""hive-partition: HWM + fingerprint-based late data -> overwritePartitions() per batch; re-check fingerprints after
the write (INC-05..07).

Not implemented in the scaffold. Writers use DataFrameWriterV2 / SQL only (D12)."""
