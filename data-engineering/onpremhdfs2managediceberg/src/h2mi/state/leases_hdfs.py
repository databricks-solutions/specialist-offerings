"""Single writer per table: atomic HDFS lock files with heartbeat, stale takeover by rename, fence before every
commit (§7.3, OPS-03).

Not implemented in the scaffold."""
