import pytest

from h2mi.config.models import RetentionClass
from h2mi.ddl import generate as g
from h2mi.scanner.typemap import plan_column
from h2mi.util.errors import ConfigError

COLS = [plan_column("id", "bigint", comment="the key"), plan_column("qty", "tinyint"), plan_column("dt", "string")]


def test_partitioned_target():
    ddl = g.create_table_ddl("c.s.t", COLS, partitioned_by=["dt"], format_version=3, comment="it's migrated")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS c.s.t (\n  `id` BIGINT COMMENT 'the key',\n  `qty` INT,\n  `dt` STRING\n)")
    assert "USING ICEBERG\nPARTITIONED BY (`dt`)" in ddl
    assert "COMMENT 'it\\'s migrated'" in ddl
    assert ddl.endswith("TBLPROPERTIES ('format-version'='3')")
    assert "merge-on-read" not in ddl


def test_clustered_target_gets_merge_on_read_and_retention():
    ddl = g.create_table_ddl("c.s.t", COLS, cluster_by=["dt", "id"], format_version=3,
                             retention=RetentionClass(log_retention="interval 30 days",
                                                      deleted_file_retention="interval 7 days"))
    assert "CLUSTER BY (`dt`, `id`)" in ddl and "PARTITIONED BY" not in ddl
    assert "'write.merge.mode'='merge-on-read'" in ddl and "'write.delete.mode'='merge-on-read'" in ddl
    assert "'delta.logRetentionDuration'='interval 30 days'" in ddl


def test_layout_and_denylist_guards():
    with pytest.raises(ConfigError, match="DDL-15"):
        g.create_table_ddl("c.s.t", COLS, format_version=3)
    with pytest.raises(ConfigError, match="DDL-15"):
        g.create_table_ddl("c.s.t", COLS, cluster_by=["id"], partitioned_by=["dt"])
    with pytest.raises(ConfigError, match="DDL-06"):
        g.create_table_ddl("c.s.t", COLS, partitioned_by=["dt"], extra_props={"gc.enabled": "true"})
    with pytest.raises(ConfigError, match="REDESIGN"):
        g.create_table_ddl("c.s.t", [plan_column("u", "uniontype<int>")], partitioned_by=["u"])


def test_staging_table_and_grants():
    ddl = g.staging_table_ddl("c.s.t", COLS)
    assert "CREATE TABLE IF NOT EXISTS c.s.__h2mi_stg_t (" in ddl
    assert "`_h2mi_change` STRING,\n  `_h2mi_batch_id` STRING" in ddl
    assert "PARTITIONED BY (`_h2mi_batch_id`)" in ddl
    gr = g.grants("c.s.t", "sp-app-id", readers=["analysts"])
    assert gr[0] == "GRANT USE CATALOG ON CATALOG c TO `sp-app-id`"
    assert any("EXTERNAL USE SCHEMA ON SCHEMA c.s" in x for x in gr)
    assert any("c.s.__h2mi_stg_t" in x for x in gr) and gr[-1].endswith("TO `analysts`")
    assert not any("CREATE" in x for x in gr)       # the sync SP never gets CREATE (DDL-08)
