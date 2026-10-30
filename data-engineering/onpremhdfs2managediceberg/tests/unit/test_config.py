import pytest

from h2mi.config import effective, load_env, load_table
from h2mi.config.models import Layout, TableConfig
from h2mi.util.errors import ConfigError


def test_examples_load_and_hash_is_stable(examples):
    env = load_env(examples / "env.yaml")
    t = load_table(examples / "tables" / "payments.txn_daily.yaml")
    e1, e2 = effective(env, t), effective(env, t)
    assert e1.config_hash == e2.config_hash and len(e1.config_hash) == 16
    assert e1.target_fq == f"{env.databricks.uc_catalog}.payments.txn_daily".lower()
    assert e1.format_version == 2                  # table overrides the env default of 3
    assert e1.merge_executor == "auto" and e1.onprem_merge_budget_bytes == 50 * 1024 ** 3
    assert e1.retention.log_retention.startswith("interval")
    assert e1.findings == ()


def test_keyless_example(examples):
    env = load_env(examples / "env.yaml")
    e = effective(env, load_table(examples / "tables" / "events.clicks_raw.yaml"))
    assert e.table.layout.partitioned_by == ["dt"] and e.table.keys == []
    assert e.target_fq.endswith(".events.clicks_raw")


def test_secret_values_are_rejected(tmp_path, examples):
    bad = (examples / "env.yaml").read_text().replace("vault:secret/h2mi/ddl-admin#secret", "dose0123456789abcdef")
    p = tmp_path / "env.yaml"
    p.write_text(bad)
    with pytest.raises(ConfigError, match="SEC-02"):
        load_env(p)


def test_layout_exactly_one():
    with pytest.raises(ValueError, match="DDL-15"):
        Layout()
    with pytest.raises(ValueError, match="DDL-15"):
        Layout(cluster_by=["k"], partitioned_by=["dt"])


def test_cluster_by_requires_keys_and_locality_order():
    with pytest.raises(ValueError, match="requires keys"):
        TableConfig(source="db.t", domain="d", layout=Layout(cluster_by=["dt", "k"]))
    with pytest.raises(ValueError, match="INC-17"):
        TableConfig(source="db.t", domain="d", keys=["k"], locality_column="dt", layout=Layout(cluster_by=["k", "dt"]))
    ok = TableConfig(source="db.t", domain="d", keys=["k"], locality_column="dt", layout=Layout(cluster_by=["dt", "k"]))
    assert ok.hive_db == "db" and ok.hive_table == "t"


def test_key_only_clustering_is_a_finding(examples):
    env = load_env(examples / "env.yaml")
    t = TableConfig(source="db.t", domain="payments", keys=["k"], layout=Layout(cluster_by=["k"]))
    f = effective(env, t).findings
    assert any("nothing prunes" in x for x in f) and any("R13" in x for x in f)
