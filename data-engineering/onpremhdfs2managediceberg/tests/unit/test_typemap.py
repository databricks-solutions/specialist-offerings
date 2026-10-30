import pytest

from h2mi.scanner import typemap as tm


@pytest.mark.parametrize("src, target, action", [
    ("tinyint", "INT", "convert"), ("smallint", "INT", "convert"), ("int", "INT", "pass"),
    ("bigint", "BIGINT", "pass"), ("float", "FLOAT", "pass"), ("double", "DOUBLE", "pass"),
    ("decimal(12,2)", "DECIMAL(12,2)", "pass"), ("decimal", "DECIMAL(10,0)", "convert"),
    ("string", "STRING", "pass"), ("char(5)", "STRING", "convert"), ("varchar(20)", "STRING", "convert"),
    ("boolean", "BOOLEAN", "pass"), ("binary", "BINARY", "pass"), ("date", "DATE", "pass"),
    ("timestamp", "TIMESTAMP_NTZ", "convert"), ("timestamp_ntz", "TIMESTAMP_NTZ", "pass"),
    ("void", "STRING", "convert"),
])
def test_hive_scalars(src, target, action):
    m = tm.map_type(tm.parse_type(src), "hive")
    assert (m.target, m.action) == (target, action)


@pytest.mark.parametrize("src", ["timestamp with local time zone", "interval_day_time", "interval_year_month",
                                 "uniontype<int,string>"])
def test_barrier_types_need_a_bridge(src):
    m = tm.map_type(tm.parse_type(src), "hive")
    assert m.target is None and m.action == "bridge"


def test_unknown_type_blocks():
    m = tm.map_type(tm.parse_type("geometry"), "hive")
    assert m.target is None and m.action == "block"


def test_iceberg_timestamp_keeps_the_instant():
    assert tm.map_type(tm.parse_type("timestamp"), "iceberg").target == "TIMESTAMP"
    assert tm.map_type(tm.parse_type("timestamp_ntz"), "iceberg").target == "TIMESTAMP_NTZ"
    assert tm.map_type(tm.parse_type("void"), "iceberg").action == "block"


def test_nested_types_map_recursively():
    m = tm.map_type(tm.parse_type("struct<a:tinyint,b:array<timestamp>,c:map<string,char(3)>>"), "hive")
    assert m.action == "convert"
    assert m.target == "STRUCT<`a`: INT, `b`: ARRAY<TIMESTAMP_NTZ>, `c`: MAP<STRING,STRING>>"
    assert tm.map_type(tm.parse_type("array<struct<x:int,y:string>>"), "hive").action == "pass"


def test_nested_barrier_propagates():
    m = tm.map_type(tm.parse_type("struct<a:int,u:uniontype<int,string>>"), "hive")
    assert m.target is None and m.action == "bridge"


def test_backticked_struct_fields():
    node = tm.parse_type("struct<`odd name`:int,`a,b`:string>")
    assert [f for f, _ in node.fields] == ["odd name", "a,b"]


def test_decimal_10_0_is_flagged_for_review():
    assert any("TYP-04" in n for n in tm.map_type(tm.parse_type("decimal(10,0)"), "hive").notes)
    assert not tm.map_type(tm.parse_type("decimal(10,0)"), "iceberg").notes


def test_plan_column_projections():
    assert tm.plan_column("qty", "tinyint").expr == "CAST(`qty` AS INT) AS `qty`"
    assert tm.plan_column("ts", "timestamp").expr == "CAST(`ts` AS TIMESTAMP_NTZ) AS `ts`"
    assert tm.plan_column("id", "bigint").expr == "`id`"
    assert tm.plan_column("v", "varchar(10)").expr == "`v`"
    assert tm.plan_column("s", "struct<a:smallint>").expr == "CAST(`s` AS STRUCT<`a`: INT>) AS `s`"
    assert tm.plan_column("id", "bigint", comment="the key").comment == "the key"


def test_char_policy():
    assert tm.plan_column("c", "char(5)", char_policy="preserve_padded").expr == "`c`"
    assert tm.plan_column("c", "char(5)", char_policy="trim").expr == "trim(`c`) AS `c`"
    assert any("not trimmed" in n for n in tm.plan_column("s", "struct<c:char(3)>", char_policy="trim").notes)
    with pytest.raises(ValueError):
        tm.plan_column("c", "char(5)", char_policy="rpad")


def test_string_only_serde_keeps_string():
    p = tm.plan_column("status", "int", string_only=True)
    assert (p.target_type, p.expr, p.action) == ("STRING", "`status`", "pass")


def test_table_action_is_the_worst_column():
    cols = [tm.plan_column("a", "int"), tm.plan_column("b", "tinyint")]
    assert tm.table_action(cols) == "convert"
    assert tm.table_action(cols + [tm.plan_column("u", "uniontype<int>")]) == "bridge"
    assert tm.table_action([]) == "block"


@pytest.mark.parametrize("serde, fmt, family", [
    ("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", None, "read_through"),
    ("org.apache.hadoop.hive.ql.io.orc.OrcSerde", None, "read_through"),
    ("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "org.apache.hadoop.mapred.TextInputFormat", "read_through"),
    ("org.apache.hadoop.hive.serde2.OpenCSVSerde", None, "string_only"),
    ("org.apache.hadoop.hive.serde2.RegexSerDe", None, "string_only"),
    ("org.apache.hive.hcatalog.data.JsonSerDe", None, "json"),
    ("com.example.CustomSerDe", None, "refuse"),
])
def test_classify_storage(serde, fmt, family):
    assert tm.classify_storage(serde, fmt).family == family


def test_sequencefile_is_untested():
    sc = tm.classify_storage("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                             "org.apache.hadoop.mapred.SequenceFileInputFormat")
    assert sc.family == "read_through" and not sc.tested


def test_barrier_error_and_packaged_policy():
    assert tm.is_barrier_error("[CANNOT_RECOGNIZE_HIVE_TYPE] Cannot recognize hive type string")
    assert not tm.is_barrier_error("Table not found")
    assert tm.MATRIX_PATH.is_file(), "typemap.yaml must ship inside the package"
    pol = tm.load_policy()
    assert pol["char_policy_default"] in tm.CHAR_POLICIES and pol["session_timezone"] == "UTC"
    assert pol["target_format_version"] == 3
