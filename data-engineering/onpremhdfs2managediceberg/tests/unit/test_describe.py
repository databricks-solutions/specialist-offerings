from h2mi.inventory import describe as inv

HIVE_ROWS = [
    ("id", "bigint", None), ("qty", "tinyint", None), ("code", "char(5)", None), ("dt", "string", None),
    ("# Partition Information", "", ""), ("# col_name", "data_type", "comment"), ("dt", "string", None),
    ("", "", ""),
    ("# Detailed Table Information", "", ""),
    ("Database", "db1", ""), ("Table", "t1", ""), ("Type", "EXTERNAL", ""), ("Provider", "hive", ""),
    ("Table Properties", "[COLUMN_STATS_ACCURATE={\"BASIC_STATS\":\"true\"}, numRows=912, "
                         "transient_lastDdlTime=1676282259]", ""),
    ("Location", "hdfs://nn/warehouse/db1.db/t1", ""),
    ("Serde Library", "org.apache.hadoop.hive.ql.io.orc.OrcSerde", ""),
    ("InputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", ""),
    ("Partition Provider", "Catalog", ""),
]


def test_parse_describe_hive():
    info = inv.parse_describe(HIVE_ROWS)
    assert info.columns == [("id", "bigint"), ("qty", "tinyint"), ("code", "char(5)"), ("dt", "string")]
    assert info.partition_cols == ["dt"]
    assert info.table_type == "EXTERNAL" and not info.is_view and not info.is_iceberg and not info.is_acid
    assert info.serde.endswith("OrcSerde") and info.location.endswith("/t1")
    assert info.properties["numRows"] == "912"
    assert info.properties["COLUMN_STATS_ACCURATE"] == '{"BASIC_STATS":"true"}'


def test_iceberg_acid_and_view_flags():
    ice = inv.parse_describe([("id", "bigint", ""), ("", "", ""), ("# Detailed Table Information", "", ""),
                              ("Table Properties", "[metadata_location=hdfs://x/m.json, table_type=ICEBERG]", "")])
    assert ice.is_iceberg and ice.partition_cols == []
    acid = inv.parse_describe([("# Detailed Table Information", "", ""),
                               ("Table Properties", "[transactional=true, transactional_properties=default]", "")])
    assert acid.is_acid
    view = inv.parse_describe([("a", "int", ""), ("# Detailed Table Information", "", ""), ("Type", "VIEW", ""),
                               ("View Text", "SELECT a FROM t", "")])
    assert view.is_view and view.view_text == "SELECT a FROM t"


def test_parse_iceberg_partitioning():
    rows = [("id", "bigint", ""), ("ts", "timestamp", ""), ("dt", "string", ""), ("", "", ""),
            ("# Partitioning", "", ""), ("Part 0", "dt", ""), ("Part 1", "days(ts)", ""), ("", "", ""),
            ("# Metadata Columns", "", "")]
    transforms = inv.parse_iceberg_partitioning(rows)
    assert transforms == ["dt", "days(ts)"]
    assert inv.identity_columns(transforms) == (["dt"], ["days(ts)"])
    assert inv.parse_iceberg_partitioning([("id", "bigint", "")]) == []


def test_parse_iceberg_identity_partition_information():
    rows = [("id", "bigint", None), ("dt", "string", None), ("# Partition Information", "", ""),
            ("# col_name", "data_type", "comment"), ("dt", "string", None)]
    assert inv.parse_iceberg_partitioning(rows) == ["dt"]


def test_parse_partition_spec_and_chunks():
    assert inv.parse_partition_spec("dt=2024-01-01/region=US%2FEast") == {"dt": "2024-01-01", "region": "US/East"}
    assert inv.parse_partition_spec("dt=__HIVE_DEFAULT_PARTITION__") == {"dt": None}
    assert inv.chunks([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert inv.chunks([1, 2], 0) == [[1, 2]]
    assert inv.chunks([], 3) == []
