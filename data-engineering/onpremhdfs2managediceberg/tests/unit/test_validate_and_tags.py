import pytest

from h2mi.audit.tagging import commit_tags
from h2mi.dbx.statements import split_statements
from h2mi.validate.canonical import canonical_sql, row_hash_sql


def test_canonical_forms():
    assert canonical_sql("`d`", "decimal(12,2)").startswith("coalesce(CAST(CAST(`d` * POW(10, 2) AS DECIMAL(38,0))")
    assert "unix_micros(`ts`)" in canonical_sql("`ts`", "timestamp")
    assert "yyyy-MM-dd HH:mm:ss.SSSSSS" in canonical_sql("`ts`", "timestamp_ntz")
    assert "array_sort(map_entries(`m`))" in canonical_sql("`m`", "map<string,int>")
    assert canonical_sql("`s`", "string").endswith("'\\u0000N')")
    rh = row_hash_sql([("`a`", "int"), ("`b`", "string")])
    assert rh.startswith("xxhash64(concat_ws('\\u001F', ") and rh.count("coalesce(") == 2


def test_commit_tags():
    tags = commit_tags(run_id="r", batch_id="b", cycle_id=3, src_watermark=None)
    assert tags == {"snapshot-property.h2mi.run_id": "r", "snapshot-property.h2mi.batch_id": "b",
                    "snapshot-property.h2mi.cycle_id": "3"}
    with pytest.raises(ValueError):
        commit_tags(bogus="x")


def test_split_statements():
    raw = "-- header only\n;;\nCREATE TABLE c.s.t (a INT)\n;;\nSELECT 1\n"
    assert split_statements(raw) == ["CREATE TABLE c.s.t (a INT)", "SELECT 1"]
