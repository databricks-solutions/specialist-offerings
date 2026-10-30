"""Source-vs-target validation helpers (design §8.8). Pure canonical-encoding SQL plus Spark-side fingerprints.

Levels:
    L0  row count
    L1  count + order-independent fingerprint: sum(xxhash64(cols)) as DECIMAL(38,0), so it cannot overflow
    L2  exact multiset diff: EXCEPT ALL both ways (handles duplicate rows in keyless tables)
Tier M (commit metadata) lives in fingerprints.py; the same `row_hash` SQL text must run on OSS Spark and DBSQL (VAL-09).
"""
from __future__ import annotations

from dataclasses import dataclass

NULL_SENTINEL = "\\u0000N"
SEP = "\\u001F"


def canonical_sql(col: str, spark_type: str) -> str:
    """Canonical text form of one column (§8.8 table). Same text on both engines; `col` is already quoted."""
    t = spark_type.lower()
    if t.startswith("decimal"):
        scale = int(t[t.index(",") + 1:-1]) if "," in t else 0
        inner = f"CAST({col} * POW(10, {scale}) AS DECIMAL(38,0))"
    elif t in ("float", "double"):
        inner = f"CASE WHEN {col} = 0 THEN '0.0' WHEN isnan({col}) THEN 'NaN' ELSE format_string('%.17g', {col}) END"
    elif t == "date":
        inner = f"date_format({col}, 'yyyy-MM-dd')"
    elif t == "timestamp_ntz":
        inner = f"date_format({col}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')"
    elif t == "timestamp":
        inner = f"CAST(unix_micros({col}) AS STRING)"
    elif t == "binary":
        inner = f"hex({col})"
    elif t.startswith("map"):
        inner = f"to_json(array_sort(map_entries({col})))"
    elif t.startswith(("array", "struct")):
        inner = f"to_json({col})"
    else:
        inner = f"CAST({col} AS STRING)"
    return f"coalesce(CAST({inner} AS STRING), '{NULL_SENTINEL}')"


def row_hash_sql(cols: list[tuple[str, str]]) -> str:
    """`xxhash64(concat_ws(SEP, canon(c1), ...))` over (quoted column, spark type) pairs."""
    return "xxhash64(concat_ws('" + SEP + "', " + ", ".join(canonical_sql(c, t) for c, t in cols) + "))"


@dataclass
class Fingerprint:
    rows: int
    hash_sum: int

    def __str__(self) -> str:
        return f"rows={self.rows} hash_sum={self.hash_sum}"


def _df(spark, src):
    if not isinstance(src, str):
        return src
    s = src.strip()
    if s.split(None, 1)[0].upper() in ("SELECT", "WITH", "("):
        return spark.sql(s)
    return spark.sql(f"SELECT * FROM {s}") if " " in s else spark.table(s)


def canonical(df):
    """Complex columns as JSON text (maps with sorted entries) so xxhash64 and EXCEPT ALL accept them."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, MapType, StructType

    cols = []
    for f in df.schema.fields:
        if isinstance(f.dataType, MapType):
            cols.append(F.to_json(F.array_sort(F.map_entries(F.col(f.name)))).alias(f.name))
        elif isinstance(f.dataType, (ArrayType, StructType)):
            cols.append(F.to_json(F.col(f.name)).alias(f.name))
        else:
            cols.append(F.col(f.name))
    return df.select(*cols)


def fingerprint(spark, src, cols: list[str] | None = None) -> Fingerprint:
    from pyspark.sql import functions as F

    df = canonical(_df(spark, src))
    cols = cols or sorted(df.columns)
    r = df.select(F.count(F.lit(1)).alias("n"),
                  F.coalesce(F.sum(F.xxhash64(*cols).cast("decimal(38,0)")), F.lit(0)).alias("h")).first()
    return Fingerprint(int(r["n"]), int(r["h"]))


def multiset_diff(spark, a, b, cols: list[str] | None = None) -> tuple[int, int]:
    da, db = canonical(_df(spark, a)), canonical(_df(spark, b))
    cols = cols or sorted(da.columns)
    da, db = da.select(*cols), db.select(*cols)
    return da.exceptAll(db).count(), db.exceptAll(da).count()
