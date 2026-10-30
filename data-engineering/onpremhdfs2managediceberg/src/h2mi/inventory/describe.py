"""Parse metastore metadata as Spark reports it: DESCRIBE TABLE [EXTENDED] rows, Iceberg partitioning and
SHOW PARTITIONS specs. Pure Python, unit-tested; Spark collects the rows. (Design §8.1; the raw HiveMetaStoreClient
path for barrier types lives in hms_client.py.)

A row is (col_name, data_type, comment), the three columns Spark's DESCRIBE returns (Spark 3.5 and 4.x).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import unquote

HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__"


@dataclass
class TableInfo:
    columns: list[tuple[str, str]] = field(default_factory=list)   # (name, type) in order, partition columns included
    partition_cols: list[str] = field(default_factory=list)
    detail: dict[str, str] = field(default_factory=dict)            # '# Detailed Table Information' key -> value
    properties: dict[str, str] = field(default_factory=dict)

    @property
    def table_type(self) -> str:
        return self.detail.get("Type", "").upper()

    @property
    def is_view(self) -> bool:
        return self.table_type == "VIEW" or "View Text" in self.detail

    @property
    def is_iceberg(self) -> bool:
        return self.properties.get("table_type", "").upper() == "ICEBERG"

    @property
    def is_acid(self) -> bool:
        return self.properties.get("transactional", "").lower() == "true"

    @property
    def serde(self) -> str:
        return self.detail.get("Serde Library", "")

    @property
    def input_format(self) -> str:
        return self.detail.get("InputFormat", "")

    @property
    def location(self) -> str:
        return self.detail.get("Location", "")

    @property
    def view_text(self) -> str:
        return self.detail.get("View Text", "") or self.detail.get("View Original Text", "")


def _clean(v) -> str:
    return "" if v is None else str(v).strip()


def parse_properties(s: str) -> dict[str, str]:
    """Parse Spark's `[k1=v1, k2=v2]` property rendering. Values may contain commas, brackets or JSON."""
    s = _clean(s)
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    if not s:
        return {}
    out = {}
    for part in re.split(r", (?=[A-Za-z0-9_.\-]+=)", s):
        k, sep, v = part.partition("=")
        if sep:
            out[k.strip()] = v.strip()
    return out


def parse_describe(rows) -> TableInfo:
    """Rows of `DESCRIBE TABLE EXTENDED <t>` -> TableInfo."""
    info, section = TableInfo(), "columns"
    for r in rows:
        name, typ = _clean(r[0]), _clean(r[1]) if len(r) > 1 else ""
        if name.startswith("#"):
            low = name.lower()
            if "partition information" in low:
                section = "partitions"
            elif "detailed table information" in low or "storage information" in low:
                section = "detail"
            elif low.startswith("# col_name"):
                pass
            else:
                section = "other"
            continue
        if not name:
            if section == "partitions":
                section = "after_partitions"
            continue
        if section == "columns":
            info.columns.append((name, typ))
        elif section == "partitions":
            info.partition_cols.append(name)
        elif section == "detail":
            info.detail[name] = typ
    info.properties = parse_properties(info.detail.get("Table Properties", ""))
    return info


def parse_iceberg_partitioning(rows) -> list[str]:
    """Partition transforms from `DESCRIBE TABLE <iceberg table>`, e.g. ['dt', 'days(ts)'].

    Spark 3.4+ prints identity-only partitioning as '# Partition Information' (one row per column) and anything with a
    non-identity transform as '# Partitioning' with 'Part N' rows (verified on Spark 3.5.2 + Iceberg 1.12.0).
    """
    out, section = [], None
    for r in rows:
        name, typ = _clean(r[0]), _clean(r[1]) if len(r) > 1 else ""
        low = name.lower()
        if low == "# partitioning":
            section = "transforms"
            continue
        if low == "# partition information":
            section = "identity"
            continue
        if section is None or low.startswith("# col_name"):
            continue
        if not name or name.startswith("#"):
            break
        if section == "identity":
            out.append(name)
        elif low.startswith("part "):
            out.append(typ)
    return out


def identity_columns(transforms: list[str]) -> tuple[list[str], list[str]]:
    """(identity columns, non-identity transforms). Only identity partitions are recreated on the target (R6)."""
    ident, other = [], []
    for t in transforms:
        (other if "(" in t else ident).append(t.strip("`"))
    return ident, other


def parse_partition_spec(spec: str) -> dict[str, str | None]:
    """One SHOW PARTITIONS row, `k1=v1/k2=v2` (values Hive-escaped) -> {k1: v1, k2: v2}; default partition -> None."""
    out = {}
    for part in _clean(spec).split("/"):
        k, _, v = part.partition("=")
        v = unquote(v)
        out[unquote(k)] = None if v == HIVE_DEFAULT_PARTITION else v
    return out


def chunks(items: list, n: int) -> list[list]:
    """Split `items` into batches of at most n (n <= 0 means one batch)."""
    if n <= 0 or len(items) <= n:
        return [list(items)] if items else []
    return [items[i:i + n] for i in range(0, len(items), n)]
