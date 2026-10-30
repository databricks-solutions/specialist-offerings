"""Source column type -> UC managed Iceberg (MI) type mapping, and the SerDe families the loader accepts (D5).

Pure Python (no Spark), so the rules are unit-tested. The matrix rows in `typemap.yaml` (same folder) are written for
people; the policy settings at its top (CHAR policy, session time zone, format version, barrier markers) are read here.

Input type strings are Spark catalog strings: what `DESCRIBE TABLE` shows for a Hive table (`tinyint`, `char(5)`,
`array<struct<a:int>>`), or `df.dtypes` for an Iceberg table read through the `hms` catalog.

Source kinds:
    hive     a Hive-format table read through the session catalog. Zone-less TIMESTAMP maps to TIMESTAMP_NTZ (DT-01).
    iceberg  an OSS Iceberg table read through the `hms` HiveCatalog. Iceberg `timestamptz` (Spark `timestamp`) stays
             TIMESTAMP, Iceberg `timestamp` (Spark `timestamp_ntz`) stays TIMESTAMP_NTZ.

Actions per column: pass (same type, no cast), convert (different MI type and/or a read-side cast), bridge (the Hive
type needs a materialized CTAS copy first, see bridges/), block (no rule: fail closed, TYP-09). classify.py maps these
to the design's PASS / AUTO_MAP / REDESIGN / BLOCK classes.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

MATRIX_PATH = Path(__file__).with_name("typemap.yaml")

DEFAULT_POLICY = {
    "char_policy_default": "preserve_padded",
    "session_timezone": "UTC",
    "target_format_version": 3,
    "barrier_error_markers": ["CANNOT_RECOGNIZE_HIVE_TYPE", "UNSUPPORTED_DATATYPE", "ParseException"],
}
CHAR_POLICIES = ("preserve_padded", "trim")
_SEVERITY = {"pass": 0, "convert": 1, "bridge": 2, "block": 3}


def load_policy(path: str | Path | None = None) -> dict:
    """Policy settings from typemap.yaml; documented defaults if pyyaml or the file is absent."""
    p = Path(path or MATRIX_PATH)
    try:
        import yaml

        data = yaml.safe_load(p.read_text()) or {}
    except (ImportError, OSError):
        data = {}
    return {k: data.get(k, v) for k, v in DEFAULT_POLICY.items()}


def is_barrier_error(msg: str, markers: list[str] | None = None) -> bool:
    """True when a Spark error is the metastore read barrier (LTZ / UNIONTYPE / INTERVAL type strings)."""
    return any(m in msg for m in (markers or DEFAULT_POLICY["barrier_error_markers"]))


# ------------------------------------------------------------------------------------------------ type-string parser
@dataclass
class TypeNode:
    name: str                                   # lower-case base name: int, decimal, char, array, map, struct, ...
    args: list[str] = field(default_factory=list)          # decimal(p,s) / char(n) / varchar(n) parameters
    children: list[TypeNode] = field(default_factory=list)  # array: [elem]; map: [key, value]
    fields: list[tuple[str, TypeNode]] = field(default_factory=list)  # struct fields


def _split_top(s: str, sep: str = ",") -> list[str]:
    """Split on `sep` outside <>, () and backticks."""
    out, depth, cur, tick = [], 0, [], False
    for ch in s:
        if ch == "`":
            tick = not tick
        elif not tick and ch in "<(":
            depth += 1
        elif not tick and ch in ">)":
            depth -= 1
        if ch == sep and depth == 0 and not tick:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    out.append("".join(cur))
    return [x.strip() for x in out if x.strip()]


split_top = _split_top      # public: split a `name:type,...` list where types themselves contain commas


def parse_type(s: str) -> TypeNode:
    """Parse a Spark / Hive catalog type string into a TypeNode."""
    t = s.strip()
    low = t.lower()
    for kind in ("array", "map", "struct", "uniontype"):
        if low.startswith(kind + "<") and t.endswith(">"):
            inner = t[len(kind) + 1:-1]
            if kind == "array":
                return TypeNode("array", children=[parse_type(inner)])
            if kind == "map":
                k, v = _split_top(inner)
                return TypeNode("map", children=[parse_type(k), parse_type(v)])
            if kind == "uniontype":
                return TypeNode("uniontype", children=[parse_type(x) for x in _split_top(inner)])
            flds = []
            for part in _split_top(inner):
                name, typ = _split_field(part)
                flds.append((name, parse_type(typ)))
            return TypeNode("struct", fields=flds)
    m = re.fullmatch(r"([a-z_ ]+?)\s*\(([^)]*)\)", low)
    if m:
        return TypeNode(m.group(1).strip(), args=[a.strip() for a in m.group(2).split(",")])
    return TypeNode(low)


def _split_field(part: str) -> tuple[str, str]:
    if part.startswith("`"):
        end = part.index("`", 1)
        return part[1:end], part[end + 1:].lstrip(" :")
    name, _, typ = part.partition(":")
    return name.strip(), typ.strip()


def quote(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


# ------------------------------------------------------------------------------------------------ mapping
@dataclass
class Mapped:
    target: str | None          # MI DDL type, None when not migratable
    action: str                 # pass | convert | bridge | block
    notes: list[str] = field(default_factory=list)


def _worst(*actions: str) -> str:
    return max(actions, key=_SEVERITY.__getitem__)


_PASS_SCALARS = {"int": "INT", "integer": "INT", "bigint": "BIGINT", "float": "FLOAT", "double": "DOUBLE",
                 "string": "STRING", "boolean": "BOOLEAN", "binary": "BINARY", "date": "DATE",
                 "timestamp_ntz": "TIMESTAMP_NTZ"}


def map_type(node: TypeNode, source: str = "hive") -> Mapped:
    """Map one (possibly nested) type. `source` is "hive" or "iceberg" (see module doc)."""
    n = node.name
    if n in _PASS_SCALARS:
        return Mapped(_PASS_SCALARS[n], "pass")
    if n in ("tinyint", "smallint", "byte", "short"):
        return Mapped("INT", "convert", [f"{n} -> INT: MI rejects byte/short at CREATE (TYP-03)"])
    if n == "decimal":
        if not node.args:
            return Mapped("DECIMAL(10,0)", "convert", ["unqualified DECIMAL pinned to (10,0); review (TYP-04)"])
        p, s = (node.args + ["0"])[:2]
        notes = ["decimal(10,0): may be an unqualified Hive DECIMAL; confirm the intended precision (TYP-04)"] \
            if (p, s) == ("10", "0") and source == "hive" else []
        return Mapped(f"DECIMAL({p},{s})", "pass", notes)
    if n == "char":
        return Mapped("STRING", "convert", [f"char({','.join(node.args)}) -> STRING; CHAR policy applies (TYP-05)"])
    if n == "varchar":
        return Mapped("STRING", "convert", [])
    if n == "timestamp":
        if source == "iceberg":
            return Mapped("TIMESTAMP", "pass")       # Iceberg timestamptz: a true instant
        return Mapped("TIMESTAMP_NTZ", "convert",
                      ["Hive zone-less TIMESTAMP -> TIMESTAMP_NTZ (DT-01); sub-us digits are truncated (DT-05)"])
    if n in ("void", "null"):
        if source == "hive":
            return Mapped("STRING", "convert", ["VOID -> STRING: MI rejects void; the column holds only NULLs"
                                                " (TYP-06)"])
        return Mapped(None, "block", ["void column: no rule for this source (TYP-09)"])
    if n.startswith("interval") or n in ("interval_day_time", "interval_year_month", "uniontype",
                                         "timestamp with local time zone", "timestamplocaltz"):
        return Mapped(None, "bridge", [f"{n}: Spark cannot read it from the metastore; materialize a bridge copy "
                                       "(bridges/, TYP-08)"])
    if n == "array":
        e = map_type(node.children[0], source)
        return Mapped(f"ARRAY<{e.target}>" if e.target else None, e.action, e.notes)
    if n == "map":
        k, v = (map_type(c, source) for c in node.children)
        tgt = f"MAP<{k.target},{v.target}>" if k.target and v.target else None
        return Mapped(tgt, _worst(k.action, v.action), k.notes + v.notes)
    if n == "struct":
        parts, action, notes = [], "pass", []
        for fname, ftype in node.fields:
            m = map_type(ftype, source)
            action, notes = _worst(action, m.action), notes + [f"{fname}: {x}" for x in m.notes]
            parts.append(f"{quote(fname)}: {m.target}" if m.target else None)
        tgt = f"STRUCT<{', '.join(parts)}>" if all(parts) else None
        return Mapped(tgt, action, notes)
    return Mapped(None, "block", [f"{n}: no mapping rule; add one to typemap.yaml first (TYP-09)"])


@dataclass
class ColumnPlan:
    name: str
    source_type: str
    target_type: str | None
    expr: str | None            # read projection, already aliased to the column name
    action: str
    notes: list[str] = field(default_factory=list)
    comment: str | None = None  # carried from HMS into the target DDL (DDL-09)


def plan_column(name: str, source_type: str, source: str = "hive", char_policy: str = "preserve_padded",
                string_only: bool = False, comment: str | None = None) -> ColumnPlan:
    """Plan one top-level column. `string_only` marks SerDes that surface every column as STRING (OpenCSV, Regex):
    the default keeps STRING with no added cast (SRC-03)."""
    if char_policy not in CHAR_POLICIES:
        raise ValueError(f"char_policy must be one of {CHAR_POLICIES} (got {char_policy!r})")
    q = quote(name)
    if string_only:
        return ColumnPlan(name, source_type, "STRING", q, "pass",
                          ["string-only SerDe: kept as STRING; typed reparse is an opt-in enrichment (SRC-03)"], comment)
    node = parse_type(source_type)
    m = map_type(node, source)
    if m.target is None:
        return ColumnPlan(name, source_type, None, None, m.action, m.notes, comment)
    if m.action == "pass":
        expr = q
    elif node.name == "char":
        expr = q if char_policy == "preserve_padded" else f"trim({q}) AS {q}"
    elif node.name == "varchar":
        expr = q
    else:
        expr = f"CAST({q} AS {m.target}) AS {q}"
    notes = list(m.notes)
    if char_policy == "trim" and node.name in ("array", "map", "struct") and "char(" in source_type.lower():
        notes.append("nested CHAR is not trimmed (the policy applies to top-level columns)")
    return ColumnPlan(name, source_type, m.target, expr, m.action, notes, comment)


def table_action(cols: list[ColumnPlan]) -> str:
    return _worst("pass", *(c.action for c in cols)) if cols else "block"


# ------------------------------------------------------------------------------------------------ SerDe families
READ_THROUGH_SERDES = {
    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe": "parquet",
    "org.apache.hadoop.hive.ql.io.orc.OrcSerde": "orc",
    "org.apache.hadoop.hive.serde2.avro.AvroSerDe": "avro",
    "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe": "rcfile",
    "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe": "rcfile",
    "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe": "text",
}
STRING_ONLY_SERDES = {
    "org.apache.hadoop.hive.serde2.OpenCSVSerde": "opencsv",
    "org.apache.hadoop.hive.serde2.RegexSerDe": "regex",
}
JSON_SERDES = {
    "org.apache.hive.hcatalog.data.JsonSerDe": "json",
    "org.apache.hadoop.hive.serde2.JsonSerDe": "json",
    "org.openx.data.jsonserde.JsonSerDe": "json",
}
# Recognized but not lab-tested: loaded only with an explicit override (SRC-02 fails closed otherwise).
UNTESTED_INPUT_FORMATS = {"org.apache.hadoop.mapred.SequenceFileInputFormat": "sequencefile (Q-12)"}


@dataclass
class StorageClass:
    family: str                 # read_through | string_only | json | refuse
    label: str
    tested: bool = True
    reason: str = ""


def classify_storage(serde: str | None, input_format: str | None = None) -> StorageClass:
    """Classify a Hive table by SerDe (+ input format). Unknown SerDes are refused (fail closed, SRC-02)."""
    s, f = (serde or "").strip(), (input_format or "").strip()
    if f in UNTESTED_INPUT_FORMATS:
        return StorageClass("read_through", UNTESTED_INPUT_FORMATS[f], tested=False,
                            reason="input format recognized but not lab-tested")
    if s in READ_THROUGH_SERDES:
        return StorageClass("read_through", READ_THROUGH_SERDES[s])
    if s in STRING_ONLY_SERDES:
        return StorageClass("string_only", STRING_ONLY_SERDES[s])
    if s in JSON_SERDES:
        return StorageClass("json", JSON_SERDES[s])
    return StorageClass("refuse", s or "unknown", reason=f"SerDe {s or '<none>'} is not in the supported list (SRC-02)")
