"""Column classification PASS / AUTO_MAP / REDESIGN / BLOCK (design §8.2) on top of typemap, plus the table verdict."""
from __future__ import annotations

from dataclasses import dataclass

from .typemap import ColumnPlan, plan_column, table_action

CLASS_OF = {"pass": "PASS", "convert": "AUTO_MAP", "bridge": "REDESIGN", "block": "BLOCK"}


@dataclass
class Finding:
    column: str
    hive_type: str
    target_type: str | None
    klass: str
    message: str


def classify_columns(columns: list[tuple[str, str]], source: str = "hive", char_policy: str = "preserve_padded",
                     string_only: bool = False, comments: dict[str, str] | None = None) -> list[ColumnPlan]:
    return [plan_column(n, t, source, char_policy, string_only, (comments or {}).get(n)) for n, t in columns]


def findings(plans: list[ColumnPlan]) -> list[Finding]:
    out = []
    for p in plans:
        for note in p.notes or ([] if p.action == "pass" else [p.action]):
            out.append(Finding(p.name, p.source_type, p.target_type, CLASS_OF[p.action], note))
    return out


def loadable(plans: list[ColumnPlan]) -> bool:
    """A table with any open REDESIGN / BLOCK column is not loadable (exit 4)."""
    return table_action(plans) in ("pass", "convert")
