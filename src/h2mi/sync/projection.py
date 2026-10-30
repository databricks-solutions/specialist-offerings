"""The single typed-projection builder (design §8.5, TYP-02): casts, CHAR policy, NTZ and VOID handling live here
and nowhere else. Pure helpers are unit-tested; `build` needs a DataFrame."""
from __future__ import annotations

from ..scanner.typemap import ColumnPlan
from ..util.errors import BlockedError


def projection_exprs(plans: list[ColumnPlan]) -> list[str]:
    bad = [p.name for p in plans if p.expr is None]
    if bad:
        raise BlockedError(f"columns without a projection (REDESIGN / BLOCK): {bad}")
    return [p.expr for p in plans]


def build(df, plans: list[ColumnPlan]):
    return df.selectExpr(*projection_exprs(plans))
