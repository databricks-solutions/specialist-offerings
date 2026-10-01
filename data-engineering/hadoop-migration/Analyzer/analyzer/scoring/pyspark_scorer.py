"""PySpark complexity scoring via AST inspection and regex."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from typing import List, Optional, Set

from analyzer.scoring.loader import ComplexityRules, SignalRule


@dataclass
class ScoreResult:
    tier: str
    signals: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)
    recommended_actions: List[str] = field(default_factory=list)


def score_pyspark_source(source: str, rules: ComplexityRules) -> ScoreResult:
    """Score PySpark source code against rule definitions."""
    tree: Optional[ast.AST] = None
    try:
        tree = ast.parse(source)
    except SyntaxError:
        # Python 2 or invalid syntax — regex-only pass still applies.
        pass

    matched: List[SignalRule] = []
    for signal in rules.signals:
        if _signal_matches(signal, source, tree):
            matched.append(signal)

    tier_signals = [s for s in matched if not s.informational]
    if tier_signals:
        tier = _max_tier([s.tier for s in tier_signals], rules.tier_order)
    else:
        tier = rules.default_tier

    signal_ids = {s.id for s in matched}
    # MVP: escalation rules deferred — tune against customer corpus before enabling.
    # tier = _apply_escalation(tier, signal_ids, matched, rules)

    reasons = [s.description for s in matched if s.description and not s.informational]
    actions = list(rules.actions.get(tier, []))

    return ScoreResult(
        tier=tier,
        signals=sorted(signal_ids),
        reasons=reasons,
        recommended_actions=actions,
    )


def _max_tier(tiers: List[str], tier_order: List[str]) -> str:
    rank = {t: i for i, t in enumerate(tier_order)}
    return max(tiers, key=lambda t: rank.get(t, -1))


def _signal_matches(signal: SignalRule, source: str, tree: Optional[ast.AST]) -> bool:
    detect = signal.detect
    if detect.get("regex"):
        for pattern in detect["regex"]:
            if re.search(pattern, source, re.IGNORECASE | re.MULTILINE):
                return True

    if tree is None:
        return False

    if detect.get("ast_imports"):
        if _imports_match(tree, detect["ast_imports"]):
            return True

    if detect.get("ast_calls"):
        if _calls_match(tree, detect["ast_calls"]):
            return True

    return False


def _imports_match(tree: ast.AST, patterns: List[str]) -> bool:
    found: Set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                found.add(f"{module}.{alias.name}" if module else alias.name)
                if module:
                    found.add(module)

    for pattern in patterns:
        for name in found:
            if name == pattern or name.endswith("." + pattern.rsplit(".", 1)[-1]):
                return True
            if pattern in name:
                return True
    return False


def _calls_match(tree: ast.AST, call_rules: List[dict]) -> bool:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func_name, receiver = _call_target(node.func)
        if not func_name:
            continue
        for rule in call_rules:
            if rule.get("pattern") != func_name:
                continue
            receivers = rule.get("receivers")
            if receivers is None:
                return True
            if _receiver_allowed(receiver, receivers):
                return True
    return False


def _call_target(func: ast.expr) -> tuple[Optional[str], Optional[str]]:
    if isinstance(func, ast.Name):
        return func.id, None
    if isinstance(func, ast.Attribute):
        receiver = None
        if isinstance(func.value, ast.Name):
            receiver = func.value.id
        return func.attr, receiver
    return None, None


def _receiver_allowed(receiver: Optional[str], allowed: List[str]) -> bool:
    if "*" in allowed:
        return True
    if receiver is None:
        return False
    return receiver in allowed
