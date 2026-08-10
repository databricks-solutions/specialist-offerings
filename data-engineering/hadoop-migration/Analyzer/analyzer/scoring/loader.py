"""Load complexity rule definitions from YAML files."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class SignalRule:
    id: str
    tier: str
    description: str
    informational: bool = False
    detect: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EscalationRule:
    id: str
    description: str
    set_tier: str
    when: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ComplexityRules:
    language: str
    default_tier: str
    tier_order: List[str]
    signals: List[SignalRule]
    escalation: List[EscalationRule]
    actions: Dict[str, List[str]]


def load_rules(rules_path: str) -> ComplexityRules:
    """Load a complexity rules YAML file."""
    with open(rules_path, "r") as f:
        raw = yaml.safe_load(f)

    signals = [
        SignalRule(
            id=s["id"],
            tier=s["tier"],
            description=s.get("description", ""),
            informational=s.get("informational", False),
            detect=s.get("detect", {}),
        )
        for s in raw.get("signals", [])
    ]

    escalation = [
        EscalationRule(
            id=e["id"],
            description=e.get("description", ""),
            set_tier=e["set_tier"],
            when=e.get("when", {}),
        )
        for e in raw.get("escalation", [])
    ]

    return ComplexityRules(
        language=raw.get("language", "unknown"),
        default_tier=raw.get("default_tier", "easy"),
        tier_order=raw.get("tier_order", ["easy", "medium", "hard", "very_hard"]),
        signals=signals,
        escalation=escalation,
        actions=raw.get("actions", {}),
    )


def load_rules_for_language(rules_dir: str, language: str) -> Optional[ComplexityRules]:
    """Load rules file for a language (e.g. pyspark -> pyspark.yaml)."""
    path = os.path.join(rules_dir, f"{language}.yaml")
    if not os.path.isfile(path):
        return None
    return load_rules(path)
