"""Orchestrate complexity scoring across workload inventory items."""

from __future__ import annotations

import logging
import os
from typing import List, Optional

from analyzer.config import AnalyzerConfig
from analyzer.models import WorkloadInventoryItem, WorkloadType
from analyzer.scoring.loader import ComplexityRules, load_rules_for_language
from analyzer.scoring.pyspark_scorer import score_pyspark_source

logger = logging.getLogger(__name__)

CONVERT_COMMANDS = {
    WorkloadType.SPARK: "spark",
    WorkloadType.HIVE: "hive-sql",
    WorkloadType.SQOOP: "sqoop",
    WorkloadType.MAPREDUCE: "spark",
    WorkloadType.SHELL: "spark",
    WorkloadType.HBASE: "hbase",
    WorkloadType.IMPALA: "hive-sql",
}


class ComplexityScorer:
    """Score code complexity for inventory items using local rule files."""

    def __init__(self, config: AnalyzerConfig):
        self.config = config.complexity
        self._pyspark_rules: Optional[ComplexityRules] = None

    def _get_pyspark_rules(self) -> Optional[ComplexityRules]:
        if self._pyspark_rules is None:
            self._pyspark_rules = load_rules_for_language(
                self.config.rules_dir, "pyspark"
            )
        return self._pyspark_rules

    def score_all(self, items: List[WorkloadInventoryItem]) -> List[WorkloadInventoryItem]:
        """Score each inventory item; mutates and returns the same list."""
        if not self.config.enabled:
            return items

        if not self.config.local_code_dir:
            logger.warning(
                "complexity.enabled but local_code_dir is empty — skipping scoring"
            )
            return items

        scored = 0
        for item in items:
            if self._score_item(item):
                scored += 1

        logger.info("Complexity scoring complete: %d/%d items scored", scored, len(items))
        return items

    def _score_item(self, item: WorkloadInventoryItem) -> bool:
        py_artifacts = [
            a for a in item.code_artifacts if a.artifact_type == "py"
        ]
        if not py_artifacts and item.workload_type != WorkloadType.SPARK:
            return False

        if item.workload_type != WorkloadType.SPARK and not py_artifacts:
            return False

        rules = self._get_pyspark_rules()
        if rules is None:
            logger.error("PySpark rules not found in %s", self.config.rules_dir)
            return False

        best_tier: Optional[str] = None
        all_signals: List[str] = []
        all_reasons: List[str] = []
        all_actions: List[str] = []
        local_path: Optional[str] = None

        targets = py_artifacts if py_artifacts else []
        if not targets and item.entry_point and item.entry_point.endswith(".py"):
            from analyzer.models import CodeArtifact

            targets = [
                CodeArtifact(
                    path=item.entry_point,
                    location_type="hdfs",
                    artifact_type="py",
                )
            ]

        for artifact in targets:
            resolved = resolve_local_code_path(
                artifact.path, self.config.local_code_dir
            )
            if not resolved:
                logger.debug("No local file for artifact %s", artifact.path)
                continue

            try:
                with open(resolved, "r", encoding="utf-8", errors="replace") as f:
                    source = f.read()
            except OSError as exc:
                logger.warning("Cannot read %s: %s", resolved, exc)
                continue

            result = score_pyspark_source(source, rules)
            local_path = resolved

            if best_tier is None:
                best_tier = result.tier
            else:
                best_tier = _max_tier(
                    best_tier, result.tier, rules.tier_order
                )

            for sig in result.signals:
                if sig not in all_signals:
                    all_signals.append(sig)
            for reason in result.reasons:
                if reason not in all_reasons:
                    all_reasons.append(reason)
            for action in result.recommended_actions:
                if action not in all_actions:
                    all_actions.append(action)

        if best_tier is None:
            return False

        item.complexity = best_tier
        item.complexity_signals = all_signals
        item.complexity_reasons = all_reasons
        item.complexity_recommended_actions = all_actions
        item.local_code_path = local_path
        item.convert_command = build_convert_command(
            item.workload_type, local_path
        )
        return True


def resolve_local_code_path(hdfs_path: str, local_code_dir: str) -> Optional[str]:
    """Resolve an HDFS artifact path to a local file under local_code_dir."""
    if not local_code_dir:
        return None

    basename = os.path.basename(hdfs_path.split("?")[0])
    candidates = [
        os.path.join(local_code_dir, basename),
        os.path.join(local_code_dir, hdfs_path.lstrip("/")),
        os.path.join(local_code_dir, hdfs_path.replace("hdfs://", "").lstrip("/")),
    ]

    for path in candidates:
        if os.path.isfile(path):
            return path

    for root, _, files in os.walk(local_code_dir):
        if basename in files:
            return os.path.join(root, basename)

    return None


def build_convert_command(
    workload_type: WorkloadType, local_code_path: Optional[str]
) -> Optional[str]:
    """Build a /convert slash command for the Converter plugin."""
    convert_type = CONVERT_COMMANDS.get(workload_type)
    if not convert_type or not local_code_path:
        return None
    return f"/convert {convert_type} {local_code_path}"


def _max_tier(a: str, b: str, tier_order: List[str]) -> str:
    rank = {t: i for i, t in enumerate(tier_order)}
    return a if rank.get(a, -1) >= rank.get(b, -1) else b
