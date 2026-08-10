"""Tests for PySpark complexity scoring."""

import os
import unittest

from analyzer.config import AnalyzerConfig, ComplexityConfig, OutputConfig
from analyzer.models import CodeArtifact, WorkloadInventoryItem, WorkloadType
from analyzer.scoring.loader import load_rules_for_language
from analyzer.scoring.pyspark_scorer import score_pyspark_source
from analyzer.scoring.scorer import ComplexityScorer, resolve_local_code_path

RULES_DIR = os.path.join(
    os.path.dirname(__file__), "..", "complexity_rules"
)
CODE_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "code")


class TestPySparkScorer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.rules = load_rules_for_language(RULES_DIR, "pyspark")
        assert cls.rules is not None

    def _read(self, name: str) -> str:
        with open(os.path.join(CODE_DIR, name), "r", encoding="utf-8") as f:
            return f.read()

    def test_dataframe_only_is_easy(self):
        result = score_pyspark_source(self._read("dataframe_only.py"), self.rules)
        self.assertEqual(result.tier, "easy")

    def test_clickstream_is_medium(self):
        result = score_pyspark_source(
            self._read("clickstream_transform.py"), self.rules
        )
        self.assertEqual(result.tier, "medium")
        self.assertIn("legacy_hive_context", result.signals)
        self.assertIn("hdfs_paths", result.signals)

    def test_session_metrics_is_medium(self):
        result = score_pyspark_source(self._read("session_metrics.py"), self.rules)
        self.assertEqual(result.tier, "medium")
        self.assertIn("coalesce_single_file", result.signals)

    def test_rdd_wordcount_is_hard(self):
        result = score_pyspark_source(self._read("rdd_wordcount.py"), self.rules)
        self.assertEqual(result.tier, "hard")
        self.assertIn("rdd_api", result.signals)

    def test_resolve_local_code_path_by_basename(self):
        path = resolve_local_code_path(
            "hdfs:///user/oozie/workflows/clickstream_transform.py", CODE_DIR
        )
        self.assertTrue(path.endswith("clickstream_transform.py"))
        self.assertTrue(os.path.isfile(path))


class TestComplexityScorerIntegration(unittest.TestCase):

    def test_score_inventory_item(self):
        config = AnalyzerConfig(
            complexity=ComplexityConfig(
                enabled=True,
                rules_dir=RULES_DIR,
                local_code_dir=CODE_DIR,
            ),
            output=OutputConfig(dir="/tmp/test-analyzer-output"),
        )
        item = WorkloadInventoryItem(
            workload_id="app-001",
            workload_name="ClickstreamTransform",
            workload_type=WorkloadType.SPARK,
            user="etl",
            queue="default",
            memory_seconds=500000,
            yarn_app_id="application_123_0001",
            code_artifacts=[
                CodeArtifact(
                    path="hdfs:///user/oozie/clickstream_transform.py",
                    location_type="hdfs",
                    artifact_type="py",
                )
            ],
        )
        ComplexityScorer(config).score_all([item])

        self.assertEqual(item.complexity, "medium")
        self.assertTrue(item.convert_command.startswith("/convert spark"))
        self.assertIn("clickstream_transform.py", item.local_code_path or "")


if __name__ == "__main__":
    unittest.main()
