"""Smoke tests for legacy PySpark codemod and cluster-setup fixtures."""

from __future__ import annotations

import importlib.util
import os
import re
import unittest


CONVERTER_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(CONVERTER_ROOT, "tests", "input", "spark-to-databricks")
OUTPUT_DIR = os.path.join(CONVERTER_ROOT, "tests", "output", "spark-to-databricks")
CODEMOD_PATH = os.path.join(
    CONVERTER_ROOT,
    "skills",
    "spark-to-databricks",
    "scripts",
    "legacy_pyspark_codemod.py",
)


def _load_codemod():
    spec = importlib.util.spec_from_file_location("legacy_pyspark_codemod", CODEMOD_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


codemod = _load_codemod()

FIXTURES = (
    ("clickstream_transform.py", "clickstream_transform_databricks.py"),
    ("session_metrics.py", "session_metrics_databricks.py"),
)


class TestLegacyPySparkFixtures(unittest.TestCase):
    def test_input_fixtures_exist(self):
        for input_name, _ in FIXTURES:
            path = os.path.join(INPUT_DIR, input_name)
            self.assertTrue(os.path.isfile(path), f"Missing input fixture: {path}")

    def test_golden_outputs_exist(self):
        for _, output_name in FIXTURES:
            path = os.path.join(OUTPUT_DIR, output_name)
            self.assertTrue(os.path.isfile(path), f"Missing golden output: {path}")

    def test_input_fixtures_contain_legacy_patterns(self):
        with open(os.path.join(INPUT_DIR, "clickstream_transform.py"), encoding="utf-8") as f:
            clickstream = f.read()
        with open(os.path.join(INPUT_DIR, "session_metrics.py"), encoding="utf-8") as f:
            session = f.read()
        self.assertIn("HiveContext", clickstream)
        self.assertIn("hdfs://", clickstream)
        self.assertIn("HiveContext", session)


class TestLegacyPySparkCodemod(unittest.TestCase):
    def _convert(self, input_name: str) -> str:
        with open(os.path.join(INPUT_DIR, input_name), encoding="utf-8") as f:
            return codemod.convert_legacy_pyspark(f.read())

    def _read_golden(self, output_name: str) -> str:
        with open(os.path.join(OUTPUT_DIR, output_name), encoding="utf-8") as f:
            return f.read()

    @staticmethod
    def _normalize(text: str) -> str:
        return re.sub(r"\s+", " ", text.strip())

    def test_codemod_removes_legacy_patterns(self):
        for input_name, _ in FIXTURES:
            with self.subTest(fixture=input_name):
                converted = self._convert(input_name)
                violations = codemod.assert_no_legacy_patterns(converted)
                self.assertEqual(violations, [], f"Legacy patterns remain: {violations}")

    def test_codemod_matches_golden_output(self):
        for input_name, output_name in FIXTURES:
            with self.subTest(fixture=input_name):
                converted = self._convert(input_name)
                golden = self._read_golden(output_name)
                self.assertEqual(converted, golden)

    def test_clickstream_uc_migration_signals(self):
        converted = self._convert("clickstream_transform.py")
        self.assertIn("/Volumes/main/raw/clickstream", converted)
        self.assertIn('saveAsTable("main.retail_analytics.enriched_sessions")', converted)
        self.assertIn('spark.conf.set("spark.sql.shuffle.partitions", "10")', converted)

    def test_session_metrics_table_read_and_write(self):
        converted = self._convert("session_metrics.py")
        self.assertIn('spark.table("main.retail_analytics.enriched_sessions")', converted)
        self.assertIn('saveAsTable("main.retail_analytics.daily_session_aggregates")', converted)
        self.assertNotIn(".coalesce(1)", converted)


if __name__ == "__main__":
    unittest.main()
