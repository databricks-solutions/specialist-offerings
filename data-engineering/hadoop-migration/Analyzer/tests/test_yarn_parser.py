"""Tests for YARN application dump parser."""

import json
import os
import tempfile
import unittest

from analyzer.models import WorkloadType
from analyzer.parsers.yarn_parser import parse_yarn_dump


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


class TestYarnParser(unittest.TestCase):

    def test_parse_sample(self):
        items = parse_yarn_dump(os.path.join(FIXTURES_DIR, "yarn_sample.json"))
        self.assertEqual(len(items), 5)

    def test_hive_classification(self):
        items = parse_yarn_dump(os.path.join(FIXTURES_DIR, "yarn_sample.json"))
        hive_item = next(i for i in items if i.workload_id == "application_1234567890_0001")
        self.assertEqual(hive_item.workload_type, WorkloadType.HIVE)
        self.assertIn("hive-initiated", hive_item.tags)

    def test_spark_classification(self):
        items = parse_yarn_dump(os.path.join(FIXTURES_DIR, "yarn_sample.json"))
        spark_item = next(i for i in items if i.workload_id == "application_1234567890_0002")
        self.assertEqual(spark_item.workload_type, WorkloadType.SPARK)

    def test_mapreduce_jar_classification(self):
        items = parse_yarn_dump(os.path.join(FIXTURES_DIR, "yarn_sample.json"))
        mr_item = next(i for i in items if i.workload_id == "application_1234567890_0003")
        self.assertEqual(mr_item.workload_type, WorkloadType.MAPREDUCE)
        self.assertEqual(mr_item.entry_point, "customers.jar")
        self.assertEqual(len(mr_item.code_artifacts), 1)
        self.assertEqual(mr_item.code_artifacts[0].artifact_type, "jar")

    def test_oozie_launched_detection(self):
        items = parse_yarn_dump(os.path.join(FIXTURES_DIR, "yarn_sample.json"))
        oozie_item = next(i for i in items if i.workload_id == "application_1234567890_0004")
        self.assertEqual(oozie_item.workload_type, WorkloadType.SPARK)
        self.assertIn("oozie-launched", oozie_item.tags)
        self.assertEqual(oozie_item.oozie_workflow_name, "etl-daily")

    def test_failed_app_metadata(self):
        items = parse_yarn_dump(os.path.join(FIXTURES_DIR, "yarn_sample.json"))
        failed_item = next(i for i in items if i.workload_id == "application_1234567890_0005")
        self.assertEqual(failed_item.final_status, "FAILED")
        self.assertEqual(failed_item.diagnostics, "java.lang.NumberFormatException")

    def test_source_is_yarn(self):
        items = parse_yarn_dump(os.path.join(FIXTURES_DIR, "yarn_sample.json"))
        for item in items:
            self.assertEqual(item.source, "yarn")

    def test_empty_dump(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"apps": {"app": []}}, f)
            f.flush()
            items = parse_yarn_dump(f.name)
        os.unlink(f.name)
        self.assertEqual(len(items), 0)


if __name__ == "__main__":
    unittest.main()
