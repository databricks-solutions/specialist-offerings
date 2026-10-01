"""Tests for InventoryBuilder."""

import os
import unittest
from unittest.mock import MagicMock, patch

from analyzer.config import AnalyzerConfig, ProfilerOutputConfig, OozieConfig, OutputConfig, WebHDFSConfig
from analyzer.inventory import InventoryBuilder
from analyzer.models import WorkloadType


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


class TestInventoryBuilderProfiler(unittest.TestCase):

    def setUp(self):
        self.config = AnalyzerConfig(
            profiler_output=ProfilerOutputConfig(base_dir=FIXTURES_DIR + "/.."),
            oozie=OozieConfig(url=""),
            webhdfs=WebHDFSConfig(enabled=False),
            output=OutputConfig(dir="/tmp/test-output"),
        )

    def test_build_from_profiler_parses_yarn(self):
        """Test that YARN parser finds and parses the fixture.

        Note: fixtures are not in YARN/ subdirectory structure, so this
        tests the fallback behavior (empty result from find_and_parse).
        """
        config = AnalyzerConfig(
            profiler_output=ProfilerOutputConfig(base_dir=FIXTURES_DIR),
            oozie=OozieConfig(url=""),
            output=OutputConfig(dir="/tmp/test-output"),
        )
        builder = InventoryBuilder(config)
        # This won't find files since fixtures aren't in YARN/ subdir
        # but it should not error out
        items = builder.build_from_profiler()
        self.assertIsInstance(items, list)

    def test_merge_yarn_spark(self):
        """Test merging of YARN and Spark HS items."""
        from analyzer.models import WorkloadInventoryItem

        yarn_items = [
            WorkloadInventoryItem(
                workload_id="app_001",
                workload_name="Spark Job",
                workload_type=WorkloadType.SPARK,
                user="user1",
                queue="root.default",
                source="yarn",
            ),
        ]
        spark_items = [
            WorkloadInventoryItem(
                workload_id="app_001",
                workload_name="Spark Job",
                workload_type=WorkloadType.SPARK,
                user="user1",
                queue="",
                entry_point="com.example.Main",
                source="spark_hs",
                tags=["pyspark"],
            ),
        ]

        builder = InventoryBuilder(self.config)
        merged = builder._merge_yarn_spark(yarn_items, spark_items)

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0].source, "yarn+spark_hs")
        self.assertEqual(merged[0].entry_point, "com.example.Main")
        self.assertIn("pyspark", merged[0].tags)


if __name__ == "__main__":
    unittest.main()
