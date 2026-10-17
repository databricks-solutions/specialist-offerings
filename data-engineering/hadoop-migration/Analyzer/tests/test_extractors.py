"""Tests for Oozie workflow XML extractors."""

import os
import unittest
import xml.etree.ElementTree as ET

from analyzer.extractors.spark_extractor import extract_spark_action
from analyzer.extractors.hive_extractor import extract_hive_action
from analyzer.extractors.sqoop_extractor import extract_sqoop_action
from analyzer.extractors.shell_extractor import extract_shell_action
from analyzer.extractors.mr_extractor import extract_mr_action
from analyzer.extractors.subworkflow_extractor import extract_subworkflow_action


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


class TestSparkExtractor(unittest.TestCase):

    def test_extract_from_workflow_xml(self):
        tree = ET.parse(os.path.join(FIXTURES_DIR, "workflow_spark.xml"))
        root = tree.getroot()

        # Find the spark action (namespace-agnostic)
        spark_elem = None
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag == "spark":
                spark_elem = elem
                break

        self.assertIsNotNone(spark_elem)
        result = extract_spark_action(spark_elem, "/user/oozie/workflows/spark-etl")

        self.assertEqual(result["jar"], "lib/etl-transform.jar")
        self.assertEqual(result["main_class"], "com.example.etl.SparkTransform")
        self.assertTrue(len(result["artifacts"]) >= 1)
        # Check that --jars dependencies were extracted
        dep_paths = [d.path for d in result["dependencies"]]
        self.assertTrue(any("utils.jar" in p for p in dep_paths))


class TestHiveExtractor(unittest.TestCase):

    def test_extract_hive_script(self):
        tree = ET.parse(os.path.join(FIXTURES_DIR, "workflow_hive.xml"))
        root = tree.getroot()

        hive_elem = None
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag == "hive":
                hive_elem = elem
                break

        self.assertIsNotNone(hive_elem)
        result = extract_hive_action(hive_elem, "/user/oozie/workflows/hive-etl")

        self.assertEqual(result["script"], "scripts/load_data.hql")
        self.assertEqual(len(result["artifacts"]), 1)
        self.assertEqual(result["artifacts"][0].artifact_type, "hql")

    def test_extract_hive2_query(self):
        tree = ET.parse(os.path.join(FIXTURES_DIR, "workflow_hive.xml"))
        root = tree.getroot()

        hive2_elem = None
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag == "hive2":
                hive2_elem = elem
                break

        self.assertIsNotNone(hive2_elem)
        result = extract_hive_action(hive2_elem, "/user/oozie/workflows/hive-etl")

        self.assertIn("INSERT OVERWRITE", result["query"])
        self.assertEqual(len(result["artifacts"]), 1)
        self.assertEqual(result["artifacts"][0].location_type, "embedded")


class TestSqoopExtractor(unittest.TestCase):

    def test_extract_sqoop_args(self):
        xml = """<sqoop xmlns="uri:oozie:sqoop-action:0.4">
            <arg>import</arg>
            <arg>--connect</arg>
            <arg>jdbc:mysql://db:3306/mydb</arg>
            <arg>--table</arg>
            <arg>customers</arg>
            <arg>--target-dir</arg>
            <arg>/data/raw/customers</arg>
        </sqoop>"""
        elem = ET.fromstring(xml)
        result = extract_sqoop_action(elem)

        self.assertEqual(result["jdbc_url"], "jdbc:mysql://db:3306/mydb")
        self.assertEqual(result["table"], "customers")
        self.assertEqual(result["target_dir"], "/data/raw/customers")


class TestShellExtractor(unittest.TestCase):

    def test_extract_shell(self):
        xml = """<shell xmlns="uri:oozie:shell-action:0.3">
            <exec>run_etl.sh</exec>
            <file>lib/helper.py</file>
        </shell>"""
        elem = ET.fromstring(xml)
        result = extract_shell_action(elem, "/user/oozie/workflows/shell-job")

        self.assertEqual(result["exec_script"], "run_etl.sh")
        self.assertEqual(len(result["artifacts"]), 1)
        self.assertEqual(result["artifacts"][0].artifact_type, "sh")
        self.assertEqual(len(result["dependencies"]), 1)


class TestMRExtractor(unittest.TestCase):

    def test_extract_mr_config(self):
        xml = """<map-reduce>
            <configuration>
                <property>
                    <name>mapred.jar</name>
                    <value>/lib/wordcount.jar</value>
                </property>
                <property>
                    <name>mapreduce.job.map.class</name>
                    <value>com.example.WordCountMapper</value>
                </property>
                <property>
                    <name>mapreduce.job.reduce.class</name>
                    <value>com.example.WordCountReducer</value>
                </property>
            </configuration>
        </map-reduce>"""
        elem = ET.fromstring(xml)
        result = extract_mr_action(elem)

        self.assertEqual(result["jar"], "/lib/wordcount.jar")
        self.assertEqual(result["mapper_class"], "com.example.WordCountMapper")
        self.assertEqual(result["reducer_class"], "com.example.WordCountReducer")
        self.assertEqual(len(result["artifacts"]), 1)


class TestSubWorkflowExtractor(unittest.TestCase):

    def test_extract_subworkflow(self):
        xml = """<sub-workflow>
            <app-path>/user/oozie/workflows/child-wf</app-path>
            <propagate-configuration/>
        </sub-workflow>"""
        elem = ET.fromstring(xml)
        result = extract_subworkflow_action(elem)

        self.assertEqual(result["app_path"], "/user/oozie/workflows/child-wf")
        self.assertTrue(result["propagate_configuration"])


if __name__ == "__main__":
    unittest.main()
