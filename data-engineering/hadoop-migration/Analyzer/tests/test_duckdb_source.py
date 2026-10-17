"""Tests for DuckDB source parser."""

import os
import tempfile
import unittest

import duckdb

from analyzer.models import WorkloadType
from analyzer.parsers.duckdb_source import load_from_duckdb


class TestDuckDBSource(unittest.TestCase):
    """Test load_from_duckdb() with an in-memory DuckDB populated with known data."""

    def setUp(self):
        """Create a temp DuckDB file with test tables and data."""
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".duckdb")
        os.close(self.db_fd)
        os.unlink(self.db_path)  # DuckDB needs to create the file itself
        conn = duckdb.connect(self.db_path)

        # Create yarn_applications table
        conn.execute("""
            CREATE TABLE yarn_applications (
                application_id VARCHAR PRIMARY KEY,
                name VARCHAR,
                "user" VARCHAR,
                queue VARCHAR,
                state VARCHAR,
                final_status VARCHAR,
                application_type VARCHAR,
                started_time BIGINT,
                finished_time BIGINT,
                elapsed_time_ms BIGINT,
                memory_seconds BIGINT,
                vcore_seconds BIGINT,
                diagnostics VARCHAR
            )
        """)

        # Insert test YARN apps matching the fixture data patterns
        conn.execute("""
            INSERT INTO yarn_applications VALUES
            ('application_1234567890_0001',
             'SELECT department, COUNT(*) FROM employees GROUP BY department(Stage-1)',
             'dataeng', 'root.production', 'FINISHED', 'SUCCEEDED', 'MAPREDUCE',
             1700000000000, 1700000010000, 10000, 15000, 30, ''),
            ('application_1234567890_0002',
             'PySpark ETL Job', 'dataeng', 'root.production', 'FINISHED', 'SUCCEEDED',
             'SPARK', 1700000020000, 1700000050000, 30000, 90000, 80, ''),
            ('application_1234567890_0003',
             'customers.jar', 'dataeng', 'root.batch', 'FINISHED', 'SUCCEEDED',
             'MAPREDUCE', 1700000060000, 1700000070000, 10000, 12000, 22, ''),
            ('application_1234567890_0004',
             'oozie:launcher:T=spark:W=etl-daily:A=spark-transform:ID=0000001-230101-C@1',
             'oozie', 'root.production', 'FINISHED', 'SUCCEEDED', 'MAPREDUCE',
             1700000080000, 1700000090000, 10000, 11000, 20, ''),
            ('application_1234567890_0005',
             'org.apache.spark.examples.SparkPi', 'dataeng', 'root.default',
             'FINISHED', 'FAILED', 'SPARK',
             1700000100000, 1700000110000, 10000, 20000, 15,
             'java.lang.NumberFormatException')
        """)

        # Create spark_applications table
        conn.execute("""
            CREATE TABLE spark_applications (
                application_id VARCHAR,
                attempt_id VARCHAR,
                name VARCHAR,
                spark_user VARCHAR,
                start_time VARCHAR,
                end_time VARCHAR,
                duration_ms BIGINT,
                completed BOOLEAN,
                PRIMARY KEY (application_id, attempt_id)
            )
        """)

        conn.execute("""
            INSERT INTO spark_applications VALUES
            ('application_1234567890_0002', '1', 'PySpark ETL Job', 'dataeng',
             '2023-11-14T00:00:20.000', '2023-11-14T00:00:50.000', 30000, true),
            ('application_1234567890_0005', '1', 'org.apache.spark.examples.SparkPi',
             'dataeng', '2023-11-14T00:01:40.000', '2023-11-14T00:01:50.000', 10000, false),
            ('application_spark_only_001', '1', 'my_etl_job.py', 'analyst',
             '2023-11-14T00:02:00.000', '2023-11-14T00:02:30.000', 30000, true),
            ('application_pyspark_001', '1', 'PySpark Shell', 'analyst',
             '2023-11-14T00:03:00.000', '2023-11-14T00:03:30.000', 30000, true)
        """)

        # Create impala_queries table
        conn.execute("""
            CREATE TABLE impala_queries (
                query_id VARCHAR PRIMARY KEY,
                statement TEXT,
                query_type VARCHAR,
                query_state VARCHAR,
                "user" VARCHAR,
                database_name VARCHAR,
                start_time VARCHAR,
                end_time VARCHAR,
                duration_millis BIGINT,
                duration_minutes DOUBLE,
                rows_produced BIGINT,
                coordinator VARCHAR
            )
        """)

        # Create yarn_analysis_vw (enriched view with cost fields)
        conn.execute("""
            CREATE TABLE yarn_analysis_vw AS
            SELECT
                ya.*,
                CAST(ya.memory_seconds AS DOUBLE) / 3600.0 / 1024.0 AS memory_gb_hours,
                CAST(ya.vcore_seconds AS DOUBLE) / 3600.0 AS vcore_hours,
                CAST(ya.elapsed_time_ms AS DOUBLE) / 60000.0 AS elapsed_time_mins,
                CASE
                    WHEN ya.application_type = 'SPARK' THEN 'Spark'
                    WHEN ya.name LIKE 'oozie:launcher:%' THEN 'Oozie Launcher'
                    ELSE 'MapReduce'
                END AS job_type,
                CAST(ya.memory_seconds AS DOUBLE) / 3600.0 / 1024.0 * 0.15 AS dollar_dbus,
                CAST(ya.memory_seconds AS DOUBLE) / 3600.0 / 1024.0 * 0.10 AS dollar_vm,
                (CAST(ya.memory_seconds AS DOUBLE) / 3600.0 / 1024.0 * 0.15) +
                (CAST(ya.memory_seconds AS DOUBLE) / 3600.0 / 1024.0 * 0.10) AS total_cost
            FROM yarn_applications ya
        """)

        # Create summary tables
        conn.execute("""
            CREATE TABLE workload_summary_by_type AS
            SELECT job_type, COUNT(*) AS total_jobs,
                   AVG(elapsed_time_mins) AS avg_duration_mins,
                   SUM(memory_gb_hours) AS total_memory_gb_hours,
                   SUM(total_cost) AS total_cost
            FROM yarn_analysis_vw GROUP BY job_type
        """)

        conn.execute("""
            CREATE TABLE workload_summary_by_user AS
            SELECT "user", COUNT(*) AS total_jobs,
                   COUNT(DISTINCT queue) AS queues_used,
                   SUM(memory_gb_hours) AS total_memory_gb_hours,
                   SUM(vcore_hours) AS total_vcore_hours,
                   SUM(total_cost) AS total_cost,
                   AVG(elapsed_time_mins) AS avg_duration_mins
            FROM yarn_analysis_vw GROUP BY "user"
        """)

        conn.execute("""
            CREATE TABLE workload_summary_by_queue AS
            SELECT queue, COUNT(*) AS total_jobs,
                   COUNT(DISTINCT "user") AS unique_users,
                   SUM(memory_gb_hours) AS total_memory_gb_hours,
                   SUM(vcore_hours) AS total_vcore_hours,
                   SUM(total_cost) AS total_cost
            FROM yarn_analysis_vw GROUP BY queue
        """)

        conn.execute("""
            CREATE TABLE hourly_yarn_view (
                hour_bucket VARCHAR,
                total_apps BIGINT,
                total_memory_gb_hours DOUBLE,
                total_vcore_hours DOUBLE,
                total_cost DOUBLE,
                unique_users BIGINT,
                unique_queues BIGINT
            )
        """)
        conn.execute("""
            INSERT INTO hourly_yarn_view VALUES
            ('2023-11-14 00:00:00', 3, 0.05, 0.12, 0.013, 2, 2),
            ('2023-11-14 01:00:00', 2, 0.03, 0.08, 0.007, 1, 1)
        """)

        conn.execute("""
            INSERT INTO impala_queries VALUES
            ('query_001', 'SELECT * FROM sales WHERE year = 2023', 'QUERY', 'FINISHED',
             'analyst', 'retail_db', '2023-11-14 00:00:00', '2023-11-14 00:00:05',
             5000, 0.083, 1500, 'host1:22000'),
            ('query_002', 'INSERT INTO summary SELECT region, SUM(amount) FROM sales GROUP BY region',
             'DML', 'FINISHED', 'etl_user', 'retail_db',
             '2023-11-14 00:01:00', '2023-11-14 00:01:10', 10000, 0.167, 50, 'host1:22000')
        """)

        conn.close()

    def tearDown(self):
        os.unlink(self.db_path)

    def test_load_yarn_count(self):
        yarn, spark, impala, _ = load_from_duckdb(self.db_path)
        self.assertEqual(len(yarn), 5)

    def test_load_spark_count(self):
        yarn, spark, impala, _ = load_from_duckdb(self.db_path)
        self.assertEqual(len(spark), 4)

    def test_load_impala_count(self):
        yarn, spark, impala, _ = load_from_duckdb(self.db_path)
        self.assertEqual(len(impala), 2)

    def test_yarn_hive_classification(self):
        yarn, _, _, _ = load_from_duckdb(self.db_path)
        hive_item = next(i for i in yarn if i.workload_id == "application_1234567890_0001")
        self.assertEqual(hive_item.workload_type, WorkloadType.HIVE)
        self.assertIn("hive-initiated", hive_item.tags)

    def test_yarn_spark_classification(self):
        yarn, _, _, _ = load_from_duckdb(self.db_path)
        spark_item = next(i for i in yarn if i.workload_id == "application_1234567890_0002")
        self.assertEqual(spark_item.workload_type, WorkloadType.SPARK)

    def test_yarn_mapreduce_jar(self):
        yarn, _, _, _ = load_from_duckdb(self.db_path)
        mr_item = next(i for i in yarn if i.workload_id == "application_1234567890_0003")
        self.assertEqual(mr_item.workload_type, WorkloadType.MAPREDUCE)
        self.assertEqual(mr_item.entry_point, "customers.jar")
        self.assertEqual(len(mr_item.code_artifacts), 1)
        self.assertEqual(mr_item.code_artifacts[0].artifact_type, "jar")

    def test_yarn_oozie_launched(self):
        yarn, _, _, _ = load_from_duckdb(self.db_path)
        oozie_item = next(i for i in yarn if i.workload_id == "application_1234567890_0004")
        self.assertEqual(oozie_item.workload_type, WorkloadType.SPARK)
        self.assertIn("oozie-launched", oozie_item.tags)
        self.assertEqual(oozie_item.oozie_workflow_name, "etl-daily")

    def test_yarn_failed_app_metadata(self):
        yarn, _, _, _ = load_from_duckdb(self.db_path)
        failed = next(i for i in yarn if i.workload_id == "application_1234567890_0005")
        self.assertEqual(failed.final_status, "FAILED")
        self.assertEqual(failed.diagnostics, "java.lang.NumberFormatException")

    def test_yarn_elapsed_time_mapping(self):
        """elapsed_time_ms in DuckDB maps to elapsed_time in the model."""
        yarn, _, _, _ = load_from_duckdb(self.db_path)
        item = next(i for i in yarn if i.workload_id == "application_1234567890_0001")
        self.assertEqual(item.elapsed_time, 10000)

    def test_yarn_source_is_yarn(self):
        yarn, _, _, _ = load_from_duckdb(self.db_path)
        for item in yarn:
            self.assertEqual(item.source, "yarn")

    def test_spark_entry_point_class(self):
        """Spark app with a class name should set entry_point."""
        _, spark, _, _ = load_from_duckdb(self.db_path)
        pi_item = next(i for i in spark if i.workload_id == "application_1234567890_0005")
        self.assertEqual(pi_item.entry_point, "org.apache.spark.examples.SparkPi")

    def test_spark_entry_point_py(self):
        """Spark app with .py name should set entry_point and artifact."""
        _, spark, _, _ = load_from_duckdb(self.db_path)
        py_item = next(i for i in spark if i.workload_id == "application_spark_only_001")
        self.assertEqual(py_item.entry_point, "my_etl_job.py")
        self.assertEqual(len(py_item.code_artifacts), 1)
        self.assertEqual(py_item.code_artifacts[0].artifact_type, "py")

    def test_spark_pyspark_tag(self):
        _, spark, _, _ = load_from_duckdb(self.db_path)
        ps_item = next(i for i in spark if i.workload_id == "application_pyspark_001")
        self.assertIn("pyspark", ps_item.tags)

    def test_spark_source(self):
        _, spark, _, _ = load_from_duckdb(self.db_path)
        for item in spark:
            self.assertEqual(item.source, "spark_hs")

    def test_impala_embedded_sql(self):
        _, _, impala, _ = load_from_duckdb(self.db_path)
        q1 = next(i for i in impala if i.workload_id == "query_001")
        self.assertEqual(len(q1.code_artifacts), 1)
        self.assertEqual(q1.code_artifacts[0].location_type, "embedded")
        self.assertEqual(q1.code_artifacts[0].artifact_type, "sql")
        self.assertIn("SELECT * FROM sales", q1.code_artifacts[0].path)

    def test_impala_database_mapping(self):
        """database_name in DuckDB maps to database in the model."""
        _, _, impala, _ = load_from_duckdb(self.db_path)
        q1 = next(i for i in impala if i.workload_id == "query_001")
        self.assertEqual(q1.database, "retail_db")

    def test_impala_query_type_tag(self):
        _, _, impala, _ = load_from_duckdb(self.db_path)
        q1 = next(i for i in impala if i.workload_id == "query_001")
        self.assertIn("query_type:query", q1.tags)
        q2 = next(i for i in impala if i.workload_id == "query_002")
        self.assertIn("query_type:dml", q2.tags)

    def test_impala_rows_and_duration(self):
        _, _, impala, _ = load_from_duckdb(self.db_path)
        q1 = next(i for i in impala if i.workload_id == "query_001")
        self.assertEqual(q1.rows_produced, 1500)
        self.assertEqual(q1.duration_millis, 5000)

    def test_impala_source(self):
        _, _, impala, _ = load_from_duckdb(self.db_path)
        for item in impala:
            self.assertEqual(item.source, "impala")

    # --- Enriched fields from yarn_analysis_vw ---

    def test_yarn_enriched_job_type(self):
        """Items should have job_type from yarn_analysis_vw."""
        yarn, _, _, _ = load_from_duckdb(self.db_path)
        spark_item = next(i for i in yarn if i.workload_id == "application_1234567890_0002")
        self.assertEqual(spark_item.job_type, "Spark")
        mr_item = next(i for i in yarn if i.workload_id == "application_1234567890_0003")
        self.assertEqual(mr_item.job_type, "MapReduce")
        oozie_item = next(i for i in yarn if i.workload_id == "application_1234567890_0004")
        self.assertEqual(oozie_item.job_type, "Oozie Launcher")

    def test_yarn_enriched_cost_fields(self):
        """Items should have cost/normalized fields from yarn_analysis_vw."""
        yarn, _, _, _ = load_from_duckdb(self.db_path)
        item = next(i for i in yarn if i.workload_id == "application_1234567890_0001")
        self.assertIsNotNone(item.memory_gb_hours)
        self.assertIsNotNone(item.vcore_hours)
        self.assertIsNotNone(item.elapsed_time_mins)
        self.assertIsNotNone(item.dollar_dbus)
        self.assertIsNotNone(item.dollar_vm)
        self.assertIsNotNone(item.total_cost)
        self.assertGreater(item.memory_gb_hours, 0)
        self.assertGreater(item.total_cost, 0)

    def test_yarn_enriched_to_dict(self):
        """Enriched fields should appear in to_dict() output."""
        yarn, _, _, _ = load_from_duckdb(self.db_path)
        item = next(i for i in yarn if i.workload_id == "application_1234567890_0002")
        d = item.to_dict()
        self.assertIn("job_type", d)
        self.assertIn("memory_gb_hours", d)
        self.assertIn("total_cost", d)

    # --- Summaries ---

    def test_summaries_returned(self):
        """load_from_duckdb should return summaries dict as 4th element."""
        _, _, _, summaries = load_from_duckdb(self.db_path)
        self.assertIn("by_job_type", summaries)
        self.assertIn("by_user", summaries)
        self.assertIn("by_queue", summaries)
        self.assertIn("demand_profile", summaries)

    def test_summary_by_job_type(self):
        _, _, _, summaries = load_from_duckdb(self.db_path)
        by_type = summaries["by_job_type"]
        self.assertGreater(len(by_type), 0)
        # Each entry should have required fields
        entry = by_type[0]
        self.assertIn("job_type", entry)
        self.assertIn("total_jobs", entry)
        self.assertIn("total_cost", entry)

    def test_summary_by_user(self):
        _, _, _, summaries = load_from_duckdb(self.db_path)
        by_user = summaries["by_user"]
        self.assertGreater(len(by_user), 0)
        self.assertIn("user", by_user[0])

    def test_summary_by_queue(self):
        _, _, _, summaries = load_from_duckdb(self.db_path)
        by_queue = summaries["by_queue"]
        self.assertGreater(len(by_queue), 0)
        self.assertIn("queue", by_queue[0])

    def test_demand_profile(self):
        _, _, _, summaries = load_from_duckdb(self.db_path)
        demand = summaries["demand_profile"]
        self.assertEqual(len(demand), 2)
        self.assertIn("hour_bucket", demand[0])
        self.assertIn("total_apps", demand[0])


class TestDuckDBSourceFallback(unittest.TestCase):
    """Test fallback to yarn_applications when yarn_analysis_vw is missing."""

    def test_fallback_no_enriched_fields(self):
        """Without yarn_analysis_vw, items should have None for cost fields."""
        fd, path = tempfile.mkstemp(suffix=".duckdb")
        os.close(fd)
        os.unlink(path)
        try:
            conn = duckdb.connect(path)
            conn.execute("""
                CREATE TABLE yarn_applications (
                    application_id VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    "user" VARCHAR,
                    queue VARCHAR,
                    state VARCHAR,
                    final_status VARCHAR,
                    application_type VARCHAR,
                    started_time BIGINT,
                    finished_time BIGINT,
                    elapsed_time_ms BIGINT,
                    memory_seconds BIGINT,
                    vcore_seconds BIGINT,
                    diagnostics VARCHAR
                )
            """)
            conn.execute("""
                INSERT INTO yarn_applications VALUES
                ('app_001', 'test job', 'user1', 'default', 'FINISHED', 'SUCCEEDED',
                 'SPARK', 1700000000000, 1700000010000, 10000, 5000, 10, '')
            """)
            conn.close()

            yarn, _, _, summaries = load_from_duckdb(path)
            self.assertEqual(len(yarn), 1)
            self.assertIsNone(yarn[0].job_type)
            self.assertIsNone(yarn[0].total_cost)
            # Summaries should be empty dicts
            self.assertEqual(summaries["by_job_type"], [])
            self.assertEqual(summaries["demand_profile"], [])
        finally:
            os.unlink(path)


class TestDuckDBSourceMissingTables(unittest.TestCase):
    """Test behavior when tables are missing from DuckDB."""

    def test_empty_db_returns_empty_lists(self):
        fd, path = tempfile.mkstemp(suffix=".duckdb")
        os.close(fd)
        os.unlink(path)
        try:
            conn = duckdb.connect(path)
            conn.close()
            yarn, spark, impala, _ = load_from_duckdb(path)
            self.assertEqual(yarn, [])
            self.assertEqual(spark, [])
            self.assertEqual(impala, [])
        finally:
            os.unlink(path)

    def test_partial_tables(self):
        """Only yarn_applications exists — spark and impala return empty."""
        fd, path = tempfile.mkstemp(suffix=".duckdb")
        os.close(fd)
        os.unlink(path)
        try:
            conn = duckdb.connect(path)
            conn.execute("""
                CREATE TABLE yarn_applications (
                    application_id VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    "user" VARCHAR,
                    queue VARCHAR,
                    state VARCHAR,
                    final_status VARCHAR,
                    application_type VARCHAR,
                    started_time BIGINT,
                    finished_time BIGINT,
                    elapsed_time_ms BIGINT,
                    memory_seconds BIGINT,
                    vcore_seconds BIGINT,
                    diagnostics VARCHAR
                )
            """)
            conn.execute("""
                INSERT INTO yarn_applications VALUES
                ('app_001', 'test job', 'user1', 'default', 'FINISHED', 'SUCCEEDED',
                 'SPARK', 1700000000000, 1700000010000, 10000, 5000, 10, '')
            """)
            conn.close()

            yarn, spark, impala, _ = load_from_duckdb(path)
            self.assertEqual(len(yarn), 1)
            self.assertEqual(spark, [])
            self.assertEqual(impala, [])
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
