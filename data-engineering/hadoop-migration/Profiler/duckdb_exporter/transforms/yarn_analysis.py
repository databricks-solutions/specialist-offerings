"""Derived YARN analysis tables."""
import logging

logger = logging.getLogger(__name__)


def create_yarn_analysis_vw(conn, dbu_rate: float = 0.15, vm_rate: float = 0.10) -> int:
    """Create yarn_analysis_vw with job type classification and cost estimates."""
    conn.execute("DROP TABLE IF EXISTS yarn_analysis_vw")
    conn.execute(f"""
        CREATE TABLE yarn_analysis_vw AS
        SELECT
            *,
            CAST(memory_seconds AS DOUBLE) / 3600.0 / 1024.0 AS memory_gb_hours,
            CAST(vcore_seconds AS DOUBLE) / 3600.0 AS vcore_hours,
            CAST(elapsed_time_ms AS DOUBLE) / 60000.0 AS elapsed_time_mins,
            CASE
                WHEN name LIKE 'oozie:launcher:T=spark%' THEN 'Spark (Oozie)'
                WHEN name LIKE 'oozie:launcher:T=hive%' THEN 'Hive (Oozie)'
                WHEN name LIKE 'oozie:launcher:T=hive2%' THEN 'Hive (Oozie)'
                WHEN name LIKE 'oozie:launcher:T=sqoop%' THEN 'Sqoop (Oozie)'
                WHEN name LIKE 'oozie:launcher%' THEN 'Oozie Launcher'
                WHEN application_type = 'SPARK' THEN 'Spark'
                WHEN application_type = 'MAPREDUCE' AND (
                    name LIKE 'SELECT %' OR name LIKE 'INSERT %' OR
                    name LIKE 'CREATE %' OR name LIKE 'DROP %' OR
                    name LIKE 'ALTER %' OR name LIKE 'LOAD %'
                ) THEN 'Hive'
                WHEN LOWER(name) LIKE '%sqoop%' THEN 'Sqoop'
                WHEN application_type = 'MAPREDUCE' THEN 'MapReduce'
                ELSE 'Other'
            END AS job_type,
            (CAST(memory_seconds AS DOUBLE) / 3600.0 / 1024.0) * {dbu_rate} AS dollar_dbus,
            (CAST(memory_seconds AS DOUBLE) / 3600.0 / 1024.0) * {vm_rate} AS dollar_vm,
            ((CAST(memory_seconds AS DOUBLE) / 3600.0 / 1024.0) * {dbu_rate}) +
            ((CAST(memory_seconds AS DOUBLE) / 3600.0 / 1024.0) * {vm_rate}) AS total_cost
        FROM yarn_applications
    """)
    count = conn.execute("SELECT COUNT(*) FROM yarn_analysis_vw").fetchone()[0]
    logger.info("Created yarn_analysis_vw with %d rows", count)
    return count


def create_oozie_analysis_vw(conn) -> int:
    """Create oozie_analysis_vw filtered to Oozie launcher apps."""
    conn.execute("DROP TABLE IF EXISTS oozie_analysis_vw")
    conn.execute("""
        CREATE TABLE oozie_analysis_vw AS
        SELECT * FROM yarn_analysis_vw
        WHERE name LIKE 'oozie:launcher%'
    """)
    count = conn.execute("SELECT COUNT(*) FROM oozie_analysis_vw").fetchone()[0]
    logger.info("Created oozie_analysis_vw with %d rows", count)
    return count


def create_hourly_yarn_view(conn) -> int:
    """Create hourly aggregation view."""
    conn.execute("DROP TABLE IF EXISTS hourly_yarn_view")
    conn.execute("""
        CREATE TABLE hourly_yarn_view AS
        SELECT
            strftime(to_timestamp(started_time / 1000), '%Y-%m-%d %H:00:00') AS hour_bucket,
            COUNT(*) AS total_apps,
            SUM(memory_gb_hours) AS total_memory_gb_hours,
            SUM(vcore_hours) AS total_vcore_hours,
            SUM(total_cost) AS total_cost,
            COUNT(DISTINCT "user") AS unique_users,
            COUNT(DISTINCT queue) AS unique_queues
        FROM yarn_analysis_vw
        WHERE started_time IS NOT NULL AND started_time > 0
        GROUP BY hour_bucket
        ORDER BY hour_bucket
    """)
    count = conn.execute("SELECT COUNT(*) FROM hourly_yarn_view").fetchone()[0]
    logger.info("Created hourly_yarn_view with %d rows", count)
    return count


def create_all_yarn_analysis(conn, dbu_rate: float = 0.15, vm_rate: float = 0.10) -> int:
    """Create all derived YARN analysis tables. Returns total rows."""
    total = 0
    total += create_yarn_analysis_vw(conn, dbu_rate, vm_rate)
    total += create_oozie_analysis_vw(conn)
    total += create_hourly_yarn_view(conn)
    return total
