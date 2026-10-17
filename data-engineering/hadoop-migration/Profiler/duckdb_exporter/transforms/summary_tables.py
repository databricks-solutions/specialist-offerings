"""Workload summary aggregation tables."""
import logging

logger = logging.getLogger(__name__)


def create_workload_summary_by_user(conn) -> int:
    """Create per-user workload summary."""
    conn.execute("DROP TABLE IF EXISTS workload_summary_by_user")
    conn.execute("""
        CREATE TABLE workload_summary_by_user AS
        SELECT
            "user",
            COUNT(*) AS total_jobs,
            COUNT(DISTINCT queue) AS queues_used,
            SUM(memory_gb_hours) AS total_memory_gb_hours,
            SUM(vcore_hours) AS total_vcore_hours,
            SUM(total_cost) AS total_cost,
            AVG(elapsed_time_mins) AS avg_duration_mins
        FROM yarn_analysis_vw
        GROUP BY "user"
        ORDER BY total_cost DESC
    """)
    count = conn.execute("SELECT COUNT(*) FROM workload_summary_by_user").fetchone()[0]
    logger.info("Created workload_summary_by_user with %d rows", count)
    return count


def create_workload_summary_by_queue(conn) -> int:
    """Create per-queue workload summary."""
    conn.execute("DROP TABLE IF EXISTS workload_summary_by_queue")
    conn.execute("""
        CREATE TABLE workload_summary_by_queue AS
        SELECT
            queue,
            COUNT(*) AS total_jobs,
            COUNT(DISTINCT "user") AS unique_users,
            SUM(memory_gb_hours) AS total_memory_gb_hours,
            SUM(vcore_hours) AS total_vcore_hours,
            SUM(total_cost) AS total_cost
        FROM yarn_analysis_vw
        GROUP BY queue
        ORDER BY total_cost DESC
    """)
    count = conn.execute("SELECT COUNT(*) FROM workload_summary_by_queue").fetchone()[0]
    logger.info("Created workload_summary_by_queue with %d rows", count)
    return count


def create_workload_summary_by_type(conn) -> int:
    """Create per-job-type workload summary."""
    conn.execute("DROP TABLE IF EXISTS workload_summary_by_type")
    conn.execute("""
        CREATE TABLE workload_summary_by_type AS
        SELECT
            job_type,
            COUNT(*) AS total_jobs,
            AVG(elapsed_time_mins) AS avg_duration_mins,
            SUM(memory_gb_hours) AS total_memory_gb_hours,
            SUM(total_cost) AS total_cost
        FROM yarn_analysis_vw
        GROUP BY job_type
        ORDER BY total_jobs DESC
    """)
    count = conn.execute("SELECT COUNT(*) FROM workload_summary_by_type").fetchone()[0]
    logger.info("Created workload_summary_by_type with %d rows", count)
    return count


def create_all_summary_tables(conn) -> int:
    """Create all summary tables. Returns total rows."""
    total = 0
    total += create_workload_summary_by_user(conn)
    total += create_workload_summary_by_queue(conn)
    total += create_workload_summary_by_type(conn)
    return total
