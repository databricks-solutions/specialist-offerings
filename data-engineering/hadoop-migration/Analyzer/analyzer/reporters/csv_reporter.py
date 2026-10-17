"""Generate CSV inventory reports."""

import csv
import logging
import os
from datetime import datetime
from typing import List

from analyzer.models import WorkloadInventoryItem

logger = logging.getLogger(__name__)


def generate_csv_report(items: List[WorkloadInventoryItem], output_dir: str) -> str:
    """Generate a CSV inventory report (one row per workload).

    Returns the path to the generated file.
    """
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"workload_inventory_{timestamp}.csv")

    fieldnames = [
        "workload_id", "workload_name", "workload_type", "user", "queue",
        "entry_point", "source", "tags", "final_status",
        "elapsed_time", "memory_seconds", "vcore_seconds",
        "job_type", "memory_gb_hours", "vcore_hours", "elapsed_time_mins",
        "dollar_dbus", "dollar_vm", "total_cost",
        "code_artifact_paths", "dependency_paths",
        "oozie_workflow_name", "oozie_app_path",
        "database", "query_type", "duration_millis",
        "complexity", "complexity_signals", "convert_command", "local_code_path",
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for item in items:
            writer.writerow({
                "workload_id": item.workload_id,
                "workload_name": item.workload_name,
                "workload_type": item.workload_type.value,
                "user": item.user,
                "queue": item.queue,
                "entry_point": item.entry_point or "",
                "source": item.source,
                "tags": ";".join(item.tags),
                "final_status": item.final_status or "",
                "elapsed_time": item.elapsed_time or "",
                "memory_seconds": item.memory_seconds or "",
                "vcore_seconds": item.vcore_seconds or "",
                "job_type": item.job_type or "",
                "memory_gb_hours": item.memory_gb_hours if item.memory_gb_hours is not None else "",
                "vcore_hours": item.vcore_hours if item.vcore_hours is not None else "",
                "elapsed_time_mins": item.elapsed_time_mins if item.elapsed_time_mins is not None else "",
                "dollar_dbus": item.dollar_dbus if item.dollar_dbus is not None else "",
                "dollar_vm": item.dollar_vm if item.dollar_vm is not None else "",
                "total_cost": item.total_cost if item.total_cost is not None else "",
                "code_artifact_paths": ";".join(a.path for a in item.code_artifacts),
                "dependency_paths": ";".join(a.path for a in item.dependencies),
                "oozie_workflow_name": item.oozie_workflow_name or "",
                "oozie_app_path": item.oozie_app_path or "",
                "database": item.database or "",
                "query_type": item.query_type or "",
                "duration_millis": item.duration_millis or "",
                "complexity": item.complexity or "",
                "complexity_signals": ";".join(item.complexity_signals),
                "convert_command": item.convert_command or "",
                "local_code_path": item.local_code_path or "",
            })

    logger.info("CSV report written to %s", output_path)
    return output_path
