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
        "code_artifact_paths", "dependency_paths",
        "oozie_workflow_name", "oozie_app_path",
        "database", "query_type", "duration_millis",
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
                "code_artifact_paths": ";".join(a.path for a in item.code_artifacts),
                "dependency_paths": ";".join(a.path for a in item.dependencies),
                "oozie_workflow_name": item.oozie_workflow_name or "",
                "oozie_app_path": item.oozie_app_path or "",
                "database": item.database or "",
                "query_type": item.query_type or "",
                "duration_millis": item.duration_millis or "",
            })

    logger.info("CSV report written to %s", output_path)
    return output_path
