"""Generate conversion queue for the Converter plugin."""

import csv
import logging
import os
from datetime import datetime
from typing import List

from analyzer.models import WorkloadInventoryItem

logger = logging.getLogger(__name__)

TIER_PRIORITY = {
    "easy": 1,
    "medium": 2,
    "hard": 3,
    "very_hard": 4,
}


def generate_conversion_queue_report(
    items: List[WorkloadInventoryItem], output_dir: str
) -> str:
    """Write a prioritized conversion queue CSV for scored workloads.

    Returns the path to the generated file.
    """
    os.makedirs(output_dir, exist_ok=True)

    scored = [i for i in items if i.complexity and i.convert_command]
    scored.sort(
        key=lambda i: (
            TIER_PRIORITY.get(i.complexity or "", 99),
            -(i.memory_seconds or 0),
        )
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"conversion_queue_{timestamp}.csv")

    fieldnames = [
        "priority",
        "workload_id",
        "workload_name",
        "workload_type",
        "yarn_app_id",
        "memory_seconds",
        "complexity",
        "complexity_signals",
        "code_path",
        "local_code_path",
        "convert_command",
        "notes",
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for rank, item in enumerate(scored, start=1):
            code_path = item.code_artifacts[0].path if item.code_artifacts else ""
            writer.writerow(
                {
                    "priority": rank,
                    "workload_id": item.workload_id,
                    "workload_name": item.workload_name,
                    "workload_type": item.workload_type.value,
                    "yarn_app_id": item.yarn_app_id or "",
                    "memory_seconds": item.memory_seconds or "",
                    "complexity": item.complexity or "",
                    "complexity_signals": ";".join(item.complexity_signals),
                    "code_path": code_path,
                    "local_code_path": item.local_code_path or "",
                    "convert_command": item.convert_command or "",
                    "notes": " | ".join(item.complexity_recommended_actions),
                }
            )

    logger.info(
        "Conversion queue written to %s (%d items)", output_path, len(scored)
    )
    return output_path
