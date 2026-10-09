"""Generate JSON inventory reports."""

import json
import logging
import os
from collections import Counter
from datetime import datetime, timezone
from typing import List

from analyzer.models import WorkloadInventoryItem

logger = logging.getLogger(__name__)


def generate_json_report(items: List[WorkloadInventoryItem], output_dir: str,
                         summaries: dict = None) -> str:
    """Generate a JSON inventory report.

    Returns the path to the generated file.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Build summary
    type_counts = Counter(item.workload_type.value for item in items)
    source_counts = Counter()
    for item in items:
        for src in item.source.split("+"):
            source_counts[src] += 1

    complexity_counts = Counter(
        item.complexity for item in items if item.complexity
    )

    summary = {
        "by_type": dict(type_counts.most_common()),
        "by_source": dict(source_counts.most_common()),
        "by_complexity": dict(complexity_counts.most_common()),
    }

    # Add DuckDB-derived aggregate summaries if available
    if summaries:
        if summaries.get("by_job_type"):
            summary["by_job_type"] = summaries["by_job_type"]
        if summaries.get("by_user"):
            summary["by_user"] = summaries["by_user"]
        if summaries.get("by_queue"):
            summary["by_queue"] = summaries["by_queue"]

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_workloads": len(items),
        "summary": summary,
    }

    # Cost summary: aggregate from per-item enriched fields
    cost_items = [i for i in items if i.total_cost is not None]
    if cost_items:
        report["cost_summary"] = {
            "total_cost": sum(i.total_cost for i in cost_items),
            "total_memory_gb_hours": sum(i.memory_gb_hours or 0 for i in cost_items),
            "total_vcore_hours": sum(i.vcore_hours or 0 for i in cost_items),
        }

    # Demand profile (hourly time-series)
    if summaries and summaries.get("demand_profile"):
        report["demand_profile"] = summaries["demand_profile"]

    report["inventory"] = [item.to_dict() for item in items]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"workload_inventory_{timestamp}.json")

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info("JSON report written to %s", output_path)
    return output_path
