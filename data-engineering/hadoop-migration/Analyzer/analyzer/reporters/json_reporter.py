"""Generate JSON inventory reports."""

import json
import logging
import os
from collections import Counter
from datetime import datetime, timezone
from typing import List

from analyzer.models import WorkloadInventoryItem

logger = logging.getLogger(__name__)


def generate_json_report(items: List[WorkloadInventoryItem], output_dir: str) -> str:
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

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_workloads": len(items),
        "summary": {
            "by_type": dict(type_counts.most_common()),
            "by_source": dict(source_counts.most_common()),
        },
        "inventory": [item.to_dict() for item in items],
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"workload_inventory_{timestamp}.json")

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info("JSON report written to %s", output_path)
    return output_path
