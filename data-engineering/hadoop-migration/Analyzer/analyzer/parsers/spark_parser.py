"""Parse Spark History Server application JSON files."""

import glob
import json
import logging
import os
from typing import List

from analyzer.models import CodeArtifact, WorkloadInventoryItem, WorkloadType

logger = logging.getLogger(__name__)


def parse_spark_apps(file_path: str) -> List[WorkloadInventoryItem]:
    """Parse a Spark_Applications JSON file."""
    logger.info("Parsing Spark HS dump: %s", file_path)

    with open(file_path, "r") as f:
        data = json.load(f)

    # Data is a JSON array of application objects
    if not isinstance(data, list):
        logger.warning("Expected JSON array in %s", file_path)
        return []

    items = []
    for app in data:
        app_id = app.get("id", "")
        name = app.get("name", "")
        attempts = app.get("attempts", [])

        # Use the latest attempt for user/timing info
        latest_attempt = attempts[0] if attempts else {}
        spark_user = latest_attempt.get("sparkUser", "")
        start_time = latest_attempt.get("startTime", "")
        end_time = latest_attempt.get("endTime", "")

        # Infer entry point from name
        entry_point = None
        artifacts = []

        if "." in name and not name.startswith("PySpark") and " " not in name:
            # Looks like a class name, e.g. org.apache.spark.examples.SparkPi
            entry_point = name
        elif name.endswith(".py"):
            entry_point = name
            artifacts.append(CodeArtifact(
                path=name,
                location_type="local",
                artifact_type="py",
            ))

        tags = []
        if name.startswith("PySpark"):
            tags.append("pyspark")

        item = WorkloadInventoryItem(
            workload_id=app_id,
            workload_name=name,
            workload_type=WorkloadType.SPARK,
            user=spark_user,
            queue="",  # Not available from Spark HS
            entry_point=entry_point,
            code_artifacts=artifacts,
            yarn_app_id=app_id,
            source="spark_hs",
            tags=tags,
        )
        items.append(item)

    logger.info("Parsed %d Spark applications", len(items))
    return items


def find_and_parse_spark_apps(base_dir: str) -> List[WorkloadInventoryItem]:
    """Find all Spark_Applications JSON files under base_dir and parse them."""
    pattern = os.path.join(base_dir, "SPARK", "**", "Spark_Applications*.json")
    files = glob.glob(pattern, recursive=True)

    if not files:
        logger.warning("No Spark HS files found under %s", base_dir)
        return []

    all_items = []
    for f in sorted(files):
        all_items.extend(parse_spark_apps(f))
    return all_items
