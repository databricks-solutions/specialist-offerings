"""Parse Impala query JSON files (extracted via Cloudera Manager API)."""

import glob
import json
import logging
import os
from typing import List

from analyzer.models import CodeArtifact, WorkloadInventoryItem, WorkloadType

logger = logging.getLogger(__name__)


def parse_impala_queries(file_path: str) -> List[WorkloadInventoryItem]:
    """Parse an Impala query JSON file."""
    logger.info("Parsing Impala dump: %s", file_path)

    with open(file_path, "r") as f:
        data = json.load(f)

    queries = data.get("queries", [])
    items = []

    for query in queries:
        query_id = query.get("queryId", "")
        statement = query.get("statement", "").strip()
        query_type = query.get("queryType", "")
        database = query.get("database", "default")

        # The SQL statement itself is an embedded code artifact
        artifacts = []
        if statement:
            artifacts.append(CodeArtifact(
                path=statement,
                location_type="embedded",
                artifact_type="sql",
            ))

        # Build a readable name from the statement (truncate long ones)
        display_name = statement[:80] + "..." if len(statement) > 80 else statement

        item = WorkloadInventoryItem(
            workload_id=query_id,
            workload_name=display_name,
            workload_type=WorkloadType.IMPALA,
            user=query.get("user", ""),
            queue="",  # Impala uses resource pools, not YARN queues directly
            code_artifacts=artifacts,
            source="impala",
            tags=[f"query_type:{query_type.lower()}"] if query_type else [],
            database=database,
            query_type=query_type,
            rows_produced=query.get("rowsProduced"),
            duration_millis=query.get("durationMillis"),
        )
        items.append(item)

    logger.info("Parsed %d Impala queries", len(items))
    return items


def find_and_parse_impala_queries(base_dir: str) -> List[WorkloadInventoryItem]:
    """Find all impala_*.json files under base_dir and parse them."""
    pattern = os.path.join(base_dir, "IMPALA", "**", "impala_*.json")
    files = glob.glob(pattern, recursive=True)

    if not files:
        logger.warning("No Impala query files found under %s", base_dir)
        return []

    all_items = []
    for f in sorted(files):
        all_items.extend(parse_impala_queries(f))
    return all_items
