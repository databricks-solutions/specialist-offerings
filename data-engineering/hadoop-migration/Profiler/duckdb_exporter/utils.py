"""Utility functions for the DuckDB exporter."""

import glob
import logging
import os
import re
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Timestamp pattern in profiler filenames: YYYYMMDD_HHMMSS
TIMESTAMP_PATTERN = re.compile(r"(\d{8})_(\d{6})")


def find_json_files(base_dir: str, subdir: str, pattern: str) -> list:
    """Find JSON files matching a glob pattern under base_dir/subdir/.

    Returns list of file paths sorted by name (most recent last).
    """
    search_path = os.path.join(base_dir, subdir, "**", pattern)
    files = glob.glob(search_path, recursive=True)
    if not files:
        logger.warning("No files matching %s found under %s/%s", pattern, base_dir, subdir)
    return sorted(files)


def extract_timestamp_from_filename(filepath: str) -> datetime:
    """Extract the extraction timestamp from a profiler filename.

    Filenames like YarnApplicationDump_20260319_015436.json
    contain the timestamp 20260319_015436 → 2026-03-19 01:54:36 UTC.
    """
    basename = os.path.basename(filepath)
    match = TIMESTAMP_PATTERN.search(basename)
    if match:
        date_str = match.group(1)
        time_str = match.group(2)
        return datetime.strptime(
            f"{date_str}{time_str}", "%Y%m%d%H%M%S"
        ).replace(tzinfo=timezone.utc)
    # Fallback: file modification time
    mtime = os.path.getmtime(filepath)
    return datetime.fromtimestamp(mtime, tz=timezone.utc)


def extract_timestamp_from_impala_filename(filepath: str) -> datetime:
    """Extract timestamp from impala filename pattern.

    Supports two naming conventions from the profiler:
      - impala_YYYY-MM-DD_HH_MM_SS_page.json  (dashes in date)
      - impala_YYYY_MM_DD_HH_MM_SS_page.json   (all underscores)
    """
    basename = os.path.basename(filepath)
    # Match impala_YYYY-MM-DD_HH_MM_SS_page.json (dashes in date)
    match = re.search(r"impala_(\d{4}-\d{2}-\d{2})_(\d{2})_(\d{2})_(\d{2})_\d+\.json", basename)
    if match:
        date_str = match.group(1)
        hour = match.group(2)
        minute = match.group(3)
        second = match.group(4)
        return datetime.strptime(
            f"{date_str} {hour}:{minute}:{second}", "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=timezone.utc)
    # Match impala_YYYY_MM_DD_HH_MM_SS_page.json (all underscores)
    match = re.search(r"impala_(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})_(\d{2})_\d+\.json", basename)
    if match:
        return datetime(
            int(match.group(1)), int(match.group(2)), int(match.group(3)),
            int(match.group(4)), int(match.group(5)), int(match.group(6)),
            tzinfo=timezone.utc,
        )
    return extract_timestamp_from_filename(filepath)
