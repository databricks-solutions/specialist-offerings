"""Retry policy for transient Databricks-side failures (design §9.2, SEC-09).

IP access lists take minutes to propagate; during that window requests intermittently return 403 "IP ACL" /
Forbidden. Those are transient. A 5xx or timeout *during a commit* is NOT retried here: its outcome is unknown and the
caller must reconcile by tag (UnknownCommitOutcome).
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable
from typing import TypeVar

T = TypeVar("T")
TRANSIENT_MARKERS = ("IP ACL", "Forbidden", "403", "TEMPORARILY_UNAVAILABLE", "503", "429")
log = logging.getLogger("h2mi.retry")


def with_backoff(fn: Callable[[], T], retries: int = 6, wait_s: float = 30, factor: float = 1.5,
                 markers: Iterable[str] = TRANSIENT_MARKERS, label: str = "") -> T:
    delay = wait_s
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if attempt == retries or not any(m in msg for m in markers):
                raise
            log.warning("retry %s attempt %d/%d transient: %s -> sleep %.0fs", label, attempt, retries,
                        msg.splitlines()[0][:120], delay)
            time.sleep(delay)
            delay *= factor
    raise AssertionError("unreachable")
