"""JSON logging keyed by run_id / table_id / cycle_id (design §9.4, OPS-10). Secrets never reach a log line:
the secret providers return values that are only ever passed to spark.conf.set / client constructors."""
from __future__ import annotations

import json
import logging
import sys
import time

_CONTEXT: dict[str, str] = {}


def set_context(**kv: str) -> None:
    _CONTEXT.update({k: v for k, v in kv.items() if v is not None})


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)), "level": record.levelname,
               "logger": record.name, "msg": record.getMessage(), **_CONTEXT}
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str)


def configure(level: int = logging.INFO) -> None:
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [h]
    root.setLevel(level)
