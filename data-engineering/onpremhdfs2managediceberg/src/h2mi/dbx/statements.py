"""SQL Statement Execution API wrapper (design §8.3, D3, D11): DDL / GRANT / RESTORE as ddl-admin, MERGE / REPLACE
WHERE / target fingerprints as the sync SP. Polls to a terminal state; IP-ACL 403 flaps are retried by the caller
with util.retry.with_backoff (SEC-09).

Statement files may hold several statements separated by a line containing only `;;`.
"""
from __future__ import annotations

import time

from ..util.errors import H2miError


class StatementError(H2miError):
    pass


def split_statements(raw: str) -> list[str]:
    """Split on lines containing only `;;`; drop chunks that are only comments."""
    chunks = [c.strip() for c in raw.split("\n;;") if c.strip()]
    return [c for c in chunks if any(ln.strip() and not ln.strip().startswith("--") for ln in c.splitlines())]


class Statements:
    def __init__(self, ws, warehouse_id: str):
        self.ws, self.warehouse_id = ws, warehouse_id

    def execute(self, statement: str, poll_s: float = 2.0, timeout_s: float = 1800) -> list[list]:
        """Run one statement; return rows (list of lists, strings as returned by the API)."""
        from databricks.sdk.service.sql import StatementState

        r = self.ws.statement_execution.execute_statement(statement=statement, warehouse_id=self.warehouse_id,
                                                          wait_timeout="50s")
        deadline = time.time() + timeout_s
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING) and time.time() < deadline:
            time.sleep(poll_s)
            r = self.ws.statement_execution.get_statement(r.statement_id)
        if r.status.state != StatementState.SUCCEEDED:
            msg = r.status.error.message if r.status.error else ""
            raise StatementError(f"{r.status.state.value}: {msg[:500]}")
        return (r.result.data_array if r.result else None) or []

    def run_file(self, path: str) -> int:
        stmts = split_statements(open(path).read())
        for s in stmts:
            self.execute(s)
        return len(stmts)
