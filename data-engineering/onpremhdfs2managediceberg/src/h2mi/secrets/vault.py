"""The customer's secret store. Open question Q-? in docs/requirements.md: which product and auth method (AppRole, Kerberos,
certificate). Wire the client here; keep the signature. Access logs from this store are half of the audit trail (AUD-06)."""
from __future__ import annotations


def from_vault(spec: str) -> str:
    raise NotImplementedError("vault provider not wired yet: implement h2mi.secrets.vault.from_vault for "
                              f"reference vault:{spec} (design §9.1)")
