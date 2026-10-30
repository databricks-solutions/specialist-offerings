"""databricks-sdk WorkspaceClient for the Statement / Jobs APIs with h2mi attribution in the user agent (AUD-04)."""
from __future__ import annotations

from .. import __version__


def workspace_client(host: str, client_id: str, client_secret: str, run_id: str | None = None):
    """OAuth M2M client. `client_secret` comes from h2mi.secrets.resolve and is passed straight through."""
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient(host=host if host.startswith("http") else f"https://{host}", client_id=client_id,
                         client_secret=client_secret, product="h2mi", product_version=__version__)
    if run_id:
        ws.config.with_user_agent_extra("h2mi_run_id", run_id)
    return ws
