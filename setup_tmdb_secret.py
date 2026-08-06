"""
One-time setup script: creates the `tmdb` Databricks secret scope and stores the
TMDB API Read Access Token (v4 auth - the long `eyJ...` JWT from your TMDB API
settings page). Run this locally (with the Databricks CLI configured) or from a
notebook - never commit the resulting secret value anywhere.

This is the TMDB counterpart to setup_secrets.py (which handles the Lakebase URL
and Massive key). The Lakebase URL already lives in the `database` scope, so it
is intentionally left untouched here.

Usage:
    python setup_tmdb_secret.py
"""
import getpass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

w = WorkspaceClient()

# create_scope fails if the scope already exists; ignore that on re-runs.
try:
    w.secrets.create_scope(scope="tmdb")
except Exception as exc:  # noqa: BLE001 - scope may already exist
    print(f"(scope 'tmdb' not created, likely already exists: {exc})")

w.secrets.put_secret(
    scope="tmdb",
    key="api-key",
    string_value=getpass.getpass("Paste your TMDB API Read Access Token: "),
)

w.secrets.put_acl(
    scope="tmdb",
    principal="users",
    permission=workspace.AclPermission.READ,
)

print("Stored TMDB read access token in secret tmdb/api-key.")
