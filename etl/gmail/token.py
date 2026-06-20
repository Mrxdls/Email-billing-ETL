"""
token.py
────────────────────────────────────────────────────────────────────────────
OAuth credentials for the Gmail API, sourced from AWS Secrets Manager — using
the SAME layout the backend writes when a user signs in / links an account.

There is NO interactive sign-in here. The backend (AuthService) performs the
OAuth dance and stores each account's token set as its own secret:

    SecretId    : "{AWS_SECRET_PREFIX}google-{googleSub}"   (the account's secret_ref)
    SecretString: {
        "access_token":  "<access token>",
        "refresh_token": "<refresh token>",
        "scope":         "openid email https://www.googleapis.com/auth/gmail.readonly",
        "expiry_date":   1718900000000          # ms since epoch, may be absent
    }

NOTE the differences from a "standalone" google token file:
  • the access token field is "access_token" (not "token")
  • "scope" is a single space-delimited STRING (not a "scopes" list)
  • "expiry_date" is epoch MILLISECONDS (google's convention), may be missing
  • client_id / client_secret / token_uri are NOT in the secret — they are the
    OAuth app's credentials and come from env (shared with the backend app).

This module loads an account's secret and mints a FRESH access token from the
stored refresh_token on every call (the stored access_token lives only ~1h, so a
daily/on-demand batch can't rely on it) — writing the new token back in the EXACT
same shape so the backend keeps reading a consistent secret.

Required env vars:
    GOOGLE_CLIENT_ID         OAuth client id      (same value as the backend)
    GOOGLE_CLIENT_SECRET     OAuth client secret  (same value as the backend)

Optional env vars:
    AWS_SECRET_PREFIX        prefix the backend writes under (default "backend/")
    GOOGLE_TOKEN_URI         token endpoint (default google's)
    AWS_REGION               region of the secret (else boto3's default chain)
"""
from __future__ import annotations

import json
import os
from datetime import timezone

import boto3
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# Read-only is enough for an extraction pipeline; widen only if sync needs to
# modify labels. Used only as a fallback when the secret carries no scope.
DEFAULT_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
DEFAULT_TOKEN_URI = "https://oauth2.googleapis.com/token"


def _secrets_client():
    region = os.environ.get("AWS_REGION")
    return boto3.client("secretsmanager", region_name=region) if region else boto3.client(
        "secretsmanager"
    )


def _secret_id(secret_ref: str) -> str:
    """Apply the same prefix the backend writes under (AWS_SECRET_PREFIX)."""
    prefix = os.environ.get("AWS_SECRET_PREFIX", "backend/")
    return f"{prefix}{secret_ref}"


def _load_token(secret_ref: str) -> dict:
    """Read and parse one account's stored OAuth token JSON from Secrets Manager."""
    raw = _secrets_client().get_secret_value(SecretId=_secret_id(secret_ref))["SecretString"]
    return json.loads(raw)


def _save_token(secret_ref: str, data: dict) -> None:
    """Persist a (refreshed) token back to Secrets Manager in the backend's shape."""
    _secrets_client().put_secret_value(
        SecretId=_secret_id(secret_ref), SecretString=json.dumps(data)
    )


def _scopes_from(token: dict) -> list[str]:
    """Backend stores `scope` as a space-delimited string; normalise to a list."""
    scope = token.get("scope")
    if isinstance(scope, str) and scope.strip():
        return scope.split()
    if isinstance(scope, list) and scope:        # tolerate a legacy list shape
        return scope
    return DEFAULT_SCOPES


def _to_expiry_ms(creds: Credentials) -> int | None:
    if not creds.expiry:
        return None
    return int(creds.expiry.replace(tzinfo=timezone.utc).timestamp() * 1000)


def get_credentials(secret_ref: str, persist_refresh: bool = True) -> Credentials:
    """
    Build google OAuth credentials for ONE linked account from its backend secret.

    `secret_ref` is the value stored in google_accounts.secret_ref
    (e.g. "google-1234567890"); the AWS_SECRET_PREFIX is applied automatically.

    The refresh_token is the durable credential the backend persists; the stored
    access_token lives only ~1h, so a daily/on-demand batch effectively never has
    a usable one. We therefore mint a FRESH access token from the refresh_token
    on every run. When persist_refresh is True the new access_token + expiry_date
    are written back to the secret in the backend's shape, so the backend keeps
    reading a consistent value.

    Returns a `google.oauth2.credentials.Credentials` ready for the Gmail client.
    """
    token = _load_token(secret_ref)

    refresh_token = token.get("refresh_token")
    if not refresh_token:
        raise RuntimeError(
            f"Gmail credentials for secret_ref={secret_ref!r} have no refresh_token; "
            "the user must re-authorise this account via the backend sign-in flow."
        )

    creds = Credentials(
        token=None,                      # always (re)minted from the refresh_token below
        refresh_token=refresh_token,
        token_uri=os.environ.get("GOOGLE_TOKEN_URI", DEFAULT_TOKEN_URI),
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        scopes=_scopes_from(token),
    )

    creds.refresh(Request())             # exchange refresh_token → fresh access token

    if persist_refresh:
        # Write back in the backend's shape; keep refresh_token & scope intact.
        token["access_token"] = creds.token
        token["expiry_date"] = _to_expiry_ms(creds)
        _save_token(secret_ref, token)

    return creds
