"""
token.py
────────────────────────────────────────────────────────────────────────────
OAuth credentials for the Gmail API, sourced from AWS Secrets Manager.

A previously-authorised OAuth token (access token + refresh token + client
id/secret) is stored as a single Secrets Manager secret. This module loads it,
builds google credentials, and silently refreshes the access token when it has
expired.

Required env vars:
    GMAIL_TOKEN_SECRET     name (or ARN) of the secret holding the token JSON
Optional:
    AWS_REGION             region of the secret (else boto3's default chain)
"""
from __future__ import annotations

import json
import os

import boto3
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

DEFAULT_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def _secrets_client():
    region = os.environ.get("AWS_REGION")
    return boto3.client("secretsmanager", region_name=region) if region else boto3.client(
        "secretsmanager"
    )


def _load_token() -> dict:
    secret_name = os.environ["GMAIL_TOKEN_SECRET"]
    raw = _secrets_client().get_secret_value(SecretId=secret_name)["SecretString"]
    return json.loads(raw)


def _save_token(data: dict) -> None:
    secret_name = os.environ["GMAIL_TOKEN_SECRET"]
    _secrets_client().put_secret_value(SecretId=secret_name, SecretString=json.dumps(data))


def get_credentials(persist_refresh: bool = True) -> Credentials:
    """Build google OAuth credentials from the stored token; refresh if expired."""
    token = _load_token()
    creds = Credentials(
        token=token.get("token"),
        refresh_token=token.get("refresh_token"),
        token_uri=token.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=token.get("client_id"),
        client_secret=token.get("client_secret"),
        scopes=token.get("scopes", DEFAULT_SCOPES),
    )

    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            if persist_refresh:
                token["token"] = creds.token
                _save_token(token)
        else:
            raise RuntimeError("Gmail credentials invalid and cannot be refreshed.")

    return creds
