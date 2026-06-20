"""
client.py
────────────────────────────────────────────────────────────────────────────
Gmail API connection — nothing more.

Responsible only for building an authenticated Gmail service from the
Key Vault token (no interactive sign-in) and executing API requests with
retry/backoff. All mail-retrieval and sync logic lives in `sync.py`.
"""
from __future__ import annotations

import random
import time

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from etl.gmail.token import get_credentials

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_MAX_RETRIES = 5


class GmailClient:
    """An authenticated Gmail connection for a single mailbox."""

    def __init__(self, secret_ref=None, credentials=None, user_id: str = "me"):
        creds = credentials or get_credentials(secret_ref)
        self.user_id = user_id
        # cache_discovery=False silences the file-cache warning on Databricks
        self.service = build("gmail", "v1", credentials=creds, cache_discovery=False)

    def execute(self, request_factory, max_retries: int = _MAX_RETRIES):
        """
        Execute a Gmail API request with exponential backoff on 429/5xx.

        `request_factory` is a zero-arg callable returning a fresh request
        object each attempt (a request can only be executed once), e.g.
            client.execute(lambda: client.service.users().getProfile(userId="me"))
        """
        attempt = 0
        while True:
            try:
                return request_factory().execute()
            except HttpError as err:
                status = int(getattr(err.resp, "status", 0) or 0)
                if status not in _RETRYABLE_STATUS or attempt >= max_retries:
                    raise
                time.sleep(min(2 ** attempt, 32) + random.uniform(0, 1))
                attempt += 1
