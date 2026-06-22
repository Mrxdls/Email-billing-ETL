"""
Gmail sync: per-sender, query-based (no mailbox history cursor).

Each target sender carries its OWN watermark (target_senders.last_internal_date,
epoch ms). A sender is synced with `from:<sender> after:<watermark seconds>`:
  • watermark NULL → full backfill of that sender (first sync, or just added)
  • watermark set  → only mail at/after it (boundary overlap is deduped)
This makes "add a sender later" trivial — the new sender backfills itself while
the others keep their own positions, with no shared cursor to reconcile.

Each email is landed transactionally:
  1. INSERT a row into browns_email_data (mints its uuid `id`), ON CONFLICT
     (account, message) DO NOTHING so re-pulls are idempotent,
  2. upload the email JSON to S3 named {id}.json (only when newly inserted),
  3. commit — or roll back if the upload fails, so a committed row always has a
     matching object.
After a sender's pull completes, its watermark is advanced to the max
internalDate seen.
"""
import base64
import json
import os
import uuid

import boto3

from etl.db import postgres
from etl.gmail.client import GmailClient


class GmailSync:
    def __init__(self, client: GmailClient, account_id, user_id,
                 conn=None, bucket=None, prefix=""):
        self.client = client
        self.account_id = account_id
        self.user_id = user_id
        self.bucket = bucket or os.environ["S3_RAW_BUCKET"]
        self.prefix = prefix
        self.s3 = boto3.client("s3")
        # One connection reused for all per-email/per-sender transactions.
        self.conn = conn or postgres.get_connection()

    # ── public ────────────────────────────────────────────────────────────────

    def run(self, senders, force=False):
        """
        Sync each target sender on its own watermark.

        `senders` is a list of (sender_id, sender_email, last_internal_date),
        as returned by postgres.load_target_senders. `force=True` ignores the
        watermark and re-pulls all of each sender's mail (safe — deduped).
        """
        for sender_id, sender_email, last_internal_date in senders:
            self.sync_sender(
                sender_id, sender_email,
                None if force else last_internal_date,
            )

    def sync_sender(self, sender_id, sender_email, last_internal_date):
        """Pull a sender's mail at/after its watermark, then advance the watermark."""
        query = f"from:{sender_email}"
        if last_internal_date:
            # Gmail `after:` takes epoch SECONDS; second-granular boundary
            # overlap is harmless thanks to the bronze dedup index.
            query += f" after:{last_internal_date // 1000}"

        max_seen = last_internal_date or 0
        for message_id in self._list_ids(query):
            max_seen = max(max_seen, self._save(self._get(message_id)))

        new_watermark = max_seen or None
        with self.conn:
            with self.conn.cursor() as cur:
                postgres.update_sender_watermark(cur, sender_id, new_watermark)
        return new_watermark

    # ── internal ──────────────────────────────────────────────────────────────

    def _list_ids(self, query):
        page_token = None
        while True:
            resp = self.client.execute(lambda: self.client.service.users().messages().list(
                userId="me", q=query, pageToken=page_token,
            ))
            for m in resp.get("messages", []):
                yield m["id"]
            page_token = resp.get("nextPageToken")
            if not page_token:
                return

    def _get(self, message_id):
        return self.client.execute(lambda: self.client.service.users().messages().get(
            userId="me", id=message_id, format="full",
        ))

    def _save(self, message):
        """Land one email transactionally. Returns its Gmail internalDate (ms)."""
        headers = {h["name"].lower(): h["value"]
                   for h in message["payload"].get("headers", [])}
        body_html, body_text = self._body(message["payload"])
        internal_date = int(message.get("internalDate", 0))

        # The JSON name IS the bronze row id, so mint it client-side — that lets
        # the INSERT carry bucket_path (NOT NULL) without a round-trip.
        record_id = str(uuid.uuid4())
        key = f"{self.prefix}{record_id}.json"
        bucket_path = f"s3://{self.bucket}/{key}"

        payload = {
            "id": record_id,
            "message_id": message["id"],
            "account_id": self.account_id,
            "user_id": self.user_id,
            "sender": headers.get("from"),
            "receiver": headers.get("to"),
            "subject": headers.get("subject"),
            "date": headers.get("date"),
            "body_html": body_html,
            "body_text": body_text,
            "label_ids": message.get("labelIds", []),
            "snippet": message.get("snippet"),
        }

        # Atomic per email: record the row, upload the JSON named by its id, then
        # commit. If the upload raises, the `with` block rolls back the INSERT —
        # so a committed row never points at a missing object. ON CONFLICT the
        # insert returns None (already ingested) → skip the upload entirely.
        with self.conn:
            with self.conn.cursor() as cur:
                inserted = postgres.insert_bronze_email(
                    cur,
                    record_id=record_id,
                    user_id=self.user_id,
                    google_account_id=self.account_id,
                    message_id=message["id"],
                    subject=headers.get("subject"),
                    sender=headers.get("from"),
                    receiver=headers.get("to"),
                    date=headers.get("date"),
                    snippet=message.get("snippet"),
                    bucket_path=bucket_path,
                )
            if inserted:
                self.s3.put_object(
                    Bucket=self.bucket, Key=key,
                    Body=json.dumps(payload).encode("utf-8"),
                    ContentType="application/json",
                )

        return internal_date

    def _body(self, payload):
        """Return (html, text) bodies, walking multipart parts."""
        html = text = None
        stack = [payload]
        while stack:
            part = stack.pop()
            data = part.get("body", {}).get("data")
            mime = part.get("mimeType", "")
            if data and mime == "text/html":
                html = base64.urlsafe_b64decode(data).decode("utf-8", "replace")
            elif data and mime == "text/plain":
                text = base64.urlsafe_b64decode(data).decode("utf-8", "replace")
            stack.extend(part.get("parts", []) or [])
        return html, text
