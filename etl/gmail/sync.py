"""
Gmail sync: backfill + incremental.

Pulls mail from target senders and stores each one as a JSON file in S3,
named {message_id}-{account_id}.json. Both methods return the new history_id.
"""
import base64
import json
import os

import boto3

from etl.gmail.client import GmailClient


class GmailSync:
    def __init__(self, client: GmailClient, account_id, senders, bucket=None, prefix=""):
        self.client = client
        self.account_id = account_id
        self.senders = list(senders)
        self.bucket = bucket or os.environ["S3_RAW_BUCKET"]
        self.prefix = prefix
        self.s3 = boto3.client("s3")

    def backfill(self):
        history_id = self.client.execute(
            lambda: self.client.service.users().getProfile(userId="me")
        )["historyId"]
        query = " OR ".join(f"from:{s}" for s in self.senders)
        for message_id in self._list_ids(query):
            self._save(self._get(message_id))
        return history_id

    def incremental(self, history_id):
        latest = history_id
        page_token = None
        while True:
            resp = self.client.execute(lambda: self.client.service.users().history().list(
                userId="me", startHistoryId=history_id,
                historyTypes=["messageAdded"], pageToken=page_token,
            ))
            latest = resp.get("historyId", latest)
            for record in resp.get("history", []):
                for added in record.get("messagesAdded", []):
                    self._save(self._get(added["message"]["id"]))
            page_token = resp.get("nextPageToken")
            if not page_token:
                return latest

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
        headers = {h["name"].lower(): h["value"]
                   for h in message["payload"].get("headers", [])}
        record = {
            "message_id": message["id"],
            "account_id": self.account_id,
            "sender": headers.get("from"),
            "subject": headers.get("subject"),
            "date": headers.get("date"),
            "snippet": message.get("snippet"),
        }
        key = f"{self.prefix}{message['id']}-{self.account_id}.json"
        self.s3.put_object(
            Bucket=self.bucket, Key=key,
            Body=json.dumps(record).encode("utf-8"),
            ContentType="application/json",
        )
