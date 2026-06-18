"""
Databricks entrypoint: forced Gmail backfill.

Ignores every target sender's watermark and re-pulls all of its mail. Safe to
re-run: the bronze (account, message) dedup index makes re-pulls no-ops. Use to
rebuild raw landing; normal scheduled runs should use incremental_sync.py, which
already backfills any sender whose watermark is NULL.
"""
import sys

from etl.db import postgres
from etl.gmail.client import GmailClient
from etl.gmail.sync import GmailSync


def main(account_id):
    meta = postgres.load_account_meta(account_id)
    if not meta:
        raise RuntimeError(f"No google_accounts row for id={account_id!r}")
    user_id, secret_ref = meta
    senders = postgres.load_target_senders(account_id)

    client = GmailClient(secret_ref=secret_ref)
    sync = GmailSync(client, account_id, user_id=user_id, prefix="gmail/raw/")
    sync.run(senders, force=True)

    print(f"backfill done: account={account_id} senders={len(senders)}")


if __name__ == "__main__":
    main(sys.argv[1])                         # account_id passed as job arg
