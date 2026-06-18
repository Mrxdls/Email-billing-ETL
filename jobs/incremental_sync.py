"""
Databricks entrypoint: Gmail sync (per-sender, incremental).

Each target sender carries its own watermark (target_senders.last_internal_date):
  • watermark NULL → that sender is fully backfilled (first run, or just added)
  • watermark set  → only mail at/after it is pulled
So this single entrypoint handles both first-load and steady-state, per sender.
Use backfill_sync.py only to FORCE a re-pull ignoring the watermarks.
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
    sync.run(senders)

    print(f"sync done: account={account_id} senders={len(senders)}")


if __name__ == "__main__":
    main(sys.argv[1])                         # account_id passed as job arg
