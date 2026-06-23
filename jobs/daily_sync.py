"""
Databricks entrypoint: the automated daily pipeline (fan-out over everyone).

This is the file the SCHEDULER runs — no arguments. It walks the whole tree:

    user → account → sender → fetch → advance that sender's watermark

For each user, for each of their linked Google accounts, GmailSync.run iterates
the account's target senders and syncs each one on its own watermark
(target_senders.last_internal_date): a sender with no watermark is backfilled,
one with a watermark gets only newer mail. The per-account incremental_sync.py /
backfill_sync.py entrypoints are leaves used for one-off / on-demand runs; this
file is what covers "everyone, every day".

Each account is isolated in its own try/except so one bad account (revoked
token, API error) never sinks the whole batch.
"""
import sys
import traceback

from etl.db import postgres
from etl.gmail.client import GmailClient
from etl.gmail.sync import GmailSync


def sync_account(user_id, account_id, secret_ref):
    """Sync one account's senders. Returns the number of senders processed."""
    senders = postgres.load_target_senders(account_id)
    if not senders:
        return 0

    client = GmailClient(secret_ref=secret_ref)
    sync = GmailSync(client, account_id, user_id=user_id, prefix="gmail/raw/")
    try:
        sync.run(senders)        # per sender: fetch + advance its watermark
    finally:
        sync.conn.close()        # don't accumulate connections across the fan-out
    return len(senders)


def main():
    users = synced = accounts = failures = 0
    for user_id in postgres.load_user_ids():
        users += 1
        for account_id, secret_ref in postgres.load_accounts_for_user(user_id):
            accounts += 1
            try:
                n = sync_account(user_id, account_id, secret_ref)
                synced += n
                print(f"ok: user={user_id} account={account_id} senders={n}")
            except Exception:
                failures += 1
                print(f"FAILED: user={user_id} account={account_id}")
                traceback.print_exc()

    print(
        f"daily sync done: users={users} accounts={accounts} "
        f"senders_synced={synced} account_failures={failures}"
    )
    # Non-zero exit if anything failed, so the scheduler surfaces it.
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
