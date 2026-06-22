"""
Databricks entrypoint: the automated daily pipeline (fan-out over everyone).

Walks user -> account -> sender and syncs each account. WIP.
"""
import sys
import traceback

from etl.db.postgres import
from etl.gmail.client import GmailClient
from etl.gmail.sync import GmailSync


def main():
    for user_id in postgres.load_user_ids():
        for account_id, secret_ref in postgres.load_accounts_for_user(user_id):
            try:
                history_id = postgres.load_history_id(account_id)
                client = GmailClient(secret_ref=secret_ref)
                sync = GmailSync(client, account_id, [], prefix="gmail/raw/")
                if history_id:
                    new_history_id = sync.incremental(history_id)
                else:
                    new_history_id = sync.backfill()
                postgres.save_history_id(account_id, new_history_id)
            except Exception:
                traceback.print_exc()


if __name__ == "__main__":
    main()
