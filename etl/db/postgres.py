"""
Postgres client (operational app DB, schema-on-read, NO migrations).

The Gmail sync watermark lives in google_accounts.gmail_history_id.

Env vars: PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
"""
import os

import psycopg2


def get_connection():
    return psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=os.environ.get("PG_PORT", 5432),
        dbname=os.environ["PG_DATABASE"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )


def load_history_id(account_id):
    """Return the stored Gmail history_id for an account, or None (→ backfill)."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT gmail_history_id FROM google_accounts WHERE id = %s",
            (account_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def save_history_id(account_id, history_id):
    """Persist the new watermark after a successful sync."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE google_accounts SET gmail_history_id = %s, last_synced_at = now() "
            "WHERE id = %s",
            (history_id, account_id),
        )
        conn.commit()
