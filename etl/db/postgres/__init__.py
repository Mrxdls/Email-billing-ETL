"""
Postgres client (operational app DB, schema-on-read, NO migrations).

This ETL only reads/updates fields here — it does not own this DB's schema.
The table/column contract is mirrored (read-only) in `schema.py`; the backend
(Drizzle) is the single source of truth for the actual schema and migrations.
The Gmail sync watermark lives PER-SENDER in target_senders.last_internal_date.

Env var: DATABASE_URL  (e.g. postgresql://user:password@host:5432/dbname)
"""
import os
import dotenv

import psycopg2
dotenv.load_dotenv(dotenv_path='/home/skynet/Downloads/Data engineering/ETL/.env')
 # for local dev; in prod these are set directly in the env
def get_connection():
    # Connect via a single connection URL (DSN) — the same DATABASE_URL the
    # backend uses, e.g. postgresql://user:password@host:5432/dbname?sslmode=require
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _query_all(sql, params=()):
    """Run a read-only query and return all rows. Opens AND closes its own conn
    (psycopg2's `with conn` commits but does not close — a leak across a fan-out)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def _query_one(sql, params=()):
    """Run a read-only query and return the first row (or None)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    finally:
        conn.close()


def load_user_ids():
    """All application user ids, oldest first — the outer loop of the pipeline."""
    return [row[0] for row in _query_all("SELECT id FROM users ORDER BY created_at")]


def load_accounts_for_user(user_id):
    """A user's linked accounts as (account_id, secret_ref) tuples."""
    return _query_all(
        "SELECT id, secret_ref FROM google_accounts WHERE user_id = %s ORDER BY created_at",
        (user_id,),
    )


def load_account_meta(account_id):
    """Resolve the fields a single-account run needs: (user_id, secret_ref)."""
    return _query_one(
        "SELECT user_id, secret_ref FROM google_accounts WHERE id = %s",
        (account_id,),
    )


def load_target_senders(account_id):
    """
    Return the tracked senders for an account, each with its own watermark:
    a list of (sender_id, sender_email, last_internal_date) tuples.
    last_internal_date is None for a sender that has never been synced.
    """
    return _query_all(
        "SELECT id, sender_email, last_internal_date "
        "FROM target_senders WHERE google_account_id = %s",
        (account_id,),
    )


def update_sender_watermark(cur, sender_id, last_internal_date):
    """
    Advance one sender's watermark using an EXISTING cursor (caller commits).

    last_internal_date is COALESCEd so a run that fetched nothing new keeps the
    prior watermark while still stamping last_synced_at.
    """
    cur.execute(
        "UPDATE target_senders "
        "SET last_internal_date = COALESCE(%s, last_internal_date), "
        "    last_synced_at = now(), sync_status = 'done' "
        "WHERE id = %s",
        (last_internal_date, sender_id),
    )


def load_unprocessed_bronze():
    """
    Bronze rows that have NO silver row yet — the silver work-list.
    Returns (user_id, google_account_id, message_id, bucket_path) tuples,
    matched on (google_account_id, message_id) so each email is silvered once.
    """
    return _query_all(
        "SELECT b.user_id, b.google_account_id, b.message_id, b.bucket_path "
        "FROM browns_email_data b "
        "LEFT JOIN silver_email_data s "
        "  ON s.google_account_id = b.google_account_id "
        " AND s.message_id = b.message_id "
        "WHERE s.id IS NULL "
        "ORDER BY b.created_at"
    )


def insert_silver_email(cur, *, record_id, user_id, google_account_id, message_id, bucket_path):
    """INSERT one silver-layer index row using an EXISTING cursor (caller commits)."""
    cur.execute(
        "INSERT INTO silver_email_data "
        "(id, user_id, google_account_id, message_id, bucket_path) "
        "VALUES (%s, %s, %s, %s, %s)",
        (record_id, user_id, google_account_id, message_id, bucket_path),
    )


def insert_bronze_email(cur, *, record_id, user_id, google_account_id, message_id,
                        subject, sender, receiver, date, snippet, bucket_path):
    """
    INSERT one bronze-layer email row using an EXISTING cursor (no commit here).

    Idempotent: ON CONFLICT against the (google_account_id, message_id) unique
    index DO NOTHING, so a re-pull of the same email is a no-op. Returns the new
    row id when inserted, or None when the row already existed — the caller uses
    that to decide whether to upload the JSON. `from`/`to` are SQL reserved
    words, hence quoted.

    The caller owns the transaction: insert, upload the JSON to S3, then commit —
    or roll back if the upload fails — so a committed row always has a matching
    object.
    """
    cur.execute(
        'INSERT INTO browns_email_data '
        '(id, user_id, google_account_id, message_id, subject, "from", "to", '
        ' date, snippet, bucket_path) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) '
        'ON CONFLICT (google_account_id, message_id) DO NOTHING '
        'RETURNING id',
        (record_id, user_id, google_account_id, message_id, subject, sender,
         receiver, date, snippet, bucket_path),
    )
    row = cur.fetchone()
    return row[0] if row else None
