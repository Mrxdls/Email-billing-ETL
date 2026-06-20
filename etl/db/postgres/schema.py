"""
schema.py
────────────────────────────────────────────────────────────────────────────
READ-ONLY mirror of the backend's operational Postgres schema.

The backend (Drizzle) OWNS these tables and is the single source of truth for
their structure and migrations — see Backend/src/infra/db/schema.ts. This module
exists so the ETL can build correct, type-aware queries against those tables
WITHOUT owning or migrating them.

HARD RULES
  • Never call `metadata.create_all()` / `drop_all()` on this metadata.
  • Never add a column here to "make the ETL work" — add it in the backend
    Drizzle schema + migration first, then mirror it here.
  • The ETL SELECTs from these tables, UPDATEs only the per-sender watermark
    columns on target_senders, and INSERTs bronze rows. Enforce that at the DB
    level with a column-scoped role:
        GRANT SELECT ON users, google_accounts, target_senders TO etl_writer;
        GRANT UPDATE (last_internal_date, last_synced_at, sync_status)
            ON target_senders TO etl_writer;
        GRANT INSERT ON browns_email_data TO etl_writer;

Keep this file in sync with Backend/src/infra/db/schema.ts.
"""
from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    ForeignKey,
    MetaData,
    Table,
    Text,
    TIMESTAMP,
)
from sqlalchemy.dialects.postgresql import UUID

# A DEDICATED metadata, kept separate from the warehouse metadata so this schema
# can never be accidentally created/dropped by a warehouse create_all().
metadata = MetaData()


# An application user. Identity = their primary linked Google account.
users = Table(
    "users",
    metadata,
    Column("id", UUID(as_uuid=False), primary_key=True),
    Column("email", Text, nullable=False, unique=True),
    Column("name", Text),
    Column("picture", Text),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False),
)


# A Google account linked to a user. Tokens are NOT stored here — `secret_ref`
# points to the secret-store entry holding access_token + refresh_token.
google_accounts = Table(
    "google_accounts",
    metadata,
    Column("id", UUID(as_uuid=False), primary_key=True),
    Column(
        "user_id",
        UUID(as_uuid=False),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("google_sub", Text, nullable=False, unique=True),
    Column("email", Text, nullable=False),
    Column("scope", Text),
    Column("secret_ref", Text, nullable=False, unique=True),
    Column("expiry_date", BigInteger),
    # Exactly one primary per user; its email is the backend login identity.
    Column("is_primary", Boolean, nullable=False),
    # NOTE: Gmail sync state lives PER-SENDER on target_senders, not here.
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False),
)


# Per-account "target senders": the addresses a user wants tracked for a given
# linked Google account. `user_id` is denormalized from the owning account.
target_senders = Table(
    "target_senders",
    metadata,
    Column("id", UUID(as_uuid=False), primary_key=True),
    Column(
        "user_id",
        UUID(as_uuid=False),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "google_account_id",
        UUID(as_uuid=False),
        ForeignKey("google_accounts.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("sender_email", Text, nullable=False),
    # ── Per-sender watermark the ETL writes ─────────────────────────────────
    # `last_internal_date` = max Gmail internalDate (epoch ms) fetched for this
    # sender; NULL → never synced → next run fully backfills this sender.
    Column("last_internal_date", BigInteger),
    Column("last_synced_at", TIMESTAMP(timezone=True)),
    Column("sync_status", Text),  # idle | queued | running | done | error
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False),
)


# Bronze landing index: one row per raw email JSON in S3. The backend owns the
# schema (Drizzle); the ETL only INSERTs here (ON CONFLICT DO NOTHING against the
# (google_account_id, message_id) unique index → idempotent re-pulls).
browns_email_data = Table(
    "browns_email_data",
    metadata,
    Column("id", UUID(as_uuid=False), primary_key=True),
    Column(
        "user_id",
        UUID(as_uuid=False),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "google_account_id",
        UUID(as_uuid=False),
        ForeignKey("google_accounts.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("message_id", Text, nullable=False),
    Column("subject", Text),
    Column("from", Text),
    Column("to", Text),
    Column("date", Text),
    Column("snippet", Text),
    Column("bucket_path", Text, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
)
