"""
schema.py
────────────────────────────────────────────────────────────────────────────
AWS Redshift warehouse — SQLAlchemy schema (star schema)

Tables
  dim_date      — pre-generated calendar dimension (static, no FK back)
  dim_vendor    — one row per billing sender, synced from scraping_configs
  dim_user      — one row per app user, synced from Postgres users table
  fact_receipts — one row per extracted billing receipt (central fact table)

Usage
  from etl.warehouse.schema import Base, engine
  Base.metadata.create_all(engine)          # create all tables
  Base.metadata.drop_all(engine)            # drop all tables

  # create the schema + seed the calendar in one shot:
  python -m etl.warehouse.schema

Redshift notes
  • Redshift speaks the PostgreSQL wire protocol, so we connect with the standard
    postgresql+psycopg2 driver (already a dependency — no pyodbc/ODBC needed).
  • Redshift does NOT enforce PK/FK/UNIQUE — they are declared for documentation
    and ORM relationship support only.
  • Redshift has NO secondary indexes. Performance is tuned with DISTKEY / SORTKEY
    instead, so create_all emits no CREATE INDEX. To tune later, add the
    sqlalchemy-redshift dialect (__table_args__) or ALTER after creation. Suggested:
      fact_receipts : DISTKEY(user_id), SORTKEY(date_key)
      dimensions    : DISTSTYLE ALL (small lookup tables)
  • JSON columns are stored as VARCHAR(65535) holding a json.dumps() string
    (Redshift TEXT is only VARCHAR(256)). Switch to the SUPER type later for
    in-database JSON querying.
  • Bulk loads should use COPY from S3, not row-by-row inserts.
────────────────────────────────────────────────────────────────────────────
"""

import uuid
from datetime import date, datetime, timedelta

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    SmallInteger,
    String,
    func,
    insert,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, relationship

# JSON-as-text width for Redshift (max VARCHAR). Holds a json.dumps() payload.
JSON_MAX = 65535


# ── Engine ────────────────────────────────────────────────────────────────────
# Built from the discrete REDSHIFT_* env vars via redshift_connector +
# the sqlalchemy-redshift dialect. See etl/warehouse/connection.py.
from etl.warehouse.connection import get_conn, get_engine  # noqa: E402

engine = get_engine()


# ── Base ──────────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


# ─────────────────────────────────────────────────────────────────────────────
# DIM DATE
# Pre-generated calendar table. Generate once with seed_dim_date() below.
# Never updated by the ETL — static reference data.
# ─────────────────────────────────────────────────────────────────────────────

class DimDate(Base):
    __tablename__ = "dim_date"

    # Surrogate key — integer YYYYMMDD format (e.g. 20260115).
    date_key        = Column(Integer, primary_key=True, autoincrement=False,
                             comment="Integer key in YYYYMMDD format")

    full_date       = Column(Date, nullable=False, unique=True,
                             comment="Actual calendar date")
    year            = Column(SmallInteger, nullable=False)
    quarter         = Column(SmallInteger, nullable=False, comment="1-4")
    month           = Column(SmallInteger, nullable=False, comment="1-12")
    month_name      = Column(String(9), nullable=False, comment="January ... December")
    month_short     = Column(String(3), nullable=False, comment="Jan ... Dec")
    week_of_year    = Column(SmallInteger, nullable=False, comment="ISO week number 1-53")
    day_of_week     = Column(SmallInteger, nullable=False, comment="1=Monday ... 7=Sunday (ISO)")
    day_name        = Column(String(9), nullable=False, comment="Monday ... Sunday")
    is_weekend      = Column(Boolean, nullable=False, default=False)
    fiscal_year     = Column(SmallInteger, nullable=True,
                             comment="Set if fiscal year differs from calendar year")
    fiscal_quarter  = Column(SmallInteger, nullable=True)

    receipts        = relationship("FactReceipts", back_populates="date_dim")

    def __repr__(self):
        return f"<DimDate {self.full_date}>"


# ─────────────────────────────────────────────────────────────────────────────
# DIM VENDOR — one row per unique billing sender. SCD Type 1 (overwrite).
# ─────────────────────────────────────────────────────────────────────────────

class DimVendor(Base):
    __tablename__ = "dim_vendor"

    # UUID surrogate key — generated in Python (string form; Redshift has no UUID type).
    vendor_key      = Column(String(36), primary_key=True,
                             default=lambda: str(uuid.uuid4()),
                             comment="UUID surrogate key for the vendor")

    sender_email    = Column(String(255), nullable=False, unique=True,
                             comment="Gmail sender address e.g. receipts@stripe.com")
    vendor_name     = Column(String(255), nullable=True,
                             comment="Human-readable name from config or email")
    category        = Column(String(100), nullable=True,
                             comment="e.g. SaaS, Cloud, Marketing, Finance")
    template_hash   = Column(String(64), nullable=True,
                             comment="SHA-256 of the sender's email HTML structure")
    config_confidence = Column(Numeric(4, 3), nullable=True,
                               comment="0.000-1.000 LLM confidence in selectors")
    is_active       = Column(Boolean, nullable=False, default=True,
                             comment="False if sender config was revoked or removed")
    synced_at       = Column(DateTime, nullable=False, server_default=text("GETDATE()"),
                             comment="Last sync from Postgres scraping_configs")

    receipts        = relationship("FactReceipts", back_populates="vendor")

    def __repr__(self):
        return f"<DimVendor {self.vendor_name or self.sender_email}>"


# ─────────────────────────────────────────────────────────────────────────────
# DIM USER — one row per application user. account_ids stored as JSON text.
# ─────────────────────────────────────────────────────────────────────────────

class DimUser(Base):
    __tablename__ = "dim_user"

    # Same UUID as Postgres users.id — avoids surrogate key mapping.
    user_key        = Column(String(36), primary_key=True,
                             comment="Matches users.id in Postgres")
    email           = Column(String(255), nullable=False)
    name            = Column(String(255), nullable=True)
    # RBAC: "user" sees own receipts; "super_admin" sees all.
    role            = Column(String(20), nullable=False, default="user",
                             comment="user | super_admin")
    # JSON array of connected google_accounts.id strings, e.g. ["uuid-1", ...]
    account_ids     = Column(String(JSON_MAX), nullable=True,
                             comment="JSON array of connected Google account UUIDs")
    is_active       = Column(Boolean, nullable=False, default=True,
                             comment="False if user was deactivated in the app")
    synced_at       = Column(DateTime, nullable=False, server_default=text("GETDATE()"),
                             comment="Last sync from Postgres users table")

    receipts        = relationship("FactReceipts", back_populates="user")

    def __repr__(self):
        return f"<DimUser {self.email} role={self.role}>"


# ─────────────────────────────────────────────────────────────────────────────
# FACT RECEIPTS — one row per extracted billing receipt email.
# Natural key: email_id (Gmail message ID). Recommended DISTKEY(user_id),
# SORTKEY(date_key) — all analytics filter by user.
# ─────────────────────────────────────────────────────────────────────────────

class FactReceipts(Base):
    __tablename__ = "fact_receipts"

    # Gmail message ID — globally unique, used for idempotent upserts.
    email_id        = Column(String(32), primary_key=True,
                             comment="Gmail message ID — natural key for upsert")

    # ── Foreign keys (declared, not enforced by Redshift) ─────────────────────
    user_id         = Column(String(36),
                             ForeignKey("dim_user.user_key", name="fk_receipts_user"),
                             nullable=False, comment="Matches dim_user.user_key")
    vendor_key      = Column(String(36),
                             ForeignKey("dim_vendor.vendor_key", name="fk_receipts_vendor"),
                             nullable=True, comment="FK to dim_vendor; null if unknown")
    date_key        = Column(Integer,
                             ForeignKey("dim_date.date_key", name="fk_receipts_date"),
                             nullable=True, comment="YYYYMMDD of receipt date; null if unparseable")

    # ── Account context (degenerate dimension) ────────────────────────────────
    account_id      = Column(String(36), nullable=False,
                             comment="Matches google_accounts.id in Postgres")

    # ── Amounts (original currency; no normalisation here) ────────────────────
    currency        = Column(String(3), nullable=True, comment="ISO 4217 e.g. USD, INR")
    amount          = Column(Numeric(14, 4), nullable=True, comment="Amount in `currency`")
    tax             = Column(Numeric(14, 4), nullable=True, comment="Tax in `currency`")
    total           = Column(Numeric(14, 4), nullable=True,
                             comment="Total in `currency` — primary analytics field")

    # ── Line items + flexible metadata (JSON as text) ─────────────────────────
    line_items      = Column(String(JSON_MAX), nullable=True,
                             comment="Extracted line items array (JSON string)")
    # Python attr is `meta` (SQLAlchemy reserves `metadata`); DB column is "metadata".
    meta            = Column("metadata", String(JSON_MAX), nullable=True,
                             comment="Flexible per-receipt metadata as JSON string")

    # ── Dates + degenerate dimension ──────────────────────────────────────────
    receipt_date    = Column(Date, nullable=True, comment="Date on the receipt")
    email_date      = Column(Date, nullable=True, comment="Date the email was received")
    invoice_number  = Column(String(100), nullable=True,
                             comment="Invoice/receipt number — degenerate dimension")

    # ── Extraction quality ────────────────────────────────────────────────────
    confidence_score = Column(Numeric(4, 3), nullable=True,
                              comment="0.000-1.000 extraction confidence")
    extraction_status = Column(String(20), nullable=False, default="extracted",
                               comment="extracted | quarantine | no_config | error")

    # ── Anomaly detection (Z-score per vendor+user window) ────────────────────
    anomaly_flag    = Column(Boolean, nullable=False, default=False,
                             comment="True if amount is a statistical outlier")
    anomaly_z_score = Column(Numeric(8, 4), nullable=True,
                             comment="Z-score of total within vendor+user window")

    # ── Soft delete (never hard-delete; queries filter deleted_at IS NULL) ────
    deleted_at      = Column(DateTime, nullable=True,
                             comment="Soft delete timestamp — null means active")

    # ── Lineage (degenerate dimension) — silver record this fact came from ────
    # Soft reference: silver_email_data lives in Postgres, this fact in Redshift
    # (no cross-DB FK). Resolve the S3 JSON path via silver_email_data.bucket_path.
    silver_id       = Column(String(36), nullable=True,
                             comment="silver_email_data.id this row was extracted from")

    # ── Pipeline timestamps ───────────────────────────────────────────────────
    extracted_at    = Column(DateTime, nullable=True, comment="When extraction ran")
    loaded_at       = Column(DateTime, nullable=False, server_default=text("GETDATE()"),
                             onupdate=datetime.utcnow,
                             comment="When last written to the warehouse — ETL watermark")

    user            = relationship("DimUser", back_populates="receipts")
    vendor          = relationship("DimVendor", back_populates="receipts")
    date_dim        = relationship("DimDate", back_populates="receipts")

    def __repr__(self):
        return f"<FactReceipts email_id={self.email_id} total={self.total} vendor_key={self.vendor_key}>"


# ─────────────────────────────────────────────────────────────────────────────
# DIM DATE SEEDER — call once after create. Generates one row per calendar day.
# ─────────────────────────────────────────────────────────────────────────────

def seed_dim_date(
    start: date = date(2020, 1, 1),
    end: date = date(2030, 12, 31),
    fiscal_year_start_month: int = 1,
) -> None:
    """Populate dim_date with one row per day between start and end. Re-runnable."""
    DAY_NAMES   = ["Monday", "Tuesday", "Wednesday", "Thursday",
                   "Friday", "Saturday", "Sunday"]
    MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
                   "July", "August", "September", "October", "November", "December"]

    def fiscal_year_quarter(d: date, fy_start_month: int):
        if fy_start_month == 1:
            return d.year, (d.month - 1) // 3 + 1
        shifted_month = (d.month - fy_start_month) % 12
        fy = d.year if d.month >= fy_start_month else d.year - 1
        return fy, shifted_month // 3 + 1

    records = []
    current = start
    while current <= end:
        iso_cal  = current.isocalendar()
        fy, fq   = fiscal_year_quarter(current, fiscal_year_start_month)
        records.append(dict(
            date_key       = int(current.strftime("%Y%m%d")),
            full_date      = current,
            year           = current.year,
            quarter        = (current.month - 1) // 3 + 1,
            month          = current.month,
            month_name     = MONTH_NAMES[current.month - 1],
            month_short    = MONTH_NAMES[current.month - 1][:3],
            week_of_year   = iso_cal[1],
            day_of_week    = iso_cal[2],
            day_name       = DAY_NAMES[iso_cal[2] - 1],
            is_weekend     = iso_cal[2] in (6, 7),
            fiscal_year    = fy,
            fiscal_quarter = fq,
        ))
        current += timedelta(days=1)

    # Redshift is a warehouse: row-by-row INSERTs are pathologically slow, so
    # insert in a few multi-row statements. Skip entirely if already seeded.
    table = DimDate.__table__
    with engine.begin() as conn:
        existing = conn.execute(select(func.count()).select_from(table)).scalar()
        if existing:
            print(f"dim_date already seeded ({existing} rows) — skipping")
            return
        CHUNK = 1000
        for i in range(0, len(records), CHUNK):
            conn.execute(insert(table).values(records[i:i + CHUNK]))

    print(f"dim_date seeded: {start} → {end} ({len(records)} rows)")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT — create schema + seed dim_date
#   python -m etl.warehouse.schema
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Table DDL is owned by Alembic — run `alembic upgrade head` first to create
    # or migrate the schema. This entrypoint only seeds the static calendar.
    print("Seeding dim_date (2020-2030)... (run `alembic upgrade head` first)")
    seed_dim_date(start=date(2020, 1, 1), end=date(2030, 12, 31), fiscal_year_start_month=1)
    print("Done.")
