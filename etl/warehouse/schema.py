"""
schema.py
────────────────────────────────────────────────────────────────────────────
Azure Synapse warehouse — SQLAlchemy schema (star schema).

Connects to a Synapse dedicated SQL pool over pyodbc (ODBC Driver 18 for SQL
Server). Dimensions + a fact_receipts central table.

Env vars: SYNAPSE_SERVER, SYNAPSE_DATABASE, SYNAPSE_USER, SYNAPSE_PASSWORD
"""
import os
import uuid

from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, Numeric, create_engine, text
from sqlalchemy.dialects.mssql import NVARCHAR, UNIQUEIDENTIFIER
from sqlalchemy.engine import URL
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def get_engine():
    url = URL.create(
        "mssql+pyodbc",
        username=os.environ["SYNAPSE_USER"],
        password=os.environ["SYNAPSE_PASSWORD"],
        host=os.environ["SYNAPSE_SERVER"],
        database=os.environ["SYNAPSE_DATABASE"],
        query={"driver": "ODBC Driver 18 for SQL Server"},
    )
    return create_engine(url)


engine = get_engine()


class DimVendor(Base):
    __tablename__ = "dim_vendor"
    vendor_key = Column(UNIQUEIDENTIFIER, primary_key=True, default=uuid.uuid4)
    sender_email = Column(NVARCHAR(255), nullable=False, unique=True)
    vendor_name = Column(NVARCHAR(255))
    category = Column(NVARCHAR(100))
    synced_at = Column(DateTime, server_default=text("GETUTCDATE()"))


class DimUser(Base):
    __tablename__ = "dim_user"
    user_key = Column(UNIQUEIDENTIFIER, primary_key=True)
    email = Column(NVARCHAR(255), nullable=False)
    name = Column(NVARCHAR(255))
    synced_at = Column(DateTime, server_default=text("GETUTCDATE()"))


class FactReceipts(Base):
    __tablename__ = "fact_receipts"
    email_id = Column(NVARCHAR(64), primary_key=True)
    user_id = Column(UNIQUEIDENTIFIER, ForeignKey("dim_user.user_key"), nullable=False)
    vendor_key = Column(UNIQUEIDENTIFIER, ForeignKey("dim_vendor.vendor_key"))
    account_id = Column(UNIQUEIDENTIFIER, nullable=False)
    sender_email = Column(NVARCHAR(255))
    subject = Column(NVARCHAR(500))
    currency = Column(NVARCHAR(3))
    total = Column(Numeric(14, 4))
    line_items = Column(NVARCHAR(None))
    loaded_at = Column(DateTime, server_default=text("GETUTCDATE()"))


if __name__ == "__main__":
    Base.metadata.create_all(engine)
    print("synapse schema created")
