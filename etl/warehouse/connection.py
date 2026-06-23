"""
connection.py
────────────────────────────────────────────────────────────────────────────
Amazon Redshift connections, built from discrete env vars:

    REDSHIFT_HOST       workgroup/cluster endpoint
    REDSHIFT_PORT       usually 5439
    REDSHIFT_DB         database name (default `dev`)
    REDSHIFT_USER       admin/login user
    REDSHIFT_PASSWORD   password

Two ways to talk to Redshift, both off the SAME env vars:

  get_conn()    raw `redshift_connector` connection — for COPY-from-S3, bulk
                DDL, and quick queries.
  get_engine()  SQLAlchemy engine over the `redshift+redshift_connector`
                dialect — for the ORM (Base.metadata.create_all) and MERGE.

Redshift requires SSL; redshift_connector enables it by default.
"""
import os

import redshift_connector
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Load ETL/.env regardless of cwd (this file is ETL/etl/warehouse/connection.py).
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))


def _params() -> dict:
    return {
        "host": os.environ["REDSHIFT_HOST"],
        "database": os.environ["REDSHIFT_DB"],
        "port": int(os.environ.get("REDSHIFT_PORT", 5439)),
        "user": os.environ["REDSHIFT_USER"],
        "password": os.environ["REDSHIFT_PASSWORD"],
    }


def get_conn():
    """Raw redshift_connector connection (COPY / bulk DDL / quick queries)."""
    return redshift_connector.connect(ssl=True, **_params())


def get_engine():
    """SQLAlchemy engine over redshift_connector (ORM create_all / Alembic)."""
    p = _params()
    url = URL.create(
        "redshift+redshift_connector",
        username=p["user"],
        password=p["password"],
        host=p["host"],
        port=p["port"],
        database=p["database"],
    )
    engine = create_engine(url)
    # Redshift has NO `... RETURNING`. The sqlalchemy-redshift dialect (built on
    # the Postgres dialect, which does) fails to disable it under SQLAlchemy 2.0,
    # so Alembic's `INSERT INTO alembic_version ... RETURNING` — and any ORM
    # insert that would use RETURNING — errors out. Force it off.
    engine.dialect.insert_returning = False
    engine.dialect.update_returning = False
    engine.dialect.delete_returning = False
    # Because Redshift doesn't support the RETURNING clause for INSERT, UPDATE, and DELETE statements
    return engine
