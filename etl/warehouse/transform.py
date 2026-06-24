"""
transform.py
────────────────────────────────────────────────────────────────────────────
Silver → gold loader for the Redshift warehouse, as a single class.

SilverToGold.run() does the whole job, in FK-safe order:
  1. load the silver records not yet in Redshift (cross-DB anti-join + S3 fetch)
  2. refresh dimensions — insert any NEW vendors/users seen in this batch
  3. map silver → a fact_receipts-ready DataFrame (columns driven by the schema)
  4. resolve the dimension foreign keys (vendor_key) onto the fact rows
  5. INSERT the rows into Redshift fact_receipts

Dims are updated in step 2 — BEFORE the fact is finalised — so every FK resolves.
"""
import json
import os
import uuid
from email.utils import parseaddr

import boto3
import pandas as pd
from sqlalchemy import insert

from etl.db import postgres
from etl.warehouse.connection import get_conn, get_engine
from etl.warehouse.schema import FactReceipts


class SilverToGold:
    SILVER_PREFIX = "gmail/silver/"
    # Fact columns straight from the model (single source of truth, not hardcoded).
    FACT_COLUMNS = [c.name for c in FactReceipts.__table__.columns]
    # loaded_at has a server default (GETDATE()), so we never insert it.
    INSERT_COLUMNS = [c for c in FACT_COLUMNS if c != "loaded_at"]

    def __init__(self, bucket=None):
        self.bucket = bucket or os.environ["S3_RAW_BUCKET"]
        self.s3 = boto3.client("s3")

    # ── 1. load silver ──────────────────────────────────────────────────────

    def get_silver_ids(self):
        """
        Silver rows not yet in Redshift. silver_email_data is in Postgres and
        fact_receipts in Redshift — no cross-DB JOIN — so anti-join in Python:
        read the silver_ids already in Redshift, keep the Postgres rows not in it.
        Returns (silver_id, user_id, google_account_id, message_id, bucket_path).
        """
        rconn = get_conn()
        try:
            cur = rconn.cursor()
            cur.execute("SELECT silver_id FROM fact_receipts WHERE silver_id IS NOT NULL")
            loaded = {str(r[0]) for r in cur.fetchall()}
        finally:
            rconn.close()

        rows = postgres._query_all(
            "SELECT id, user_id, google_account_id, message_id, bucket_path "
            "FROM silver_email_data"
        )
        return [r for r in rows if str(r[0]) not in loaded]

    def get_silver_df(self):
        """Read each not-yet-loaded silver JSON from S3 into a DataFrame."""
        records = []
        for silver_id, user_id, account_id, message_id, bucket_path in self.get_silver_ids():
            key = f"{self.SILVER_PREFIX}{silver_id}.json"
            body = self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
            rec = json.loads(body)
            rec["silver_id"] = silver_id
            rec["user_id"] = user_id
            rec["google_account_id"] = account_id
            records.append(rec)
        return pd.DataFrame(records)

    # ── 2. refresh dimensions (insert new members) ────────────────────────────

    @staticmethod
    def sender_email_of(sender):
        """'Uber <noreply@uber.com>' -> 'noreply@uber.com' (lowercased)."""
        return (parseaddr(sender or "")[1] or "").lower() or None

    def upsert_dim_vendor(self, df):
        """Insert vendors new to this batch; return {sender_email -> vendor_key} for all."""
        tmp = (df.assign(sender_email=df["sender"].map(self.sender_email_of))
                 .dropna(subset=["sender_email"]).drop_duplicates("sender_email"))
        batch = {r["sender_email"]: (r.get("vendor"), r.get("category"), r.get("template_hash"))
                 for _, r in tmp.iterrows()}

        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT sender_email, vendor_key FROM dim_vendor")
            keymap = {se: str(vk) for se, vk in cur.fetchall()}
            for se, (vname, cat, th) in batch.items():
                if se in keymap:
                    continue                                   # SCD-1: leave existing as-is
                vk = str(uuid.uuid4())
                cur.execute(
                    "INSERT INTO dim_vendor "
                    "(vendor_key, sender_email, vendor_name, category, template_hash, is_active, synced_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, GETDATE())",
                    (vk, se, vname, cat, th, True),
                )
                keymap[se] = vk
            conn.commit()
        finally:
            conn.close()
        return keymap

    def sync_dim_user(self, user_ids):
        """Insert dim_user rows new to this batch (sourced from Postgres users)."""
        ids = [str(u) for u in set(user_ids) if u]
        if not ids:
            return set()
        ph = ",".join(["%s"] * len(ids))
        users = postgres._query_all(f"SELECT id, email, name FROM users WHERE id IN ({ph})", tuple(ids))
        accts = postgres._query_all(
            f"SELECT user_id, id FROM google_accounts WHERE user_id IN ({ph})", tuple(ids))
        acct_map = {}
        for uid, aid in accts:
            acct_map.setdefault(str(uid), []).append(str(aid))

        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT user_key FROM dim_user")
            existing = {str(r[0]) for r in cur.fetchall()}
            for uid, email, name in users:
                uid = str(uid)
                if uid in existing:
                    continue
                cur.execute(
                    "INSERT INTO dim_user (user_key, email, name, role, account_ids, is_active, synced_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, GETDATE())",
                    (uid, email, name, "user", json.dumps(acct_map.get(uid, [])), True),
                )
                existing.add(uid)
            conn.commit()
        finally:
            conn.close()
        return existing

    # ── 3. map silver → fact ──────────────────────────────────────────────────

    def to_fact_df(self, df, drop_quarantine=True):
        """Map silver DataFrame → fact_receipts-ready DataFrame (schema column set)."""
        d = df.copy()
        if drop_quarantine:
            d = d[d["extraction_status"] != "quarantine"].copy()

        email_dt = pd.to_datetime(d["email_date"], errors="coerce", utc=True, format="mixed")
        extracted_dt = pd.to_datetime(d["extracted_at"], errors="coerce", utc=True, format="mixed")

        def _dumps(v):
            return None if v is None else json.dumps(v, ensure_ascii=False)

        def _meta(r):
            m = dict(r["metadata"] or {})
            if r.get("summary"):
                m["_summary"] = r["summary"]
            return json.dumps(m, ensure_ascii=False)

        out = pd.DataFrame({
            "email_id":          d["message_id"],
            "user_id":           d["user_id"],
            "vendor_key":        pd.NA,                              # resolved in step 4
            "date_key":          pd.to_numeric(email_dt.dt.strftime("%Y%m%d"), errors="coerce").astype("Int64"),
            "account_id":        d["account_id"].fillna(d["google_account_id"]),
            "currency":          d["currency"],
            "amount":            pd.NA,
            "tax":               pd.NA,
            "total":             pd.to_numeric(d["total_amount"], errors="coerce"),
            "line_items":        d["line_items"].apply(_dumps),
            "metadata":          d.apply(_meta, axis=1),
            "receipt_date":      email_dt.dt.date,
            "email_date":        email_dt.dt.date,
            "invoice_number":    d["transaction_id"],
            "confidence_score":  pd.to_numeric(d["confidence"], errors="coerce"),
            "extraction_status": d["extraction_status"],
            "anomaly_flag":      False,
            "anomaly_z_score":   pd.NA,
            "deleted_at":        pd.NaT,
            "silver_id":         d["silver_id"],
            "extracted_at":      extracted_dt.dt.tz_localize(None),  # Redshift timestamp has no tz
        })
        for c in self.INSERT_COLUMNS:
            if c not in out.columns:
                out[c] = pd.NA
        return out[self.INSERT_COLUMNS].reset_index(drop=True)

    # ── 5. insert into Redshift ────────────────────────────────────────────────

    def load_fact(self, fact_df, chunk=500):
        """Insert fact rows into Redshift in multi-row batches (row-by-row is slow)."""
        if fact_df.empty:
            return 0
        records = fact_df.astype(object).where(pd.notnull(fact_df), None).to_dict("records")
        table = FactReceipts.__table__
        eng = get_engine()
        with eng.begin() as conn:
            for i in range(0, len(records), chunk):
                conn.execute(insert(table).values(records[i:i + chunk]))
        return len(records)

    # ── orchestrate ────────────────────────────────────────────────────────────

    def run(self):
        """Full silver → gold: load, refresh dims, map, resolve FKs, insert."""
        df = self.get_silver_df()
        print(f"silver not yet in gold: {len(df)}")
        if df.empty:
            return {"silver": 0, "fact_loaded": 0}

        # 2. dims FIRST so the FKs resolve
        vendor_map = self.upsert_dim_vendor(df)
        self.sync_dim_user(df["user_id"].unique())
        print(f"dim_vendor members: {len(vendor_map)}")

        # 3 + 4. map and resolve vendor_key (by each receipt's sender)
        fact_df = self.to_fact_df(df)
        mid_to_vendor = {r["message_id"]: vendor_map.get(self.sender_email_of(r["sender"]))
                         for _, r in df.iterrows()}
        fact_df["vendor_key"] = fact_df["email_id"].map(mid_to_vendor)

        null_fk = int(fact_df["vendor_key"].isna().sum())
        if null_fk:
            print(f"WARNING: {null_fk} rows have an unresolved vendor_key")

        # 5. insert
        n = self.load_fact(fact_df)
        print(f"inserted {n} rows into fact_receipts")
        return {"silver": len(df), "fact_loaded": n, "vendors": len(vendor_map)}


if __name__ == "__main__":
    print(SilverToGold().run())
