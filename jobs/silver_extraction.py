"""
Databricks entrypoint: Silver extraction.

For every bronze email NOT yet in silver_email_data:
  1. read the bronze JSON from S3 (browns_email_data.bucket_path),
  2. run the config-free receipt extractor (cleanJson.extract_receipt),
  3. write the silver JSON back to the SAME bucket under gmail/silver/{id}.json,
  4. index it in silver_email_data.

Idempotent BY SELECTION: a bronze row is picked up only while it has no silver
row, so re-runs (or a crash mid-batch) simply resume the remainder. Each email
is landed transactionally — the silver index row is rolled back if its S3 upload
fails, so a committed row always has a matching object.
"""
import json
import os
import sys
import traceback
import uuid

import boto3

from etl.db import postgres
from etl.rawJSON.cleanJson import extract_receipt

SILVER_PREFIX = "gmail/silver/"


def _parse_s3_uri(uri):
    """s3://bucket/key -> (bucket, key)."""
    rest = uri[len("s3://"):] if uri.startswith("s3://") else uri
    bucket, _, key = rest.partition("/")
    return bucket, key


class SilverExtractor:
    def __init__(self, conn=None, bucket=None):
        self.s3 = boto3.client("s3")
        # Same bucket as bronze; silver just lives under a different prefix.
        self.bucket = bucket or os.environ["S3_RAW_BUCKET"]
        self.conn = conn or postgres.get_connection()

    def process(self, user_id, google_account_id, message_id, bronze_uri):
        """Extract one bronze email → silver JSON in S3 + silver index row."""
        # 1. read the bronze record JSON from S3
        src_bucket, src_key = _parse_s3_uri(bronze_uri)
        obj = self.s3.get_object(Bucket=src_bucket, Key=src_key)
        record = json.loads(obj["Body"].read())

        # 2. run the silver extractor
        result = extract_receipt(record)

        # 3 + 4. write silver JSON named by its row id, then index it — atomically.
        silver_id = str(uuid.uuid4())
        key = f"{SILVER_PREFIX}{silver_id}.json"
        silver_uri = f"s3://{self.bucket}/{key}"
        with self.conn:
            with self.conn.cursor() as cur:
                postgres.insert_silver_email(
                    cur,
                    record_id=silver_id,
                    user_id=user_id,
                    google_account_id=google_account_id,
                    message_id=message_id,
                    bucket_path=silver_uri,
                )
            self.s3.put_object(
                Bucket=self.bucket, Key=key,
                Body=json.dumps(result, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )
        return result.get("extraction_status", "extracted")


def main():
    rows = postgres.load_unprocessed_bronze()
    extractor = SilverExtractor()
    extracted = quarantined = failures = 0
    try:
        for user_id, account_id, message_id, bronze_uri in rows:
            try:
                status = extractor.process(user_id, account_id, message_id, bronze_uri)
                if status == "quarantine":
                    quarantined += 1
                else:
                    extracted += 1
            except Exception:
                failures += 1
                print(f"FAILED: message_id={message_id} bronze={bronze_uri}")
                traceback.print_exc()
    finally:
        extractor.conn.close()

    print(
        f"silver extraction done: candidates={len(rows)} extracted={extracted} "
        f"quarantined={quarantined} failures={failures}"
    )
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
