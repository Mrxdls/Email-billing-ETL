"""
transform.py
────────────────────────────────────────────────────────────────────────────
Load silver JSON records into a pandas DataFrame and (eventually) shape them for
the gold warehouse. WIP.
"""
import json
import os

import boto3
import pandas as pd

SILVER_PREFIX = "gmail/silver/"


def load_silver_df(bucket=None, prefix=SILVER_PREFIX):
    """Read every silver JSON under s3://{bucket}/{prefix} into a DataFrame."""
    bucket = bucket or os.environ["S3_RAW_BUCKET"]
    s3 = boto3.client("s3")
    records = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                records.append(json.loads(body))
    return pd.DataFrame(records)


def get_silver_df():
    return load_silver_df()


def pre_processing(df):
    """Preprocess the DataFrame before loading into Redshift."""
    # Example: convert date strings to datetime objects
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    # Example: fill missing values
    df.fillna({'some_column': 'default_value'}, inplace=True)
    # TODO: map to fact_receipts columns
    return df


print(pre_processing(get_silver_df()))
