"""
Databricks/Airflow entrypoint: gold load.

Silver → Redshift warehouse (dims + fact_receipts). Thin wrapper — the logic
lives in etl.warehouse.transform.SilverToGold, matching how jobs.daily_sync and
jobs.silver_extraction wrap their etl.* logic. Run as a module from the ETL root:

    python -m jobs.gold_load
"""
from etl.warehouse.transform import SilverToGold


def main():
    result = SilverToGold().run()
    print(f"gold load done: {result}")
    return result


if __name__ == "__main__":
    main()
