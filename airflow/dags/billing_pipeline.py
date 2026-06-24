"""
billing_pipeline — daily Gmail → bronze → silver pipeline.

Airflow only ORCHESTRATES. Each task runs the existing ETL entrypoint using the
ETL project's OWN virtualenv (BashOperator), so Airflow's dependencies never
collide with the ETL's (notably SQLAlchemy 1.4 vs 2.0). The ETL loads its own
.env, so no env wiring is needed here.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ETL project root — this DAG lives at ETL/airflow/dags/, so ../../ is the ETL
# root. Overridable via the ETL_PROJECT_DIR env var.
ETL_DIR = os.environ.get(
    "ETL_PROJECT_DIR",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
)
PY = f"{ETL_DIR}/venv/bin/python"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _run(module: str) -> str:
    """Bash command: run an ETL module with the ETL venv from the ETL root."""
    return f'cd "{ETL_DIR}" && "{PY}" -m {module}'


with DAG(
    dag_id="billing_pipeline",
    description="Daily medallion pipeline: Gmail -> bronze -> silver -> gold (Redshift)",
    schedule="0 2 * * *",                 # every day at 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "gmail", "medallion"],
) as dag:

    bronze_sync = BashOperator(
        task_id="bronze_sync",
        bash_command=_run("jobs.daily_sync"),
    )

    silver_extraction = BashOperator(
        task_id="silver_extraction",
        bash_command=_run("jobs.silver_extraction"),
    )

    gold_load = BashOperator(
        task_id="gold_load",
        bash_command=_run("jobs.gold_load"),
    )

    bronze_sync >> silver_extraction >> gold_load
