# ETL image — Apache Airflow + an ISOLATED virtualenv for the ETL jobs.
#
# Airflow pins SQLAlchemy 1.4 while the ETL needs SQLAlchemy 2.0, so the two
# cannot share one Python. Airflow lives in the base image's Python; the ETL
# gets its own venv at /opt/etl/venv. The DAG's BashOperator runs that venv
# (ETL_PROJECT_DIR=/opt/etl -> /opt/etl/venv/bin/python -m jobs.X), keeping the
# two dependency sets cleanly separated inside a single image.
FROM apache/airflow:2.10.4-python3.12

# /opt/etl owned by the airflow user so it can build the venv there.
USER root
RUN mkdir -p /opt/etl && chown -R airflow: /opt/etl
USER airflow

# ETL deps in their own venv (cached layer — only re-runs when requirements change).
COPY --chown=airflow:0 requirements.txt /opt/etl/requirements.txt
RUN python -m venv /opt/etl/venv \
 && /opt/etl/venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/etl/venv/bin/pip install --no-cache-dir -r /opt/etl/requirements.txt

# ETL source (etl package + job entrypoints). DAGs are mounted at runtime, not copied.
COPY --chown=airflow:0 etl  /opt/etl/etl
COPY --chown=airflow:0 jobs /opt/etl/jobs
