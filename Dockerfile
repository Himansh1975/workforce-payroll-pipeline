FROM apache/airflow:2.10.5-python3.11

USER root
# Spark needs a JVM.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
# dbt gets its own virtualenv so its dependencies cannot clash with Airflow's.
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-duckdb~=1.9.0" \
    && chown -R airflow /opt/dbt-venv

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.3 pytest

ENV PROJECT_ROOT=/opt/project \
    PYTHONPATH=/opt/project/src
WORKDIR /opt/project
