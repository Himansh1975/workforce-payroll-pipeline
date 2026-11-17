"""Daily workforce pipeline.

    generate_source_files -> silver_employees -> silver_punches -> dbt_build

- catchup=True: Airflow backfills every day since start_date, one run per day.
- max_active_runs=1 + depends_on_past=True: days run strictly in order, because
  the delta sync compares each day against the days before it.
- Every task takes the run's logical date ({{ ds }}), so a rerun of an old day
  reprocesses that day's data, not today's.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project"

with DAG(
    dag_id="workforce_daily",
    start_date=datetime(2026, 11, 10),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={"depends_on_past": True, "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["workforce", "pyspark", "dbt"],
) as dag:

    generate = BashOperator(
        task_id="generate_source_files",
        bash_command=f"cd {PROJECT} && python -m pipeline.generate --date {{{{ ds }}}}",
    )
    employees = BashOperator(
        task_id="silver_employees",
        bash_command=f"cd {PROJECT} && python -m pipeline.silver_employees --date {{{{ ds }}}}",
    )
    punches = BashOperator(
        task_id="silver_punches",
        bash_command=f"cd {PROJECT} && python -m pipeline.silver_punches --date {{{{ ds }}}}",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROJECT}/dbt && /opt/dbt-venv/bin/dbt build --profiles-dir .",
    )

    generate >> employees >> punches >> dbt_build
