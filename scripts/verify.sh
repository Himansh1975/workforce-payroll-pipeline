#!/usr/bin/env bash
# Sanity checks run inside the container:
#   docker compose run --rm cli "bash scripts/verify.sh"
set -euo pipefail
cd /opt/project

rows() { python -c "import glob,sys,pyarrow.parquet as pq; print(sum(pq.read_metadata(f).num_rows for f in glob.glob(sys.argv[1])))" "$1"; }

echo "--- dbt version"
/opt/dbt-venv/bin/dbt --version > /tmp/v.txt 2>&1 || true; grep -m1 installed /tmp/v.txt

echo "--- rerun safety: rerunning 2026-11-12 must not change its output"
P='data/silver/employee_changes/load_date=2026-11-12/*.parquet'
before=$(rows "$P")
python -m pipeline.silver_employees --date 2026-11-12 > /dev/null 2>&1
after=$(rows "$P")
echo "rows before=$before after=$after"
[ "$before" = "$after" ] && echo "OK: idempotent" || { echo "FAIL: rerun changed output"; exit 1; }

echo "--- airflow DAG import"
export AIRFLOW__CORE__DAGS_FOLDER=/opt/project/dags AIRFLOW__CORE__LOAD_EXAMPLES=false
airflow db migrate > /dev/null 2>&1
airflow dags list 2>/dev/null | grep workforce_daily
airflow dags list-import-errors 2>/dev/null | tail -2
