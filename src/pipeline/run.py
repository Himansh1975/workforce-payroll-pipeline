"""Run generate + both silver jobs for a range of dates in one Spark session.

Handy for a local backfill without Airflow:
    python -m pipeline.run --start 2026-11-10 --end 2026-11-14
Add --bigquery to also load each day's silver output into BigQuery.
Dates run in order, which the delta sync relies on.
"""
import argparse
from datetime import date, timedelta

from . import generate, silver_employees, silver_punches
from .common import get_spark

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--bigquery", action="store_true", help="also load silver into BigQuery")
    args = parser.parse_args()

    bq_client = None
    if args.bigquery:
        from . import load_bigquery  # only needs Google libraries when asked for
        bq_client = load_bigquery.get_client()

    spark = get_spark()
    day, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    while day <= end:
        d = day.isoformat()
        generate.run(d)
        silver_employees.run(spark, d)
        silver_punches.run(spark, d)
        if bq_client:
            load_bigquery.run(d, bq_client)
        day += timedelta(days=1)
    spark.stop()
