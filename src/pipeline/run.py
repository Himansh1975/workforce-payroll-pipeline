"""Run generate + both silver jobs for a range of dates in one Spark session.

Handy for a local backfill without Airflow:
    python -m pipeline.run --start 2026-11-10 --end 2026-11-14
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
    args = parser.parse_args()

    spark = get_spark()
    day, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    while day <= end:
        d = day.isoformat()
        generate.run(d)
        silver_employees.run(spark, d)
        silver_punches.run(spark, d)
        day += timedelta(days=1)
    spark.stop()
