"""Bronze -> silver for the HR employee feed, with a hash-based delta sync.

The HR system sends a FULL snapshot every day, but about 98% of rows are the same
as yesterday. Instead of reprocessing everything downstream, this job:

  1. cleans and validates the snapshot (rejects go to quarantine with a reason)
  2. fingerprints each row with a SHA-256 hash of its business columns
  3. compares the hash with the latest hash already stored for that employee
  4. writes ONLY new and changed rows to silver/employee_changes/load_date=<date>

That change feed is the input for the SCD2 employee dimension in dbt.

Idempotent by design: the "previous state" is rebuilt from partitions OLDER than
the run date, never from the run date itself. Rerunning a date therefore produces
the same output, and backfills are safe as long as dates run in order.
"""
import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .common import (enforce_reject_threshold, get_spark, partitions_before,
                     write_metrics, write_partition)
from .config import BRONZE, QUARANTINE, SILVER

RAW_SCHEMA = ("employee_id string, first_name string, last_name string, email string, "
              "department string, job_title string, site_id string, hourly_rate string, "
              "status string, hire_date string")

# Columns that define "this employee changed". load metadata is deliberately excluded.
TRACKED = ["first_name", "last_name", "email", "department", "job_title",
           "site_id", "hourly_rate", "status", "hire_date"]
VALID_STATUS = ["ACTIVE", "LEAVE", "TERMINATED"]
CHANGES = SILVER / "employee_changes"


def clean(raw: DataFrame) -> DataFrame:
    """Normalise formatting so cosmetic noise is not mistaken for a real change."""
    return (
        raw.select([F.trim(F.col(c)).alias(c) for c in raw.columns])
        .withColumn("employee_id", F.when(F.col("employee_id") != "", F.col("employee_id")))
        .withColumn("email", F.lower("email"))
        .withColumn("status", F.upper("status"))
        .withColumn("hourly_rate", F.col("hourly_rate").cast("decimal(10,2)"))
        .withColumn("hire_date", F.to_date("hire_date", "yyyy-MM-dd"))
    )


def add_reject_reason(df: DataFrame) -> DataFrame:
    """Tag each row with the first rule it breaks. Null means the row is valid."""
    return df.withColumn(
        "reject_reason",
        F.when(F.col("employee_id").isNull() | ~F.col("employee_id").rlike(r"^E\d{5}$"),
               "missing_or_malformed_employee_id")
        .when(F.col("hourly_rate").isNull() | (F.col("hourly_rate") <= 0), "invalid_hourly_rate")
        .when(F.col("hire_date").isNull(), "invalid_hire_date")
        .when(~F.col("status").isin(VALID_STATUS), "invalid_status"),
    )


def add_row_hash(df: DataFrame) -> DataFrame:
    """One fingerprint per row. coalesce() keeps nulls from collapsing the whole hash."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in TRACKED]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def latest_state(changes: DataFrame) -> DataFrame:
    """Most recent stored hash per employee, from the existing change feed."""
    newest_first = Window.partitionBy("employee_id").orderBy(F.col("load_date").desc())
    return (changes.withColumn("rn", F.row_number().over(newest_first))
            .filter("rn = 1").select("employee_id", "row_hash"))


def detect_changes(current: DataFrame, previous: DataFrame) -> DataFrame:
    """Keep only rows that are new (INSERT) or whose hash differs (UPDATE)."""
    prev = previous.select(F.col("employee_id").alias("_prev_id"),
                           F.col("row_hash").alias("_prev_hash"))
    return (
        current.join(prev, current["employee_id"] == prev["_prev_id"], "left")
        .withColumn("change_type",
                    F.when(F.col("_prev_id").isNull(), "INSERT")
                    .when(F.col("_prev_hash") != F.col("row_hash"), "UPDATE"))
        .filter(F.col("change_type").isNotNull())
        .drop("_prev_id", "_prev_hash")
    )


def run(spark: SparkSession, run_date: str) -> dict:
    raw = (spark.read.option("header", True).schema(RAW_SCHEMA)
           .csv(str(BRONZE / "hr_employees" / f"date={run_date}")))
    checked = add_reject_reason(clean(raw)).cache()

    rejected = checked.filter(F.col("reject_reason").isNotNull())
    valid = checked.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    deduped = valid.dropDuplicates(["employee_id"])
    current = add_row_hash(deduped)

    older = partitions_before(CHANGES, "load_date", run_date)
    if older:
        previous = latest_state(spark.read.option("basePath", str(CHANGES)).parquet(*older))
    else:
        previous = spark.createDataFrame([], "employee_id string, row_hash string")

    changes = detect_changes(current, previous).cache()

    write_partition(changes, CHANGES, "load_date", run_date)
    write_partition(rejected, QUARANTINE / "employees", "load_date", run_date)

    by_type = {r["change_type"]: r["count"] for r in changes.groupBy("change_type").count().collect()}
    total, n_rejected, n_valid, n_current = raw.count(), rejected.count(), valid.count(), current.count()
    n_changes = sum(by_type.values())
    metrics = {
        "rows_read": total,
        "rows_rejected": n_rejected,
        "duplicates_dropped": n_valid - n_current,
        "employees_in_snapshot": n_current,
        "inserts": by_type.get("INSERT", 0),
        "updates": by_type.get("UPDATE", 0),
        "unchanged_skipped": n_current - n_changes,
        "pct_skipped_by_delta": round(100 * (n_current - n_changes) / n_current, 1) if n_current else 0.0,
    }
    write_metrics("silver_employees", run_date, metrics)
    checked.unpersist()
    changes.unpersist()
    enforce_reject_threshold("silver_employees", total, n_rejected)
    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    run(get_spark("silver_employees"), parser.parse_args().date)
