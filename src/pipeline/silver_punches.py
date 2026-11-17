"""Bronze -> silver for timeclock punches: validate, deduplicate, pair IN/OUT into shifts.

Raw input is one event per clock-in or clock-out. Payroll needs one row per SHIFT
with hours worked, so this job pairs each IN with the employee's next event using a
window function. A clock-in with no matching clock-out is kept and flagged
(is_complete = false) so payroll can chase it instead of silently losing the hours.
"""
import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .common import enforce_reject_threshold, get_spark, write_metrics, write_partition
from .config import BRONZE, QUARANTINE, SILVER

RAW_SCHEMA = "punch_id string, employee_id string, punch_ts string, punch_type string, site_id string"


def clean(raw: DataFrame) -> DataFrame:
    return (
        raw.select([F.trim(F.col(c)).alias(c) for c in raw.columns])
        .withColumn("punch_type", F.upper("punch_type"))
        .withColumn("punch_time", F.to_timestamp("punch_ts"))  # unparseable -> null
    )


def add_reject_reason(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "reject_reason",
        F.when(F.col("punch_id").isNull(), "missing_punch_id")
        .when(F.col("employee_id").isNull() | ~F.col("employee_id").rlike(r"^E\d{5}$"),
              "missing_or_malformed_employee_id")
        .when(F.col("punch_time").isNull(), "invalid_timestamp")
        .when(~F.col("punch_type").isin("IN", "OUT"), "invalid_punch_type"),
    )


def pair_shifts(punches: DataFrame) -> DataFrame:
    """One row per clock-in, matched to the same employee's next event if it is an OUT."""
    in_order = Window.partitionBy("employee_id").orderBy("punch_time")
    seconds = F.col("next_time").cast("long") - F.col("punch_time").cast("long")
    return (
        punches.withColumn("next_type", F.lead("punch_type").over(in_order))
        .withColumn("next_time", F.lead("punch_time").over(in_order))
        .filter(F.col("punch_type") == "IN")
        .withColumn("is_complete", F.coalesce(F.col("next_type") == "OUT", F.lit(False)))
        .select(
            F.col("punch_id").alias("shift_id"),
            "employee_id",
            "site_id",
            F.to_date("punch_time").alias("work_date"),
            F.col("punch_time").cast("timestamp_ntz").alias("shift_start"),
            F.when(F.col("is_complete"), F.col("next_time")).cast("timestamp_ntz").alias("shift_end"),
            F.when(F.col("is_complete"), F.round(seconds / 3600, 2)).alias("hours_worked"),
            "is_complete",
        )
    )


def run(spark: SparkSession, run_date: str) -> dict:
    raw = spark.read.schema(RAW_SCHEMA).json(str(BRONZE / "punches" / f"date={run_date}"))
    checked = add_reject_reason(clean(raw)).cache()

    rejected = checked.filter(F.col("reject_reason").isNotNull())
    valid = checked.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    deduped = valid.dropDuplicates(["punch_id"])
    shifts = pair_shifts(deduped).cache()

    write_partition(shifts.drop("work_date"), SILVER / "shifts", "work_date", run_date)
    write_partition(rejected, QUARANTINE / "punches", "load_date", run_date)

    total, n_rejected, n_valid, n_deduped = raw.count(), rejected.count(), valid.count(), deduped.count()
    n_shifts = shifts.count()
    n_complete = shifts.filter("is_complete").count()
    metrics = {
        "events_read": total,
        "events_rejected": n_rejected,
        "duplicates_dropped": n_valid - n_deduped,
        "shifts": n_shifts,
        "shifts_missing_clock_out": n_shifts - n_complete,
    }
    write_metrics("silver_punches", run_date, metrics)
    checked.unpersist()
    shifts.unpersist()
    enforce_reject_threshold("silver_punches", total, n_rejected)
    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    run(get_spark("silver_punches"), parser.parse_args().date)
