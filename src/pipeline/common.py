"""Shared helpers: Spark session, partition IO, run metrics and the data quality gate."""
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .config import MAX_REJECT_RATIO, METRICS


def get_spark(app: str = "workforce-pipeline") -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName(app)
        .config("spark.sql.shuffle.partitions", "4")  # small data: 200 partitions would be waste
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")  # bad dates become null, not errors
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def write_partition(df: DataFrame, base: Path, key: str, value: str) -> None:
    """Overwrite exactly one partition folder (base/key=value).

    Overwriting the whole partition is what makes a rerun safe: running the same
    date twice leaves the same files, never duplicates.
    """
    df.coalesce(1).write.mode("overwrite").parquet(str(base / f"{key}={value}"))


def partitions_before(base: Path, key: str, run_date: str) -> list[str]:
    """Paths of the partitions strictly older than run_date (ISO dates sort as text)."""
    if not base.is_dir():
        return []
    prefix = f"{key}="
    return sorted(
        str(p) for p in base.iterdir()
        if p.is_dir() and p.name.startswith(prefix) and p.name[len(prefix):] < run_date
    )


def write_metrics(job: str, run_date: str, metrics: dict) -> None:
    out = METRICS / job
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{run_date}.json").write_text(json.dumps(metrics, indent=2))
    print(f"[{job}] {run_date} {json.dumps(metrics)}")


def enforce_reject_threshold(job: str, total: int, rejected: int) -> None:
    """Fail the job when too many rows were rejected, so bad data never flows downstream."""
    ratio = rejected / total if total else 0.0
    if ratio > MAX_REJECT_RATIO:
        raise RuntimeError(
            f"{job}: {rejected} of {total} rows rejected ({ratio:.1%}), "
            f"above the {MAX_REJECT_RATIO:.0%} limit. Check the source file."
        )
