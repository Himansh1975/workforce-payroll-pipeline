"""Folder layout for the pipeline. Everything hangs off PROJECT_ROOT."""
import os
from pathlib import Path

ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
DATA = ROOT / "data"

BRONZE = DATA / "bronze"          # raw files exactly as the source systems sent them
SILVER = DATA / "silver"          # cleaned, validated, deduplicated parquet
QUARANTINE = DATA / "quarantine"  # rejected rows with the reason they were rejected
METRICS = DATA / "run_metrics"    # one JSON per job per day: row counts and reject counts

# A run fails if more than this share of input rows is rejected.
# A few bad rows are normal; a lot of them means the source file is broken.
MAX_REJECT_RATIO = 0.05
