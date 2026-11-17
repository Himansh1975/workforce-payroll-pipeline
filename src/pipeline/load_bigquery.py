"""Silver parquet -> BigQuery raw tables, one date partition at a time.

Each run REPLACES exactly one partition (table$YYYYMMDD with WRITE_TRUNCATE),
the same idempotency rule as the Spark jobs: loading a date twice leaves the
same rows, never duplicates.

The partition column (load_date / work_date) lives in the folder name, not in
the parquet files, so it is added here before upload.

Works in the BigQuery free sandbox: load jobs and table creation are allowed
there; DML (INSERT/UPDATE/MERGE) is not, and this loader uses none.

Needs:
    GCP_PROJECT   your Google Cloud project id
    GCP_KEYFILE   path to a service-account JSON key (default: secrets/gcp-key.json)
    BQ_LOCATION   dataset location (default: US)
"""
import argparse
import io
import os
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery
from google.oauth2 import service_account

from .config import ROOT, SILVER

RAW_DATASET = "workforce_raw"

# table name -> (silver folder, partition column)
TABLES = {
    "employee_changes": ("employee_changes", "load_date"),
    "shifts": ("shifts", "work_date"),
}


def get_client() -> bigquery.Client:
    project = os.environ.get("GCP_PROJECT")
    if not project:
        raise SystemExit("Set GCP_PROJECT (see .env.example).")
    keyfile = os.environ.get("GCP_KEYFILE", str(ROOT / "secrets" / "gcp-key.json"))
    creds = service_account.Credentials.from_service_account_file(keyfile)
    return bigquery.Client(project=project, credentials=creds,
                           location=os.environ.get("BQ_LOCATION", "US"))


def ensure_dataset(client: bigquery.Client) -> None:
    ds = bigquery.Dataset(f"{client.project}.{RAW_DATASET}")
    ds.location = client.location
    client.create_dataset(ds, exists_ok=True)


def read_partition(folder: str, column: str, run_date: str) -> pa.Table | None:
    path = SILVER / folder / f"{column}={run_date}"
    files = sorted(path.glob("*.parquet"))
    if not files:
        return None
    table = pa.concat_tables(pq.read_table(f) for f in files)
    day = date.fromisoformat(run_date)
    return table.append_column(column, pa.array([day] * table.num_rows, type=pa.date32()))


def load_partition(client: bigquery.Client, name: str, folder: str, column: str, run_date: str) -> int:
    table = read_partition(folder, column, run_date)
    if table is None:
        print(f"[load_bigquery] {name} {run_date}: no silver data, skipped")
        return 0

    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    destination = f"{client.project}.{RAW_DATASET}.{name}${run_date.replace('-', '')}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # replace this partition only
        time_partitioning=bigquery.TimePartitioning(field=column),
        clustering_fields=["employee_id"],
    )
    client.load_table_from_file(buf, destination, job_config=job_config).result()
    print(f"[load_bigquery] {name} {run_date}: {table.num_rows} rows -> {destination}")
    return table.num_rows


def run(run_date: str, client: bigquery.Client | None = None) -> dict:
    client = client or get_client()
    ensure_dataset(client)
    return {name: load_partition(client, name, folder, column, run_date)
            for name, (folder, column) in TABLES.items()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    run(parser.parse_args().date)
