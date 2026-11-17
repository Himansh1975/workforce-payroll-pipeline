"""Check the BigQuery warehouse: table layout, partition pruning, and totals.

    docker compose run --rm cli "python scripts/inspect_bigquery.py"
"""
from google.cloud import bigquery

from pipeline.load_bigquery import get_client

client = get_client()
P = client.project

print("--- table layout")
for name in ["workforce_raw.employee_changes", "workforce_raw.shifts", "workforce.dim_employee", "workforce.fct_shifts"]:
    t = client.get_table(f"{P}.{name}")
    part = t.time_partitioning.field if t.time_partitioning else None
    print(f"{name:34} rows={t.num_rows:>5}  partitioned_by={part}  clustered_by={t.clustering_fields}")


def bytes_scanned(sql: str) -> int:
    job = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    return job.total_bytes_processed


print("\n--- partition pruning (dry runs, nothing billed)")
full = bytes_scanned(f"select sum(gross_pay) from `{P}.workforce.fct_shifts`")
one_day = bytes_scanned(f"select sum(gross_pay) from `{P}.workforce.fct_shifts` where work_date = '2026-11-12'")
print(f"whole table: {full:,} bytes | one day: {one_day:,} bytes | scanned {one_day / full:.0%} of the table")

print("\n--- totals")
row = list(client.query(f"""
    select count(*) as shifts, round(sum(gross_pay), 2) as gross_pay
    from `{P}.workforce.fct_shifts`
""").result())[0]
print(f"shifts={row.shifts} gross_pay={row.gross_pay}")
