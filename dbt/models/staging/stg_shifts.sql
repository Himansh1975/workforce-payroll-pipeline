-- One row per clock-in, already paired with its clock-out by the Spark job.
select
    shift_id,
    employee_id,
    site_id,
    cast(work_date as date) as work_date,
    shift_start,
    shift_end,
    hours_worked,
    is_complete
from read_parquet('{{ silver_path() }}/shifts/*/*.parquet', hive_partitioning = true)
