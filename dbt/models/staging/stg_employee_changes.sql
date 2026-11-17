-- The change feed written by the Spark delta sync: one row per employee per day it changed.
select
    employee_id,
    first_name,
    last_name,
    email,
    department,
    job_title,
    site_id,
    cast(hourly_rate as decimal(10, 2)) as hourly_rate,
    status,
    cast(hire_date as date)             as hire_date,
    change_type,
    row_hash,
    cast(load_date as date)             as load_date
from read_parquet('{{ silver_path() }}/employee_changes/*/*.parquet', hive_partitioning = true)
