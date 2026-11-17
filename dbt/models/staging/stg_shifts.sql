-- One row per clock-in, already paired with its clock-out by the Spark job.
select
    shift_id,
    employee_id,
    site_id,
    cast(work_date as date) as work_date,
    shift_start,
    shift_end,
    -- Exact decimal, not float: hours feed pay calculations, and float maths
    -- rounds differently across engines (DuckDB vs BigQuery differed by 23 paise).
    cast(hours_worked as {{ money_type() }}) as hours_worked,
    is_complete
{% if target.type == 'bigquery' %}
from {{ source('raw', 'shifts') }}
{% else %}
from read_parquet('{{ silver_path() }}/shifts/*/*.parquet', hive_partitioning = true)
{% endif %}
