-- Slowly changing dimension, type 2: one row per VERSION of an employee.
--
-- Each row in the change feed starts a new version. A version is valid from its
-- load_date until the next change for the same employee (LEAD), or forever if it
-- is the latest. valid_to is exclusive.
--
-- Built from the change feed with a window function rather than a dbt snapshot,
-- so the history depends only on the data, not on when dbt happened to run.
-- That keeps reruns and backfills deterministic.
{{ bq_layout(cluster_by=['employee_id']) }}

with versioned as (

    select
        *,
        lead(load_date) over (partition by employee_id order by load_date) as next_change_date
    from {{ ref('stg_employee_changes') }}

)

select
    {{ dbt.hash("employee_id || '|' || cast(load_date as " ~ dbt.type_string() ~ ")") }} as employee_sk,
    employee_id,
    first_name,
    last_name,
    email,
    department,
    job_title,
    site_id,
    hourly_rate,
    status,
    hire_date,
    load_date                                              as valid_from,
    coalesce(next_change_date, date '9999-12-31')          as valid_to,
    next_change_date is null                               as is_current
from versioned
