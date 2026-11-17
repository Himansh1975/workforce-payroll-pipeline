-- SCD2 integrity: every employee has exactly one current version.
-- A dbt test passes when the query returns no rows.
select employee_id, count(*) as current_versions
from {{ ref('dim_employee') }}
where is_current
group by employee_id
having count(*) <> 1
