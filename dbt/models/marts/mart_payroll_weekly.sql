-- Payroll export: one row per employee per week (weeks start on Monday).
select
    employee_id,
    cast(date_trunc('week', work_date) as date) as week_start,
    count(*)                                    as shifts_worked,
    round(sum(regular_hours), 2)                as regular_hours,
    round(sum(overtime_hours), 2)               as overtime_hours,
    round(sum(gross_pay), 2)                    as gross_pay
from {{ ref('fct_shifts') }}
group by 1, 2
