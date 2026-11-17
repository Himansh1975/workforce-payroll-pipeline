-- One row per completed shift, priced at the rate the employee had ON THAT DAY.
--
-- The join to dim_employee is a point-in-time join: it picks the version whose
-- validity window contains the work date. A raise next week must not change what
-- last week's shift cost.
--
-- Overtime: hours beyond 8 in a shift are paid at 1.5x.
select
    s.shift_id,
    d.employee_sk,
    s.employee_id,
    s.site_id,
    d.department,
    s.work_date,
    s.shift_start,
    s.shift_end,
    s.hours_worked,
    least(s.hours_worked, 8)                                      as regular_hours,
    greatest(s.hours_worked - 8, 0)                               as overtime_hours,
    d.hourly_rate,
    round(
        least(s.hours_worked, 8) * d.hourly_rate
        + greatest(s.hours_worked - 8, 0) * d.hourly_rate * 1.5,
        2
    )                                                             as gross_pay
from {{ ref('stg_shifts') }} as s
inner join {{ ref('dim_employee') }} as d
    on  s.employee_id = d.employee_id
    and s.work_date >= d.valid_from
    and s.work_date <  d.valid_to
where s.is_complete
