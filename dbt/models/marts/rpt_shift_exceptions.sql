-- Shifts that could NOT be paid, and why. Nothing is dropped silently:
-- every shift is either in fct_shifts or listed here.
select
    s.shift_id,
    s.employee_id,
    s.site_id,
    s.work_date,
    s.shift_start,
    case
        when d.employee_id is null then 'unknown_employee'
        when not s.is_complete     then 'missing_clock_out'
    end as exception_reason
from {{ ref('stg_shifts') }} as s
left join {{ ref('dim_employee') }} as d
    on  s.employee_id = d.employee_id
    and s.work_date >= d.valid_from
    and s.work_date <  d.valid_to
where d.employee_id is null
   or not s.is_complete
