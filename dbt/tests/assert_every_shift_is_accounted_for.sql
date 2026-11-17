-- Reconciliation: shifts in = shifts paid + shifts listed as exceptions.
-- Returns a row (and fails) only if a shift went missing or was counted twice.
with counts as (
    select
        (select count(*) from {{ ref('stg_shifts') }})           as shifts_in,
        (select count(*) from {{ ref('fct_shifts') }})           as shifts_paid,
        (select count(*) from {{ ref('rpt_shift_exceptions') }}) as shifts_excepted
)
select * from counts
where shifts_in <> shifts_paid + shifts_excepted
