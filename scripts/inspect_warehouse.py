"""Print a few facts from the warehouse. Run with the dbt venv's Python:
    docker compose run --rm cli "/opt/dbt-venv/bin/python scripts/inspect_warehouse.py"
"""
import duckdb

con = duckdb.connect("warehouse/workforce.duckdb", read_only=True)


def show(title, sql):
    print(f"\n--- {title}")
    rel = con.sql(sql)
    print(" | ".join(rel.columns))
    for row in rel.fetchall():
        print(" | ".join(str(v) for v in row))


show("row counts", """
    select 'dim_employee' t, count(*) n from dim_employee
    union all select 'dim_employee (current)', count(*) from dim_employee where is_current
    union all select 'fct_shifts', count(*) from fct_shifts
    union all select 'mart_payroll_weekly', count(*) from mart_payroll_weekly
    union all select 'rpt_shift_exceptions', count(*) from rpt_shift_exceptions
""")
show("exceptions by reason", "select exception_reason, count(*) n from rpt_shift_exceptions group by 1 order by 1")
show("one employee's history (SCD2)", """
    select employee_id, department, job_title, hourly_rate, status, valid_from, valid_to, is_current
    from dim_employee
    where employee_id = (select employee_id from dim_employee group by 1 having count(*) > 1 order by 1 limit 1)
    order by valid_from
""")
show("payroll sample", "select * from mart_payroll_weekly order by gross_pay desc limit 3")
