"""Unit tests for the transformation logic. Small hand-made inputs, no files."""
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from pipeline import silver_employees as emp
from pipeline import silver_punches as pun
from pipeline.common import enforce_reject_threshold, get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("tests")
    yield session
    session.stop()


def employee_rows(spark, rows):
    cols = ["employee_id", "first_name", "last_name", "email", "department",
            "job_title", "site_id", "hourly_rate", "status", "hire_date"]
    return spark.createDataFrame(rows, emp.RAW_SCHEMA).select(cols)


BASE = ("E00001", "Asha", "Rao", "asha.rao@example.com", "Nursing",
        "Associate", "HYD01", "250.00", "ACTIVE", "2023-04-01")


def hashed(spark, rows):
    return emp.add_row_hash(emp.clean(employee_rows(spark, rows)))


def test_formatting_noise_is_not_a_change(spark):
    noisy = (BASE[0], BASE[1], BASE[2], "ASHA.RAO@EXAMPLE.COM", "  Nursing ", *BASE[5:])
    assert hashed(spark, [BASE]).first()["row_hash"] == hashed(spark, [noisy]).first()["row_hash"]


def test_detect_changes_classifies_insert_update_and_skips_unchanged(spark):
    unchanged = BASE
    before_raise = ("E00002", *BASE[1:])
    after_raise = ("E00002", *BASE[1:7], "275.00", *BASE[8:])
    new_hire = ("E00003", *BASE[1:])

    previous = hashed(spark, [unchanged, before_raise]).select("employee_id", "row_hash")
    current = hashed(spark, [unchanged, after_raise, new_hire])

    result = {r["employee_id"]: r["change_type"]
              for r in emp.detect_changes(current, previous).collect()}
    assert result == {"E00002": "UPDATE", "E00003": "INSERT"}


def test_latest_state_picks_newest_hash(spark):
    changes = spark.createDataFrame(
        [("E00001", "old", "2026-11-10"), ("E00001", "new", "2026-11-12"), ("E00002", "only", "2026-11-11")],
        "employee_id string, row_hash string, load_date string",
    ).withColumn("load_date", F.to_date("load_date"))
    state = {r["employee_id"]: r["row_hash"] for r in emp.latest_state(changes).collect()}
    assert state == {"E00001": "new", "E00002": "only"}


def test_employee_reject_reasons(spark):
    rows = [
        BASE,
        ("", *BASE[1:]),
        ("E00004", *BASE[1:7], "-5.00", *BASE[8:]),
        ("E00005", *BASE[1:9], "31/31/2020"),
        ("E00006", *BASE[1:8], "RETIRED", BASE[9]),
    ]
    checked = emp.add_reject_reason(emp.clean(employee_rows(spark, rows)))
    reasons = sorted(r["reject_reason"] or "ok" for r in checked.collect())
    assert reasons == ["invalid_hire_date", "invalid_hourly_rate", "invalid_status",
                       "missing_or_malformed_employee_id", "ok"]


def test_pair_shifts_computes_hours_and_flags_missing_clock_out(spark):
    punches = spark.createDataFrame(
        [
            ("P1", "E00001", datetime(2026, 11, 10, 8, 0), "IN", "HYD01"),
            ("P2", "E00001", datetime(2026, 11, 10, 16, 30), "OUT", "HYD01"),
            ("P3", "E00002", datetime(2026, 11, 10, 9, 0), "IN", "HYD01"),  # never clocks out
        ],
        "punch_id string, employee_id string, punch_time timestamp, punch_type string, site_id string",
    )
    shifts = {r["shift_id"]: r for r in pun.pair_shifts(punches).collect()}
    assert shifts["P1"]["hours_worked"] == 8.5 and shifts["P1"]["is_complete"] is True
    assert shifts["P3"]["hours_worked"] is None and shifts["P3"]["is_complete"] is False
    assert set(shifts) == {"P1", "P3"}


def test_quality_gate_fails_on_too_many_rejects():
    enforce_reject_threshold("job", total=100, rejected=5)  # exactly at the limit: fine
    with pytest.raises(RuntimeError):
        enforce_reject_threshold("job", total=100, rejected=6)
