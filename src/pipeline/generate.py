"""Synthetic source systems.

Stands in for an HR system (daily FULL snapshot of employees, CSV) and a timeclock
system (punch events, JSON lines). All data is made up; no real people or clients.

The output is deliberately messy, the way real feeds are:
  - employees: blank ids, negative pay rates, malformed dates, exact duplicate rows,
    stray whitespace and upper-cased emails
  - punches: duplicate events, missing clock-outs, null ids, bad timestamps,
    unknown punch types, punches for employees that do not exist

Everything is seeded by date, so the same date always produces the same files.
"""
import argparse
import csv
import json
import random
from datetime import date, datetime, timedelta

from .config import BRONZE

EPOCH = date(2026, 11, 1)  # day the simulated company "starts"; no changes before it
FIELDS = ["employee_id", "first_name", "last_name", "email", "department",
          "job_title", "site_id", "hourly_rate", "status", "hire_date"]

FIRST = ["Aarav", "Diya", "Kabir", "Meera", "Rohan", "Isha", "Vikram", "Ananya", "Arjun", "Sara",
         "Dev", "Nisha", "Karan", "Pooja", "Rahul", "Tara", "Imran", "Leela", "Sameer", "Zoya"]
LAST = ["Sharma", "Reddy", "Iyer", "Khan", "Patel", "Nair", "Singh", "Das", "Mehta", "Rao",
        "Bose", "Joshi", "Kapoor", "Menon", "Verma", "Gill"]
DEPTS = ["Nursing", "Pharmacy", "Radiology", "Facilities", "Warehouse", "Packaging", "Quality", "Finance"]
TITLES = ["Associate", "Senior Associate", "Technician", "Supervisor", "Coordinator"]
SITES = ["HYD01", "BLR01", "PUN01", "CHN01"]


def _hire(employees: dict, rng: random.Random, hire_date: date) -> None:
    n = len(employees) + 1
    first, last = rng.choice(FIRST), rng.choice(LAST)
    employees[f"E{n:05d}"] = {
        "employee_id": f"E{n:05d}",
        "first_name": first,
        "last_name": last,
        "email": f"{first}.{last}{n}@example.com".lower(),
        "department": rng.choice(DEPTS),
        "job_title": rng.choice(TITLES),
        "site_id": rng.choice(SITES),
        "hourly_rate": round(rng.uniform(180, 950), 2),
        "status": "ACTIVE",
        "hire_date": hire_date.isoformat(),
    }


def simulate_employees(run_date: date) -> dict:
    """Replay the company's history from EPOCH up to run_date.

    Each day about 2% of active employees change (raise, transfer, promotion,
    leave, termination) and a few people are hired. Most rows do NOT change from
    one day to the next, which is exactly why a delta sync pays off.
    """
    if run_date < EPOCH:
        raise ValueError(f"run_date must be on or after {EPOCH}")
    employees: dict = {}
    seed_rng = random.Random(42)
    for _ in range(500):
        _hire(employees, seed_rng, EPOCH - timedelta(days=seed_rng.randint(30, 2000)))

    day = EPOCH + timedelta(days=1)
    while day <= run_date:
        rng = random.Random(f"employees-{day}")
        active = [e for e in employees.values() if e["status"] == "ACTIVE"]
        for emp in rng.sample(active, k=max(1, int(len(active) * 0.02))):
            change = rng.choices(["raise", "transfer", "promotion", "leave", "terminate"],
                                 weights=[40, 25, 20, 5, 10])[0]
            if change == "raise":
                emp["hourly_rate"] = round(emp["hourly_rate"] * rng.uniform(1.02, 1.10), 2)
            elif change == "transfer":
                emp["department"] = rng.choice([d for d in DEPTS if d != emp["department"]])
            elif change == "promotion":
                emp["job_title"] = rng.choice([t for t in TITLES if t != emp["job_title"]])
            elif change == "leave":
                emp["status"] = "LEAVE"
            else:
                emp["status"] = "TERMINATED"
        for _ in range(rng.randint(0, 3)):
            _hire(employees, rng, day)
        day += timedelta(days=1)
    return employees


def write_employee_snapshot(run_date: date, employees: dict) -> int:
    rng = random.Random(f"employee-noise-{run_date}")
    rows = [dict(e, hourly_rate=f"{e['hourly_rate']:.2f}") for e in employees.values()]

    # Formatting noise that is NOT a real change. The silver job must normalise it
    # before hashing, or every noisy row would look like an update.
    for row in rng.sample(rows, k=len(rows) // 20):
        row["department"] = f"  {row['department']} "
        row["email"] = row["email"].upper()

    # Rows that must be rejected.
    bad = [dict(rng.choice(rows), employee_id="") for _ in range(2)]
    bad += [dict(rng.choice(rows), employee_id=f"E8{i:04d}", hourly_rate="-5.00") for i in range(2)]
    bad += [dict(rng.choice(rows), employee_id=f"E9{i:04d}", hire_date="31/31/2020") for i in range(2)]
    duplicates = [dict(r) for r in rng.sample(rows, k=3)]  # exact repeats of good rows

    out_rows = rows + bad + duplicates
    rng.shuffle(out_rows)
    out = BRONZE / "hr_employees" / f"date={run_date}"
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "employees.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(out_rows)
    return len(out_rows)


def write_punches(run_date: date, employees: dict) -> int:
    rng = random.Random(f"punches-{run_date}")
    start_of_day = datetime.combine(run_date, datetime.min.time())
    events, n = [], 0

    def event(emp_id, ts, kind, site):
        nonlocal n
        n += 1
        return {"punch_id": f"P{run_date:%Y%m%d}-{n:06d}", "employee_id": emp_id,
                "punch_ts": ts.isoformat(timespec="seconds") if isinstance(ts, datetime) else ts,
                "punch_type": kind, "site_id": site}

    for emp in employees.values():
        if emp["status"] != "ACTIVE" or emp["hire_date"] > run_date.isoformat():
            continue
        if rng.random() > 0.7:  # not everyone works every day
            continue
        clock_in = start_of_day + timedelta(hours=6, minutes=rng.randint(0, 240))
        clock_out = clock_in + timedelta(hours=rng.uniform(6.5, 10.5))
        punch_in = event(emp["employee_id"], clock_in, "IN", emp["site_id"])
        events.append(punch_in)
        if rng.random() < 0.01:
            events.append(dict(punch_in))  # device sent the same event twice
        if rng.random() > 0.01:            # 1% forget to clock out
            events.append(event(emp["employee_id"], clock_out, "OUT", emp["site_id"]))

    noon = start_of_day + timedelta(hours=12)
    events += [event(None, noon, "IN", "HYD01") for _ in range(3)]
    events += [event("E00001", "not-a-time", "IN", "HYD01") for _ in range(2)]
    events += [event("E00002", noon, "BREAK", "HYD01") for _ in range(2)]
    for _ in range(3):  # well-formed, but the employee does not exist: caught in the warehouse
        events.append(event("E99999", noon, "IN", "HYD01"))
        events.append(event("E99999", noon + timedelta(hours=8), "OUT", "HYD01"))

    rng.shuffle(events)
    out = BRONZE / "punches" / f"date={run_date}"
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "punches.jsonl", "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    return len(events)


def run(run_date: str) -> dict:
    d = date.fromisoformat(run_date)
    employees = simulate_employees(d)
    counts = {"employee_rows": write_employee_snapshot(d, employees),
              "punch_events": write_punches(d, employees)}
    print(f"[generate] {run_date} {json.dumps(counts)}")
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    run(parser.parse_args().date)
