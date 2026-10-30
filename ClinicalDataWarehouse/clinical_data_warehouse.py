"""
Clinical Data Warehouse - simple base project (Python + SQLite, no extra installs).

Pipeline:  Source CSV  ->  Extract  ->  Transform/Validate  ->  Load (star schema)  ->  Analytics

Star schema:
    dim_patient    (patient_key, patient_id, name, gender, birth_year)
    dim_doctor     (doctor_key, doctor_name, department)
    dim_diagnosis  (diagnosis_key, icd_code, diagnosis_name)
    dim_date       (date_key, full_date, year, quarter, month, month_name, day_of_week)
    fact_visit     (visit_id, patient_key, doctor_key, diagnosis_key, date_key,
                    age_at_visit, length_of_stay_days, cost, readmitted)

Run:  python clinical_data_warehouse.py
"""
import csv
import os
import random
import sqlite3
from datetime import date, datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_CSV = os.path.join(BASE_DIR, "source_visits.csv")
REJECT_CSV = os.path.join(BASE_DIR, "rejected_rows.csv")
DB_PATH = os.path.join(BASE_DIR, "clinical_dw.db")

DIAGNOSES = [
    ("I10", "Hypertension"), ("E11", "Type 2 Diabetes"), ("J45", "Asthma"),
    ("I21", "Myocardial Infarction"), ("J18", "Pneumonia"), ("M54", "Back Pain"),
    ("N39", "Urinary Tract Infection"),
]
DOCTORS = [
    ("Dr. Sharma", "Cardiology"), ("Dr. Reddy", "Orthopedics"),
    ("Dr. Iyer", "General Medicine"), ("Dr. Khan", "Pulmonology"),
    ("Dr. Rao", "Endocrinology"),
]


# --------------------------------------------------------------------------
# 0. Create a messy sample source file (stands in for hospital system export)
# --------------------------------------------------------------------------
def generate_source_data(n_patients=60, n_visits=400, seed=42):
    rng = random.Random(seed)
    first = ["Asha", "Ravi", "Meena", "Kiran", "Sara", "Arjun", "Divya", "Imran", "Neha", "Vikram"]
    last = ["Kumar", "Singh", "Patel", "Reddy", "Sharma", "Khan", "Nair", "Das"]
    patients = []
    for i in range(1, n_patients + 1):
        patients.append((f"P{i:04d}", f"{rng.choice(first)} {rng.choice(last)}",
                         rng.choice(["M", "F"]), rng.randint(1935, 2018)))

    rows = []
    start = date(2025, 1, 1)
    for v in range(1, n_visits + 1):
        pid, name, gender, byear = rng.choice(patients)
        icd, dx = rng.choice(DIAGNOSES)
        doc, dept = rng.choice(DOCTORS)
        visit_date = start + timedelta(days=rng.randint(0, 600))
        rows.append({
            "visit_id": f"V{v:05d}", "patient_id": pid, "patient_name": name,
            "gender": gender, "birth_year": byear, "visit_date": visit_date.isoformat(),
            "icd_code": icd, "diagnosis": dx, "doctor": doc, "department": dept,
            "length_of_stay": rng.randint(0, 12), "cost": round(rng.uniform(500, 25000), 2),
            "readmitted_30d": rng.choice(["Y", "N", "N", "N"]),
        })

    # inject dirty data so the transform step has real work to do
    rows[10]["visit_date"] = "31/02/2025"         # invalid date
    rows[20]["cost"] = -150                       # negative cost
    rows[30]["birth_year"] = 1850                 # impossible age
    rows[40]["patient_name"] = "  asha   KUMAR "  # messy text
    rows[50]["gender"] = "female"                 # non-standard code
    rows[60]["patient_id"] = ""                   # missing key
    rows.append(dict(rows[5]))                    # duplicate visit
    rows.append(dict(rows[6]))                    # duplicate visit

    with open(SOURCE_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"[source] wrote {len(rows)} raw rows -> {os.path.basename(SOURCE_CSV)}")


# --------------------------------------------------------------------------
# 1. Extract
# --------------------------------------------------------------------------
def extract(path):
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    print(f"[extract] read {len(rows)} rows")
    return rows


# --------------------------------------------------------------------------
# 2. Transform + validate
# --------------------------------------------------------------------------
def clean_gender(g):
    g = (g or "").strip().upper()
    if g in ("M", "MALE"):
        return "M"
    if g in ("F", "FEMALE"):
        return "F"
    return "U"


def transform(raw_rows):
    clean, rejected, seen = [], [], set()
    for r in raw_rows:
        try:
            visit_id = r["visit_id"].strip()
            patient_id = r["patient_id"].strip()
            if not visit_id or not patient_id:
                raise ValueError("missing visit_id/patient_id")
            if visit_id in seen:
                raise ValueError("duplicate visit_id")

            visit_date = datetime.strptime(r["visit_date"].strip(), "%Y-%m-%d").date()
            birth_year = int(r["birth_year"])
            age = visit_date.year - birth_year
            if not 0 <= age <= 110:
                raise ValueError(f"implausible age {age}")
            cost = float(r["cost"])
            if cost < 0:
                raise ValueError("negative cost")
            los = int(r["length_of_stay"])
            if los < 0:
                raise ValueError("negative length of stay")

            seen.add(visit_id)
            clean.append({
                "visit_id": visit_id,
                "patient_id": patient_id,
                "name": " ".join(r["patient_name"].split()).title(),
                "gender": clean_gender(r["gender"]),
                "birth_year": birth_year,
                "visit_date": visit_date,
                "age": age,
                "icd_code": r["icd_code"].strip().upper(),
                "diagnosis": r["diagnosis"].strip().title(),
                "doctor": r["doctor"].strip(),
                "department": r["department"].strip().title(),
                "los": los,
                "cost": cost,
                "readmitted": 1 if r["readmitted_30d"].strip().upper() == "Y" else 0,
            })
        except (ValueError, KeyError) as e:
            rejected.append({**r, "reject_reason": str(e)})

    if rejected:
        with open(REJECT_CSV, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=rejected[0].keys())
            w.writeheader()
            w.writerows(rejected)
    print(f"[transform] {len(clean)} clean, {len(rejected)} rejected -> {os.path.basename(REJECT_CSV)}")
    return clean, rejected


# --------------------------------------------------------------------------
# 3. Load into star schema
# --------------------------------------------------------------------------
SCHEMA = """
DROP TABLE IF EXISTS fact_visit;
DROP TABLE IF EXISTS dim_patient;
DROP TABLE IF EXISTS dim_doctor;
DROP TABLE IF EXISTS dim_diagnosis;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_patient (
    patient_key INTEGER PRIMARY KEY AUTOINCREMENT,
    patient_id  TEXT UNIQUE NOT NULL,
    name        TEXT, gender TEXT, birth_year INTEGER
);
CREATE TABLE dim_doctor (
    doctor_key  INTEGER PRIMARY KEY AUTOINCREMENT,
    doctor_name TEXT NOT NULL, department TEXT NOT NULL,
    UNIQUE (doctor_name, department)
);
CREATE TABLE dim_diagnosis (
    diagnosis_key  INTEGER PRIMARY KEY AUTOINCREMENT,
    icd_code       TEXT UNIQUE NOT NULL, diagnosis_name TEXT
);
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,          -- yyyymmdd
    full_date TEXT, year INTEGER, quarter INTEGER,
    month INTEGER, month_name TEXT, day_of_week TEXT
);
CREATE TABLE fact_visit (
    visit_id TEXT PRIMARY KEY,
    patient_key   INTEGER NOT NULL REFERENCES dim_patient(patient_key),
    doctor_key    INTEGER NOT NULL REFERENCES dim_doctor(doctor_key),
    diagnosis_key INTEGER NOT NULL REFERENCES dim_diagnosis(diagnosis_key),
    date_key      INTEGER NOT NULL REFERENCES dim_date(date_key),
    age_at_visit INTEGER, length_of_stay_days INTEGER,
    cost REAL, readmitted INTEGER
);
CREATE INDEX ix_fact_date ON fact_visit(date_key);
CREATE INDEX ix_fact_diag ON fact_visit(diagnosis_key);
"""


def load(conn, rows):
    conn.executescript(SCHEMA)
    cur = conn.cursor()
    for r in rows:
        cur.execute("INSERT OR IGNORE INTO dim_patient (patient_id, name, gender, birth_year) VALUES (?,?,?,?)",
                    (r["patient_id"], r["name"], r["gender"], r["birth_year"]))
        cur.execute("INSERT OR IGNORE INTO dim_doctor (doctor_name, department) VALUES (?,?)",
                    (r["doctor"], r["department"]))
        cur.execute("INSERT OR IGNORE INTO dim_diagnosis (icd_code, diagnosis_name) VALUES (?,?)",
                    (r["icd_code"], r["diagnosis"]))
        d = r["visit_date"]
        date_key = int(d.strftime("%Y%m%d"))
        cur.execute("INSERT OR IGNORE INTO dim_date VALUES (?,?,?,?,?,?,?)",
                    (date_key, d.isoformat(), d.year, (d.month - 1) // 3 + 1,
                     d.month, d.strftime("%B"), d.strftime("%A")))

        pk = cur.execute("SELECT patient_key FROM dim_patient WHERE patient_id=?", (r["patient_id"],)).fetchone()[0]
        dk = cur.execute("SELECT doctor_key FROM dim_doctor WHERE doctor_name=? AND department=?",
                         (r["doctor"], r["department"])).fetchone()[0]
        gk = cur.execute("SELECT diagnosis_key FROM dim_diagnosis WHERE icd_code=?", (r["icd_code"],)).fetchone()[0]
        cur.execute("INSERT INTO fact_visit VALUES (?,?,?,?,?,?,?,?,?)",
                    (r["visit_id"], pk, dk, gk, date_key, r["age"], r["los"], r["cost"], r["readmitted"]))
    conn.commit()
    counts = {t: cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
              for t in ("dim_patient", "dim_doctor", "dim_diagnosis", "dim_date", "fact_visit")}
    print("[load] row counts:", counts)


# --------------------------------------------------------------------------
# 4. Analytics
# --------------------------------------------------------------------------
REPORTS = {
    "Visits & revenue by department": """
        SELECT d.department, COUNT(*) AS visits, ROUND(SUM(f.cost),2) AS total_cost,
               ROUND(AVG(f.length_of_stay_days),1) AS avg_stay_days
        FROM fact_visit f JOIN dim_doctor d USING (doctor_key)
        GROUP BY d.department ORDER BY visits DESC""",
    "Top diagnoses (with avg cost & readmission rate %)": """
        SELECT g.diagnosis_name, COUNT(*) AS visits, ROUND(AVG(f.cost),2) AS avg_cost,
               ROUND(100.0*SUM(f.readmitted)/COUNT(*),1) AS readmit_pct
        FROM fact_visit f JOIN dim_diagnosis g USING (diagnosis_key)
        GROUP BY g.diagnosis_name ORDER BY visits DESC LIMIT 5""",
    "Visits by quarter": """
        SELECT t.year, t.quarter, COUNT(*) AS visits, ROUND(SUM(f.cost),2) AS total_cost
        FROM fact_visit f JOIN dim_date t USING (date_key)
        GROUP BY t.year, t.quarter ORDER BY t.year, t.quarter""",
    "Visits by age group": """
        SELECT CASE WHEN age_at_visit < 18 THEN '0-17'
                    WHEN age_at_visit < 40 THEN '18-39'
                    WHEN age_at_visit < 65 THEN '40-64' ELSE '65+' END AS age_group,
               COUNT(*) AS visits, ROUND(AVG(cost),2) AS avg_cost
        FROM fact_visit GROUP BY age_group ORDER BY age_group""",
    "Patients with most visits": """
        SELECT p.patient_id, p.name, COUNT(*) AS visits, ROUND(SUM(f.cost),2) AS total_cost
        FROM fact_visit f JOIN dim_patient p USING (patient_key)
        GROUP BY p.patient_key ORDER BY visits DESC LIMIT 5""",
}


def print_table(cur):
    headers = [c[0] for c in cur.description]
    rows = cur.fetchall()
    widths = [max(len(str(x)) for x in [h] + [r[i] for r in rows]) for i, h in enumerate(headers)]
    print("  " + " | ".join(h.ljust(w) for h, w in zip(headers, widths)))
    print("  " + "-+-".join("-" * w for w in widths))
    for r in rows:
        print("  " + " | ".join(str(v).ljust(w) for v, w in zip(r, widths)))


def run_reports(conn):
    for title, sql in REPORTS.items():
        print(f"\n=== {title} ===")
        print_table(conn.execute(sql))


def main():
    generate_source_data()
    raw = extract(SOURCE_CSV)
    clean, _ = transform(raw)
    with sqlite3.connect(DB_PATH) as conn:
        load(conn, clean)
        run_reports(conn)
    print(f"\nWarehouse saved to {os.path.basename(DB_PATH)} (open with any SQLite viewer).")


if __name__ == "__main__":
    main()
