import os
import re
import sqlite3
import requests
import zipfile
import io
import pandas as pd
import yaml
from rapidfuzz import process, fuzz

BASE = os.path.dirname(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE, "db", "project_mermaid.db")
INSURERS_YAML = os.path.join(BASE, "etl", "insurers.yaml")
INSURER_MAP_YAML = os.path.join(BASE, "etl", "insurer_name_map.yaml")
RAW_DATA_DIR = os.path.join(BASE, "..", "data", "raw")
LOGS_DIR = os.path.join(BASE, "..", "data", "logs")

os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0"}

HANDBOOK_URLS = {
    2024: "https://irdai.gov.in/sites/default/files/2025-02/Handbook_on_Indian_Insurance_Statistics_2023_24.zip",
    2023: "https://irdai.gov.in/sites/default/files/2024-02/Handbook_on_Indian_Insurance_Statistics_2022_23.zip",
    2022: "https://irdai.gov.in/sites/default/files/2023-02/Handbook_on_Indian_Insurance_Statistics_2021_22.zip",
    2021: "https://irdai.gov.in/sites/default/files/2022-02/Handbook_on_Indian_Insurance_Statistics_2020_21.zip",
    2020: "https://irdai.gov.in/sites/default/files/2021-02/Handbook_on_Indian_Insurance_Statistics_2019_20.zip",
}

def seed_insurers():
    print("Seeding insurers...")
    with open(INSURERS_YAML) as f:
        insurers = yaml.safe_load(f)

    with open(os.path.join(BASE, "db", "schema.sql")) as f:
        schema_sql = f.read()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executescript(schema_sql)
    cur.executemany("INSERT OR IGNORE INTO insurers(id, name, type) VALUES (?, ?, ?)", insurers)
    conn.commit()
    conn.close()
    print(f"Seeded {len(insurers)} insurers.")

def load_insurers():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT name, id FROM insurers")
    rows = cur.fetchall()
    conn.close()
    names = [n for n, _ in rows]
    ids = {n: i for n, i in rows}
    if os.path.exists(INSURER_MAP_YAML):
        with open(INSURER_MAP_YAML) as f:
            overrides = yaml.safe_load(f) or {}
        for raw, canonical in overrides.items():
            if canonical in ids:
                ids[raw] = ids[canonical]
                names.append(raw)
    return names, ids

def normalize(text):
    return re.sub(r"\s+", " ", (text or "").strip())

def map_insurer_id(raw, names, ids):
    n = normalize(raw)
    if n in ids:
        return ids[n]
    m = process.extractOne(n, names, scorer=fuzz.WRatio)
    if m and m[1] >= 85:
        return ids.get(m[0])
    return None

def clean_num(value):
    try:
        s = str(value).replace(",", "").replace("%", "").strip().lower()
        if s in ("", "-", "na", "n/a", "none", "nan"):
            return None
        return float(s)
    except:
        return None

def parse_and_upsert(year, df, names, ids):
    if df.empty:
        return 0, set()
    df.columns = [str(c).lower().strip() for c in df.columns]
    name_col = next((c for c in df.columns if any(k in c for k in ("insurer", "company", "name"))), None)
    if not name_col:
        return 0, set()
    unmatched = set()
    records = []
    metric_keys = {
        "solvency_ratio": ["solvency", "solvency ratio"],
        "claims_ratio": ["incurred claims ratio", "claims ratio"],
        "claim_settlement_ratio": ["claim settlement ratio", "csr", "%"],
        "gross_premium_total": ["gross written premium", "gross premium", "gwp"],
        "grievances_received": ["grievances received", "received"],
        "grievances_resolved": ["grievances resolved", "resolved"],
        "grievances_pending": ["grievances pending", "pending"],
        "grievances_within_tat_percent": ["within tat", "tat", "within tat %"],
    }
    # Identify metric columns that exist in df
    available_metrics = {}
    for metric, keywords in metric_keys.items():
        for col in df.columns:
            if any(k in col for k in keywords):
                available_metrics[metric] = col
                break
    for _, row in df.iterrows():
        raw_name = str(row.get(name_col, "")).strip()
        if not raw_name or raw_name.lower() in ("total", "grand total"):
            continue
        iid = map_insurer_id(raw_name, names, ids)
        if iid is None:
            unmatched.add(raw_name)
            continue
        record = {"insurer_id": iid, "year": year}
        for metric, col in available_metrics.items():
            val = clean_num(row.get(col, None))
            if val is not None:
                record[metric] = val
        records.append(record)
    # Upsert records in DB
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    count = 0
    for r in records:
        iid = r.pop("insurer_id")
        yr = r.pop("year")
        cols = ", ".join(r.keys())
        placeholders = ", ".join("?" for _ in r)
        sql = f"INSERT OR REPLACE INTO indicators (insurer_id, year, {cols}) VALUES (?, ?, {placeholders})"
        cur.execute(sql, [iid, yr] + list(r.values()))
        count +=1
    conn.commit()
    conn.close()
    return count, unmatched

def ingest_data():
    names, ids = load_insurers()
    total_rows = 0
    all_unmatched = set()

    for year, url in HANDBOOK_URLS.items():
        print(f"Downloading {year} data...")
        resp = requests.get(url, headers=HEADERS, timeout=120)
        resp.raise_for_status()
        if len(resp.content) < 50_000:
            print(f"Warning: downloaded file too small for {year}, skipping.")
            continue
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            for fname in z.namelist():
                if fname.lower().endswith(('.xls', '.xlsx', '.csv')):
                    try:
                        if fname.lower().endswith('.csv'):
                            df = pd.read_csv(z.open(fname))
                        else:
                            df = pd.read_excel(z.open(fname), engine='openpyxl')
                    except Exception as e:
                        print(f"Error reading {fname} in {year}: {e}")
                        continue
                    inserted, unmatched = parse_and_upsert(year, df, names, ids)
                    print(f"{year} - Inserted {inserted} rows from {fname}")
                    total_rows += inserted
                    all_unmatched.update(unmatched)
    if all_unmatched:
        print(f"Unmatched insurer names ({len(all_unmatched)}). Writing to logs/unmatched_names.txt")
        with open(os.path.join(LOGS_DIR, 'unmatched_names.txt'), 'a') as f:
            for name in sorted(all_unmatched):
                f.write(name + "\n")
    print(f"Total rows inserted: {total_rows}")

def verify_ingestion():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT year, COUNT(*) FROM indicators GROUP BY year ORDER BY year;")
    rows = cur.fetchall()
    conn.close()
    if not rows:
        print("No indicator data found in database.")
    else:
        print("Indicator row counts per year:")
        for row in rows:
            print(f"Year {row[0]}: {row[1]} rows")

if __name__=="__main__":
    print("Starting seeding...")
    seed_insurers()
    print("Starting ingestion...")
    ingest_data()
    print("Verifying ingestion results...")
    verify_ingestion()
