import os, re, sqlite3, requests, yaml, zipfile
import pandas as pd
from rapidfuzz import process, fuzz
from bs4 import BeautifulSoup
from io import BytesIO

# ==== Paths ====
BASE = os.path.dirname(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE, "db", "project_mermaid.db")
RAW_DIR = os.path.join(BASE, "..", "data", "raw")
LOG_DIR = os.path.join(BASE, "..", "data", "logs")
MAP_FILE = os.path.join(BASE, "etl", "insurer_name_map.yaml")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

UA = {"User-Agent": "Mozilla/5.0"}

# ===================================
# Scrape IRDAI handbook ZIP links
# ===================================
def fetch_handbook_zip_links():
    url = "https://irdai.gov.in/handbooks"
    print(f"📥 Fetching Handbook ZIP links from: {url}")
    r = requests.get(url, headers=UA, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    zip_links = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".zip"):
            match = re.search(r"(20\d{2})", href + a.get_text())
            if match:
                year = int(match.group(1))
                if year >= 2019:  # ignore old
                    full_url = href if href.startswith("http") else "https://irdai.gov.in" + href
                    zip_links[year] = full_url
    if not zip_links:
        raise RuntimeError("❌ No ZIP links found on the IRDAI handbooks page.")
    print(f"✅ Found ZIPs for years: {sorted(zip_links.keys(), reverse=True)}")
    return zip_links

# ===================================
# Load insurers + name mappings
# ===================================
def load_insurers():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT name, id FROM insurers")
    rows = cur.fetchall()
    conn.close()
    names = [n for n, _ in rows]
    ids = {n: i for n, i in rows}
    if os.path.exists(MAP_FILE):
        with open(MAP_FILE) as f:
            overrides = yaml.safe_load(f) or {}
        for raw, canon in overrides.items():
            if canon in ids:
                ids[raw] = ids[canon]
                names.append(raw)
    return names, ids

# ===================================
# Data utils
# ===================================
normalize = lambda s: re.sub(r"\s+", " ", (s or "").strip())

def map_id(raw, names, ids):
    n = normalize(raw)
    if n in ids: return ids[n]
    m = process.extractOne(n, names, scorer=fuzz.WRatio)
    return ids.get(m[0]) if m and m[1] >= 85 else None

def clean_num(v):
    try:
        s = str(v).replace(",", "").replace("%", "").strip().lower()
        return None if s in ("", "-", "na", "n/a", "none", "nan") else float(s)
    except:
        return None

# ===================================
# Parse + insert data
# ===================================
def parse_table(df, labels, names, ids, unmatched):
    if df.empty: return []
    df.columns = [str(c).strip().lower() for c in df.columns]
    name_col = next((c for c in df.columns if any(k in c for k in ("insurer","company","name"))), None)
    if not name_col: return []
    out = []
    for label, hints in labels.items():
        value_col = next((c for c in df.columns if any(h in c for h in hints)), None)
        if not value_col: continue
        for _, row in df.iterrows():
            raw = str(row[name_col]).strip()
            if not raw or raw.lower() in ("total","grand total"):
                continue
            iid = map_id(raw, names, ids)
            if not iid:
                unmatched.add(raw)
                continue
            val = clean_num(row[value_col])
            if val is not None:
                out.append({"insurer_id": iid, label: val})
    merged = {}
    for r in out:
        iid = r["insurer_id"]
        merged.setdefault(iid, {"insurer_id": iid}).update({k:v for k,v in r.items() if k != "insurer_id"})
    return list(merged.values())

def upsert(year, recs):
    if not recs: return 0
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor(); n = 0
    for rec in recs:
        iid = rec.pop("insurer_id", None)
        if not iid: continue
        cols = ", ".join(rec.keys())
        cur.execute(f"""
            INSERT OR REPLACE INTO indicators (insurer_id, year, {cols})
            VALUES (?, ?, {','.join('?' for _ in rec)})
        """, [iid, year] + list(rec.values()))
        n += 1
    conn.commit(); conn.close()
    return n

# ===================================
# Process one year
# ===================================
def process_year(year, url):
    print(f"\n📅 [{year}] Downloading and processing: {url}")
    r = requests.get(url, headers=UA, timeout=90)
    r.raise_for_status()
    if len(r.content) < 50_000:
        print(f"⚠️ [{year}] File too small - skipping.")
        return 0
    names, ids = load_insurers()
    unmatched, total = set(), 0
    with zipfile.ZipFile(BytesIO(r.content)) as z:
        for file in z.namelist():
            if file.lower().endswith((".csv",".xlsx",".xls")):
                try:
                    df = pd.read_csv(z.open(file)) if file.endswith(".csv") else pd.read_excel(z.open(file))
                except Exception as e:
                    print(f"❌ [{year}] Failed to read {file}: {e}")
                    continue
                labels = {
                    "claim_settlement_ratio": ["claim settlement ratio","csr","%"],
                    "solvency_ratio": ["solvency","solvency ratio"],
                    "gross_premium_total": ["gross written premium","gross premium","gwp"],
                    "claims_ratio": ["incurred claims ratio","claims ratio"],
                    "grievances_received": ["grievances received","received"],
                    "grievances_resolved": ["grievances resolved","resolved"],
                    "grievances_pending": ["grievances pending","pending"],
                    "grievances_within_tat_percent": ["within tat","tat","within tat %"],
                }
                total += upsert(year, parse_table(df, labels, names, ids, unmatched))
    if unmatched:
        with open(os.path.join(LOG_DIR, "unmatched_names.txt"), "a") as f:
            for name in sorted(unmatched): f.write(name + "\n")
        print(f"⚠️ [{year}] {len(unmatched)} unmatched insurer names logged.")
    print(f"✅ [{year}] Inserted rows: {total}")
    return total

# ===================================
# Optional: Scrape Annual Reports (PDF links only for now)
# ===================================
def fetch_annual_reports():
    url = "https://irdai.gov.in/annual-reports"
    print(f"\n📥 Fetching Annual Report PDF links from: {url}")
    r = requests.get(url, headers=UA, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    pdf_links = []
    for a in soup.find_all("a", href=True):
        if a['href'].lower().endswith(".pdf"):
            full_url = a['href'] if a['href'].startswith("http") else "https://irdai.gov.in" + a['href']
            pdf_links.append(full_url)
    print(f"Found {len(pdf_links)} Annual Report PDFs.")
    return pdf_links

# ===================================
# Main run
# ===================================
if __name__ == "__main__":
    try:
        zip_links = fetch_handbook_zip_links()
    except Exception as e:
        print("❌ Failed to get handbook ZIP links:", e)
        exit(1)

    total_all = 0
    for yr in sorted(zip_links.keys(), reverse=True):
        total_all += process_year(yr, zip_links[yr])

    if total_all == 0:
        print("\n❌ No handbook data ingested — check unmatched_names.txt")
    else:
        print(f"\n🎯 Ingestion complete — total inserted rows: {total_all}")

    # Annual reports list
    fetch_annual_reports()
