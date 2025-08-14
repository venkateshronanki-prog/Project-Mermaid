import os, re, io, sqlite3, requests, zipfile
import pandas as pd
import yaml
from bs4 import BeautifulSoup
from rapidfuzz import process, fuzz

BASE = os.path.dirname(os.path.dirname(__file__))
DB = os.path.join(BASE, "db", "project_mermaid.db")
RAW = os.path.join(BASE, "..", "data", "raw")
LOGS = os.path.join(BASE, "..", "data", "logs")
os.makedirs(RAW, exist_ok=True)
os.makedirs(LOGS, exist_ok=True)

INSURERS_YAML = os.path.join(BASE, "etl", "insurers.yaml")
MAP_YAML = os.path.join(BASE, "etl", "insurer_name_map.yaml")

UA = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124 Safari/537.36"}

YEAR = 2024

# IRDAI listing pages
HANDBOOK_LIST = "https://irdai.gov.in/handbooks"
HANDBOOK_LIST_DM = "https://irdai.gov.in/handbook-of-indian-insurance?p_p_id=com_irdai_document_media_IRDAIDocumentMediaPortlet&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&_com_irdai_document_media_IRDAIDocumentMediaPortlet_cur=1&_com_irdai_document_media_IRDAIDocumentMediaPortlet_delta=8&_com_irdai_document_media_IRDAIDocumentMediaPortlet_orderByCol=dateid_String_sortable&_com_irdai_document_media_IRDAIDocumentMediaPortlet_orderByType=desc"
ANNUAL_REPORT_LIST = "https://irdai.gov.in/annual-reports?p_p_id=com_irdai_document_media_IRDAIDocumentMediaPortlet&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&_com_irdai_document_media_IRDAIDocumentMediaPortlet_orderByCol=dateid_String_sortable&_com_irdai_document_media_IRDAIDocumentMediaPortlet_orderByType=asc&_com_irdai_document_media_IRDAIDocumentMediaPortlet_resetCur=false&_com_irdai_document_media_IRDAIDocumentMediaPortlet_delta=40"

def ensure_schema():
    schema_path = os.path.join(BASE, "db", "schema.sql")
    with open(schema_path) as f: schema = f.read()
    conn = sqlite3.connect(DB)
    conn.executescript(schema)
    conn.commit(); conn.close()

def seed_insurers():
    with open(INSURERS_YAML) as f:
        insurers = yaml.safe_load(f)
    conn = sqlite3.connect(DB)
    conn.executemany("INSERT OR IGNORE INTO insurers(id,name,type) VALUES (?,?,?)", insurers)
    conn.commit(); conn.close()

def load_maps():
    conn = sqlite3.connect(DB)
    rows = conn.execute("SELECT name,id FROM insurers").fetchall()
    conn.close()
    names = [n for n,_ in rows]
    ids = dict(rows)
    if os.path.exists(MAP_YAML):
        with open(MAP_YAML) as f:
            m = yaml.safe_load(f) or {}
        for raw, canon in m.items():
            if canon in ids:
                ids[raw] = ids[canon]
                names.append(raw)
    return names, ids

def normalize(s): return re.sub(r"\s+"," ",(str(s) or "").strip())
def map_id(raw, names, ids):
    n = normalize(raw)
    if n in ids: return ids[n]
    hit = process.extractOne(n, names, scorer=fuzz.WRatio)
    return ids.get(hit[0]) if hit and hit[1] >= 85 else None

def clean_num(v):
    try:
        s = str(v).replace("\u00A0"," ")
        s = s.replace(",","").replace("%","").replace("Rs.","").replace("Rs","")
        s = re.sub(r"[^\d\.\-]", "", s).strip()
        if s in ("","-",".","na","n/a"): return None
        return float(s)
    except: return None

# Wider header patterns
METRICS = {
    "claim_settlement_ratio": [
        "claim settlement ratio","claim-settlement ratio","settlement ratio",
        "claim settlement (%)","policyholder claims settled","claim settlement %",
        "claim paid ratio","claim paid (%)","claims settled"
    ],
    "solvency_ratio": [
        "solvency ratio","solvency","solvency margin ratio","solvency ratio (%)",
        "available solvency margin","required solvency margin","actual solvency ratio"
    ],
    "gross_premium_total": [
        "gross written premium","gross direct premium","gross premium","gwp",
        "total gross premium","gross premium income","gdi","total premium"
    ],
    "claims_ratio": [
        "incurred claims ratio","claims ratio","icr","net incurred claims ratio","loss ratio"
    ],
    "eom_ratio": [
        "expenses of management","eom","eom ratio","expense of management ratio",
        "total management expenses","management expense ratio"
    ],
    "commission_ratio": [
        "commission","commission ratio","commission expenses","commissions to premium","commission expense ratio"
    ],
    "grievances_received": [
        "grievances received","complaints received","total grievances received","grievances - received"
    ],
    "grievances_resolved": [
        "grievances resolved","complaints resolved","total grievances resolved","grievances - resolved"
    ],
    "grievances_pending": [
        "grievances pending","complaints pending","pending grievances","grievances - pending"
    ],
    "grievances_within_tat_percent": [
        "within tat","tat","resolved within tat","within tat %","resolved within turnaround time"
    ],
    "aum_total": [
        "assets under management","aum","investments","total investments","funds under management"
    ],
}

def find_col(raw_cols, candidates):
    lc = [str(c).lower().strip() for c in raw_cols]
    for cand in candidates:
        cl = cand.lower()
        for i,c in enumerate(lc):
            if cl == c or cl in c:
                return raw_cols[i]
    best = (None, -1, None)
    for cand in candidates:
        m = process.extractOne(cand.lower(), lc, scorer=fuzz.partial_ratio)
        if m and m[1] > best[1]:
            best = (m[0], m[1], lc.index(m[0]))
    return raw_cols[best[2]] if best[2] is not None and best[1] >= 80 else None

def parse_df(df, names, ids):
    if df is None or df.empty: return []
    raw_cols = list(df.columns)
    name_col = find_col(raw_cols, ["insurer","insurer name","company","insurance company","name","name of insurer","company name"])
    if not name_col: return []
    out = []
    for label, hints in METRICS.items():
        val_col = find_col(raw_cols, hints)
        if not val_col: continue
        for _, row in df.iterrows():
            raw_name = str(row.get(name_col, "")).strip()
            if not raw_name or raw_name.lower() in ("total","grand total"): continue
            out.append((raw_name, label, clean_num(row.get(val_col))))
    merged = {}
    for raw_name, label, val in out:
        if val is None: continue
        merged.setdefault(raw_name, {}).update({label: val})
    # map to insurer_id
    recs = []
    unmatched = set()
    for raw_name, vals in merged.items():
        iid = map_id(raw_name, names, ids)
        if not iid:
            unmatched.add(raw_name); continue
        rec = {"insurer_id": iid}
        rec.update(vals)
        recs.append(rec)
    if unmatched:
        with open(os.path.join(LOGS, "unmatched_2024.txt"), "a") as f:
            for nm in sorted(unmatched): f.write(nm + "\n")
    return recs

def upsert(year, recs, source):
    if not recs: return 0
    conn = sqlite3.connect(DB); cur = conn.cursor(); n=0
    for r in recs:
        iid = r.pop("insurer_id", None)
        if not iid: continue
        cols = ", ".join(r.keys())
        vals = list(r.values())
        sql = f"INSERT OR REPLACE INTO indicators (insurer_id, year, {cols}, source) VALUES (?, ?, {','.join('?' for _ in r)}, ?)"
        cur.execute(sql, [iid, year] + vals + [source])
        n += 1
    conn.commit(); conn.close()
    return n

def cache_path(fname): 
    p = os.path.join(RAW, "2024")
    os.makedirs(p, exist_ok=True)
    return os.path.join(p, fname)

def fetch_handbook_2024_zip():
    # Discover via Handbooks listing (and DM page)
    zips = {}
    for url in (HANDBOOK_LIST, HANDBOOK_LIST_DM):
        try:
            r = requests.get(url, headers=UA, timeout=60); r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                text = a.get_text(" ", strip=True)
                if href.lower().endswith(".zip") or text.lower().endswith(".zip"):
                    if "2023-24" in text or "2023_24" in href or "2023-24" in href:
                        full = href if href.startswith("http") else "https://irdai.gov.in" + href
                        zips["2023-24"] = full
        except: 
            continue
    if not zips:
        raise RuntimeError("2023-24 Handbook ZIP not found from IRDAI listings.")
    return zips["2023-24"]

def download_cached(url, fname, min_size=100_000):
    dest = cache_path(fname)
    if os.path.exists(dest) and os.path.getsize(dest) > min_size:
        return dest
    r = requests.get(url, headers=UA, timeout=120); r.raise_for_status()
    if len(r.content) < min_size:
        raise RuntimeError(f"Downloaded file too small: {len(r.content)} bytes")
    with open(dest, "wb") as f: f.write(r.content)
    return dest

def ingest_handbook_2024(names, ids):
    url = fetch_handbook_2024_zip()
    zip_path = download_cached(url, "Handbook_2023_24.zip")
    total = 0
    with zipfile.ZipFile(zip_path, "r") as z:
        members = [m for m in z.namelist() if m.lower().endswith((".csv",".xlsx",".xls"))]
        for m in members:
            try:
                if m.lower().endswith(".csv"):
                    df = pd.read_csv(z.open(m))
                    recs = parse_df(df, names, ids)
                    total += upsert(YEAR, recs, "handbook")
                else:
                    xls = pd.ExcelFile(z.open(m))
                    for sh in xls.sheet_names:
                        df = pd.read_excel(z.open(m), sheet_name=sh)
                        recs = parse_df(df, names, ids)
                        total += upsert(YEAR, recs, "handbook")
            except:
                continue
    print(f"[Handbook 2023-24] Inserted rows: {total}")
    return total

def fetch_annual_report_2024_pdf():
    r = requests.get(ANNUAL_REPORT_LIST, headers=UA, timeout=90); r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(" ", strip=True)
        if ("Annual Report 2023-24" in text or "Annual Report 2023-24" in href) and href.lower().endswith(".pdf"):
            return href if href.startswith("http") else "https://irdai.gov.in" + href
    raise RuntimeError("Annual Report 2023-24 PDF not found in IRDAI listings.")

def parse_annual_report_pdf_simple(pdf_bytes, names, ids):
    # Minimal pass: look for summary tables exported via tabula-like structure in pdfminer fallback
    # To avoid heavy deps, we attempt camelot/tabula substitutes not used; instead rely on simple text scan for AUM/premium per insurer is impractical.
    # As a pragmatic approach, extract only top-level market aggregates not tied to insurer_id.
    # Therefore, for now, we do not ingest insurer-level metrics from AR unless tables are easily parsed.
    # Return empty to avoid wrong data; you can later extend with pdfplumber for detailed tables.
    return []

def ingest_annual_report_2024(names, ids):
    url = fetch_annual_report_2024_pdf()
    pdf_path = download_cached(url, "Annual_Report_2023_24.pdf", min_size=500_000)
    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()
    recs = parse_annual_report_pdf_simple(pdf_bytes, names, ids)
    total = upsert(YEAR, recs, "annual_report")
    print(f"[Annual Report 2023-24] Inserted rows: {total}")
    return total

def year_counts():
    conn = sqlite3.connect(DB)
    rows = conn.execute("""
        SELECT year, source,
               SUM(solvency_ratio IS NOT NULL),
               SUM(claim_settlement_ratio IS NOT NULL),
               SUM(claims_ratio IS NOT NULL),
               SUM(gross_premium_total IS NOT NULL),
               SUM(eom_ratio IS NOT NULL),
               SUM(commission_ratio IS NOT NULL),
               SUM(aum_total IS NOT NULL),
               SUM(grievances_received IS NOT NULL)
        FROM indicators
        GROUP BY year, source
        ORDER BY year, source
    """).fetchall()
    conn.close()
    print("Non-null metric counts by source:", rows)

if __name__ == "__main__":
    ensure_schema()
    seed_insurers()
    names, ids = load_maps()
    # Ingest Handbook 2023-24 into year=2024
    ingest_handbook_2024(names, ids)
    # Ingest Annual Report 2023-24 (placeholder insurer-level parse; extend later)
    try:
        ingest_annual_report_2024(names, ids)
    except Exception as e:
        print("Annual Report parsing skipped (basic mode):", e)
    year_counts()
