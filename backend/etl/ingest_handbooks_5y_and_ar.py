import os, re, sqlite3, requests, zipfile
import pandas as pd
from bs4 import BeautifulSoup
from rapidfuzz import process, fuzz
import yaml, concurrent.futures

BASE = os.path.dirname(os.path.dirname(__file__))
DB = os.path.join(BASE, "db", "project_mermaid.db")
RAW_DIR = os.path.join(BASE, "..", "data", "raw")
LOGS_DIR = os.path.join(BASE, "..", "data", "logs")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

INSURERS_YAML = os.path.join(BASE, "etl", "insurers.yaml")
MAP_YAML = os.path.join(BASE, "etl", "insurer_name_map.yaml")
UA = {"User-Agent": "Mozilla/5.0 (IngestionBot)"}
VERBOSE = False

FY_TO_YEAR = {
    "2019-20": 2020, "2020-21": 2021,
    "2021-22": 2022, "2022-23": 2023,
    "2023-24": 2024,
}

LIST_PAGES = [
    "https://irdai.gov.in/handbook-of-indian-insurance?p_p_id=com_irdai_document_media_IRDAIDocumentMediaPortlet"
    "&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&_com_irdai_document_media_IRDAIDocumentMediaPortlet_cur=1"
    "&_com_irdai_document_media_IRDAIDocumentMediaPortlet_delta=20&_com_irdai_document_media_IRDAIDocumentMediaPortlet_orderByCol=dateid_String_sortable"
    "&_com_irdai_document_media_IRDAIDocumentMediaPortlet_orderByType=desc",
    "https://irdai.gov.in/handbooks"
]

AR_LIST = "https://irdai.gov.in/annual-reports?p_p_id=com_irdai_document_media_IRDAIDocumentMediaPortlet&p_p_lifecycle=0" \
          "&p_p_state=normal&p_p_mode=view&_com_irdai_document_media_IRDAIDocumentMediaPortlet_orderByCol=dateid_String_sortable" \
          "&_com_irdai_document_media_IRDAIDocumentMediaPortlet_orderByType=asc&_com_irdai_document_media_IRDAIDocumentMediaPortlet_resetCur=false" \
          "&_com_irdai_document_media_IRDAIDocumentMediaPortlet_delta=40"

METRICS = {
    "claim_settlement_ratio": [
        "claim settlement ratio","claim-settlement ratio","settlement ratio",
        "claim settlement (%)","policyholder claims settled","claim settlement %",
        "claim paid ratio","claim paid (%)","claims settled","claim settlement percentage","claim settled (%)"
    ],
    "solvency_ratio": ["solvency ratio","solvency","solvency margin ratio","solvency ratio (%)",
        "available solvency margin","required solvency margin","actual solvency ratio"],
    "gross_premium_total": ["gross written premium","gross direct premium","gross premium","gwp",
        "total gross premium","gross premium income","gdi","total premium"],
    "claims_ratio": ["incurred claims ratio","claims ratio","icr","net incurred claims ratio","loss ratio"],
    "eom_ratio": ["expenses of management","eom","eom ratio","expense of management ratio","total management expenses","management expense ratio"],
    "commission_ratio": ["commission","commission ratio","commission expenses","commissions to premium","commission expense ratio"],
    "grievances_received": ["grievances received","complaints received","total grievances received","grievances - received"],
    "grievances_resolved": ["grievances resolved","complaints resolved","total grievances resolved","grievances - resolved"],
    "grievances_pending": ["grievances pending","complaints pending","pending grievances","grievances - pending"],
    "grievances_within_tat_percent": ["within tat","tat","resolved within tat","within tat %","resolved within turnaround time"],
    "aum_total": ["assets under management","aum","investments","total investments","funds under management"],
}

def ensure_schema():
    with open(os.path.join(BASE, "db", "schema.sql")) as f: schema = f.read()
    conn = sqlite3.connect(DB); cur = conn.cursor()
    cur.executescript(schema)
    for col, coltype in [("eom_ratio", "REAL"),("commission_ratio", "REAL"),("aum_total", "REAL"),("source", "TEXT")]:
        cur.execute("SELECT COUNT(*) FROM pragma_table_info('indicators') WHERE name=?", (col,))
        if cur.fetchone()[0] == 0:
            cur.execute(f"ALTER TABLE indicators ADD COLUMN {col} {coltype}")
    conn.commit(); conn.close()

def seed_insurers():
    with open(INSURERS_YAML) as f: insurers = yaml.safe_load(f)
    conn = sqlite3.connect(DB); conn.executemany("INSERT OR IGNORE INTO insurers(id,name,type) VALUES (?,?,?)", insurers)
    conn.commit(); conn.close()

def load_maps():
    conn = sqlite3.connect(DB); rows = conn.execute("SELECT name,id FROM insurers").fetchall(); conn.close()
    names = [n for n, _ in rows]; ids = dict(rows)
    if os.path.exists(MAP_YAML):
        with open(MAP_YAML) as f: m = yaml.safe_load(f) or {}
        for raw, canon in m.items():
            if canon in ids:
                ids[raw] = ids[canon]; names.append(raw)
    return names, ids

normalize = lambda s: re.sub(r"\s+"," ",(str(s) or "").strip())
def map_id(raw, names, ids):
    n = normalize(raw)
    if n in ids: return ids[n]
    hit = process.extractOne(n, names, scorer=fuzz.WRatio)
    return ids.get(hit[0]) if hit and hit[1] >= 85 else None
def clean_num(v):
    try:
        s = str(v).replace("\u00A0"," ").replace(",","").replace("%","").replace("Rs.","").replace("Rs","")
        s = re.sub(r"[^\d\.\-]", "", s).strip()
        if s in ("","-",".","na","n/a"): return None
        return float(s)
    except: return None
def find_col(raw_cols, candidates):
    lc = [str(c).lower().strip() for c in raw_cols]
    for cand in candidates:
        cl = cand.lower()
        for i, c in enumerate(lc):
            if cl == c or cl in c: return raw_cols[i]
    best = (None, -1, None)
    for cand in candidates:
        match = process.extractOne(cand.lower(), lc, scorer=fuzz.partial_ratio)
        if match and match[1] > best[1]: best = (match[0], match[1], lc.index(match[0]))
    return raw_cols[best[2]] if best[2] is not None and best[1] >= 80 else None

def download_cached(url, dest, min_size=100_000):
    if os.path.exists(dest) and os.path.getsize(dest) > min_size: return dest
    with requests.get(url, headers=UA, timeout=120, stream=True) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1<<20):
                if chunk: f.write(chunk)
    if os.path.getsize(dest) <= min_size: raise RuntimeError(f"Downloaded too small: {os.path.getsize(dest)} bytes")
    return dest

def parse_df(df, names, ids, fy):
    if df is None or df.empty: return []
    raw_cols = list(df.columns)
    name_col = find_col(raw_cols, ["insurer","insurer name","company","insurance company","name","name of insurer","company name"])
    if not name_col: return []
    metric_cols = {label: find_col(raw_cols, hints) for label, hints in METRICS.items()}
    out_rows = []
    for _, row in df.iterrows():
        nm = str(row.get(name_col, "")).strip()
        if not nm or nm.lower() in ("total", "grand total"): continue
        rec = {label: clean_num(row.get(col)) for label, col in metric_cols.items() if col}
        rec = {k:v for k,v in rec.items() if v is not None}
        if rec: out_rows.append((nm, rec))
    merged = {}
    for nm, vals in out_rows: merged.setdefault(nm, {}).update(vals)
    recs, unmatched = [], set()
    for nm, vals in merged.items():
        iid = map_id(nm, names, ids)
        if not iid: unmatched.add(nm); continue
        rec = {"insurer_id": iid}; rec.update(vals); recs.append(rec)
    if unmatched:
        with open(os.path.join(LOGS_DIR, f"unmatched_names_{fy}.txt"), "a") as f:
            for u in sorted(unmatched): f.write(u + "\n")
    return recs

def upsert(year, recs, source):
    if not recs: return 0
    conn = sqlite3.connect(DB); cur = conn.cursor()
    n = 0
    for r in recs:
        iid = r.pop("insurer_id", None)
        if not iid: continue
        cols = ", ".join(r.keys())
        sql = f"INSERT OR REPLACE INTO indicators (insurer_id, year, {cols}, source) VALUES (?, ?, {','.join('?' for _ in r)}, ?)"
        cur.execute(sql, [iid, year] + list(r.values()) + [source]); n += 1
    conn.commit(); conn.close(); return n

def discover_zip_links():
    found = {}
    for url in LIST_PAGES:
        try:
            r = requests.get(url, headers=UA, timeout=60)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                text = a.get_text(" ", strip=True)
                if href.lower().endswith(".zip") or text.lower().endswith(".zip"):
                    for fy in FY_TO_YEAR.keys():
                        if fy in f"{text} {href}":
                            full = href if href.startswith("http") else "https://irdai.gov.in" + href
                            found[fy] = full
        except Exception: pass
    return found

def fetch_ar_2023_24():
    try:
        r = requests.get(AR_LIST, headers=UA, timeout=90); r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip(); text = a.get_text(" ", strip=True)
            if "Annual Report 2023-24" in f"{text} {href}" and href.lower().endswith(".pdf"):
                full = href if href.startswith("http") else "https://irdai.gov.in" + href
                dest = cache_path(2024, "Annual_Report_2023_24.pdf")
                download_cached(full, dest, min_size=500_000)
                print("Cached Annual Report 2023–24 PDF")
                return
        print("Annual Report 2023–24 not found.")
    except Exception as e:
        print("AR fetch failed:", e)

def cache_path(year, fname):
    ydir = os.path.join(RAW_DIR, str(year)); os.makedirs(ydir, exist_ok=True)
    return os.path.join(ydir, fname)

def process_year(args):
    fy, year, url, names, ids = args
    total = 0
    try:
        dest = cache_path(year, f"handbook_{fy.replace('-', '_')}.zip")
        download_cached(url, dest)
        with zipfile.ZipFile(dest, "r") as z:
            for m in z.namelist():
                if not m.lower().endswith((".csv",".xlsx",".xls")): continue
                if z.getinfo(m).file_size < 2000: continue
                try:
                    if m.lower().endswith(".csv"):
                        df = pd.read_csv(z.open(m), dtype=str)
                        total += upsert(year, parse_df(df, names, ids, fy), "handbook")
                    else:
                        df0 = pd.read_excel(z.open(m), sheet_name=0, dtype=str, engine="openpyxl")
                        ins = upsert(year, parse_df(df0, names, ids, fy), "handbook"); total += ins
                        if ins == 0:
                            xls = pd.ExcelFile(z.open(m), engine="openpyxl")
                            for sh in xls.sheet_names[1:]:
                                dfx = pd.read_excel(z.open(m), sheet_name=sh, dtype=str, engine="openpyxl")
                                total += upsert(year, parse_df(dfx, names, ids, fy), "handbook")
                except Exception as e:
                    if VERBOSE: print(f"Error reading {m}: {e}")
        print(f"[{fy}] Done, inserted: {total}")
    except Exception as e:
        print(f"[{fy}] Failed: {e}")
    return total

def ingest_all():
    ensure_schema(); seed_insurers()
    names, ids = load_maps()
    links = discover_zip_links()
    conn = sqlite3.connect(DB); conn.execute("PRAGMA synchronous = OFF"); conn.execute("PRAGMA journal_mode = MEMORY"); conn.commit(); conn.close()
    tasks = [(fy, year, links.get(fy), names, ids) for fy, year in FY_TO_YEAR.items() if fy in links]
    total = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(tasks))) as exe:
        for res in exe.map(process_year, tasks):
            total += res
    fetch_ar_2023_24()
    print("Total rows inserted:", total)

if __name__ == "__main__":
    ingest_all()
