import os, re, sqlite3, requests, zipfile
import pandas as pd
from io import BytesIO
from bs4 import BeautifulSoup
from rapidfuzz import process, fuzz
import yaml

BASE = os.path.dirname(os.path.dirname(__file__))
DB = os.path.join(BASE, "db", "project_mermaid.db")
RAW_DIR = os.path.join(BASE, "..", "data", "raw")
LOGS_DIR = os.path.join(BASE, "..", "data", "logs")
INSURERS_YAML = os.path.join(BASE, "etl", "insurers.yaml")
MAP_YAML = os.path.join(BASE, "etl", "insurer_name_map.yaml")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

UA = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124 Safari/537.36"}

FY_TO_YEAR = {
    "2019-20": 2020,
    "2020-21": 2021,
    "2021-22": 2022,
    "2022-23": 2023,
    "2023-24": 2024,
}

LIST_PAGES = [
    "https://irdai.gov.in/handbook-of-indian-insurance?p_p_id=com_irdai_document_media_IRDAIDocumentMediaPortlet&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&_com_irdai_document_media_IRDAIDocumentMediaPortlet_cur=1&_com_irdai_document_media_IRDAIDocumentMediaPortlet_delta=20&_com_irdai_document_media_IRDAIDocumentMediaPortlet_orderByCol=dateid_String_sortable&_com_irdai_document_media_IRDAIDocumentMediaPortlet_orderByType=desc",
    "https://irdai.gov.in/handbooks"
]

METRICS = {
    "claim_settlement_ratio": ["claim settlement ratio","claim-settlement ratio","settlement ratio","claim settlement (%)","policyholder claims settled","claim settlement %","claim paid ratio","claim paid (%)","claims settled"],
    "solvency_ratio": ["solvency ratio","solvency","solvency margin ratio","solvency ratio (%)","available solvency margin","required solvency margin","actual solvency ratio"],
    "gross_premium_total": ["gross written premium","gross direct premium","gross premium","gwp","total gross premium","gross premium income","gdi","total premium"],
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
    with open(os.path.join(BASE, "db", "schema.sql")) as f:
        schema = f.read()
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
                ids[raw] = ids[canon]; names.append(raw)
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

def find_col(raw_cols, candidates):
    lc = [str(c).lower().strip() for c in raw_cols]
    for cand in candidates:
        cl = cand.lower()
        for i,c in enumerate(lc):
            if cl == c or cl in c:
                return raw_cols[i]
    return None

def parse_df(df, names, ids):
    if df is None or df.empty: return []
    raw_cols = list(df.columns)
    name_col = find_col(raw_cols, ["insurer","insurer name","company","insurance company","name","name of insurer","company name"])
    if not name_col: return []
    rows = []
    for label, hints in METRICS.items():
        val_col = find_col(raw_cols, hints)
        if not val_col: continue
        for _, row in df.iterrows():
            nm = str(row.get(name_col, "")).strip()
            if not nm or nm.lower() in ("total","grand total"): continue
            val = clean_num(row.get(val_col))
            if val is not None:
                rows.append((nm, label, val))
    if not rows: return []
    by_name = {}
    for nm, label, val in rows:
        by_name.setdefault(nm, {})[label] = val
    recs, unmatched = [], set()
    for nm, vals in by_name.items():
        iid = map_id(nm, names, ids)
        if not iid:
            unmatched.add(nm); continue
        rec = {"insurer_id": iid}
        rec.update(vals)
        recs.append(rec)
    if unmatched:
        with open(os.path.join(LOGS_DIR, "unmatched_names.txt"), "a") as f:
            for u in sorted(unmatched): f.write(u + "\n")
    return recs

def discover_zip_links():
    found = {}
    for url in LIST_PAGES:
        try:
            r = requests.get(url, headers=UA, timeout=60); r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                text = a.get_text(" ", strip=True)
                if href.lower().endswith(".zip") or text.lower().endswith(".zip"):
                    for fy in FY_TO_YEAR.keys():
                        if fy in f"{text} {href}":
                            full = href if href.startswith("http") else "https://irdai.gov.in" + href
                            found[fy] = full
        except:
            continue
    return found

def cache_path(year, fname):
    ydir = os.path.join(RAW_DIR, str(year)); os.makedirs(ydir, exist_ok=True)
    return os.path.join(ydir, fname)

def download_cached(url, dest, min_size=100_000):
    if os.path.exists(dest) and os.path.getsize(dest) > min_size:
        return dest
    r = requests.get(url, headers=UA, timeout=120); r.raise_for_status()
    if len(r.content) < min_size:
        raise RuntimeError(f"Downloaded too small: {len(r.content)} bytes")
    with open(dest, "wb") as f: f.write(r.content)
    return dest

def upsert(year, recs, source):
    if not recs: return 0
    conn = sqlite3.connect(DB); cur = conn.cursor(); n=0
    for r in recs:
        iid = r.pop("insurer_id", None)
        if not iid: continue
        cols = ", ".join(r.keys())
        sql = f"INSERT OR REPLACE INTO indicators (insurer_id, year, {cols}, source) VALUES (?, ?, {','.join('?' for _ in r)}, ?)"
        cur.execute(sql, [iid, year] + list(r.values()) + [source])
        n += 1
    conn.commit(); conn.close()
    return n

def ingest():
    ensure_schema(); seed_insurers()
    names, ids = load_maps()
    links = discover_zip_links()
    total = 0
    for fy, year in FY_TO_YEAR.items():
        url = links.get(fy)
        if not url:
            print(f"[{fy}] ZIP URL not found — skipping.")
            continue
        try:
            dest = cache_path(year, f"handbook_{fy.replace('-', '_')}.zip")
            download_cached(url, dest)
            with zipfile.ZipFile(dest, "r") as z:
                for m in z.namelist():
                    if not m.lower().endswith((".csv",".xlsx",".xls")): continue
                    try:
                        if m.lower().endswith(".csv"):
                            df = pd.read_csv(z.open(m))
                            total += upsert(year, parse_df(df, names, ids), "handbook")
                        else:
                            xls = pd.ExcelFile(z.open(m))
                            for sh in xls.sheet_names:
                                df = pd.read_excel(z.open(m), sheet_name=sh)
                                total += upsert(year, parse_df(df, names, ids), "handbook")
                    except: continue
            print(f"[{fy}] Inserted rows for year={year}")
        except Exception as e:
            print(f"[{fy}] Failed: {e}")
    print("Total rows inserted:", total)

if __name__ == "__main__":
    ingest()
