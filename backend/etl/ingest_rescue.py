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
MAP_FILE = os.path.join(BASE, "etl", "insurer_name_map.yaml")
INSURERS_YAML = os.path.join(BASE, "etl", "insurers.yaml")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124 Safari/537.36"}

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
        "total gross premium","gross premium income","gdi"
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

def ensure_schema():
    with open(os.path.join(BASE, "db", "schema.sql")) as f:
        schema = f.read()
    conn = sqlite3.connect(DB)
    conn.executescript(schema)
    conn.commit()
    conn.close()

def seed_insurers():
    with open(INSURERS_YAML) as f:
        insurers = yaml.safe_load(f)
    conn = sqlite3.connect(DB)
    conn.executemany("INSERT OR IGNORE INTO insurers(id,name,type) VALUES (?,?,?)", insurers)
    conn.commit()
    conn.close()

def fetch_handbook_zip_links():
    pages = [
        "https://irdai.gov.in/handbook-of-indian-insurance?p_p_id=com_irdai_document_media_IRDAIDocumentMediaPortlet&p_p_lifecycle=0&_com_irdai_document_media_IRDAIDocumentMediaPortlet_cur=1",
        "https://irdai.gov.in/handbook-of-indian-insurance?p_p_id=com_irdai_document_media_IRDAIDocumentMediaPortlet&p_p_lifecycle=0&_com_irdai_document_media_IRDAIDocumentMediaPortlet_cur=2"
    ]
    links = {}
    for url in pages:
        r = requests.get(url, headers=UA, timeout=60)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href, text = a["href"].strip(), a.get_text(" ", strip=True)
            if href.lower().endswith(".zip") or text.lower().endswith(".zip"):
                m = re.search(r"(20\d{2})", href + " " + text)
                if m:
                    year = int(m.group(1))
                    if year >= 2019:
                        full = href if href.startswith("http") else "https://irdai.gov.in" + href
                        links[year] = full
    if not links:
        raise RuntimeError("No handbook ZIP links found.")
    print("Years found:", sorted(links.keys(), reverse=True))
    return links

def load_maps():
    conn = sqlite3.connect(DB)
    rows = conn.execute("SELECT name,id FROM insurers").fetchall()
    conn.close()
    names = [n for n,_ in rows]
    ids = dict(rows)
    if os.path.exists(MAP_FILE):
        with open(MAP_FILE) as f:
            extra = yaml.safe_load(f) or {}
        for raw, canon in extra.items():
            if canon in ids:
                ids[raw] = ids[canon]
                names.append(raw)
    return names, ids

def normalize(s): return re.sub(r"\s+"," ",(s or "").strip())

def map_id(raw, names, ids):
    n = normalize(raw)
    if n in ids: return ids[n]
    hit = process.extractOne(n, names, scorer=fuzz.WRatio)
    return ids.get(hit[0]) if hit and hit[1] >= 85 else None

def clean_num(v):
    try:
        s = str(v).replace("\u00A0"," ")
        s = s.replace(",", "").replace("%", "").replace("Rs.", "").replace("Rs", "")
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
    best = (None, -1, None)
    for cand in candidates:
        m = process.extractOne(cand.lower(), lc, scorer=fuzz.partial_ratio)
        if m and m[1] > best[1]:
            best = (m[0], m[1], lc.index(m[0]))
    return raw_cols[best[2]] if best[2] is not None and best[1] >= 80 else None

def parse_df(df, names, ids, unmatched):
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
            iid = map_id(raw_name, names, ids)
            if not iid:
                unmatched.add(raw_name); continue
            val = clean_num(row.get(val_col))
            if val is not None:
                out.append({"insurer_id": iid, label: val})
    merged = {}
    for r in out:
        iid = r["insurer_id"]
        merged.setdefault(iid, {"insurer_id": iid}).update({k:v for k,v in r.items() if k!="insurer_id"})
    return list(merged.values())

def upsert(year, recs):
    if not recs: return 0
    conn = sqlite3.connect(DB); cur = conn.cursor(); n=0
    for rec in recs:
        iid = rec.pop("insurer_id", None)
        if not iid: continue
        cols = ", ".join(rec.keys())
        cur.execute(
            f"INSERT OR REPLACE INTO indicators (insurer_id, year, {cols}) VALUES (?, ?, {','.join('?' for _ in rec)})",
            [iid, year] + list(rec.values())
        )
        n += 1
    conn.commit(); conn.close()
    return n

def cache_path_for_year(year):
    ydir = os.path.join(RAW_DIR, str(year))
    os.makedirs(ydir, exist_ok=True)
    return os.path.join(ydir, f"handbook_{year}.zip")

def download_zip_cached(year, url):
    dest = cache_path_for_year(year)
    if os.path.exists(dest) and os.path.getsize(dest) > 100_000:
        return dest
    r = requests.get(url, headers=UA, timeout=120)
    r.raise_for_status()
    if len(r.content) < 100_000:
        raise RuntimeError(f"{year} ZIP too small ({len(r.content)} bytes).")
    with open(dest, "wb") as f:
        f.write(r.content)
    return dest

def process_year(year, url, names, ids):
    try:
        zip_path = download_zip_cached(year, url)
    except Exception as e:
        print(f"[{year}] Download failed: {e}")
        return 0
    unmatched = set(); total = 0
    with zipfile.ZipFile(zip_path, "r") as z:
        members = [m for m in z.namelist() if m.lower().endswith((".csv",".xlsx",".xls"))]
        ordered = sorted(members, key=lambda x: (0 if "statement" in x.lower() else 1, x))
        for fname in ordered:
            try:
                if fname.lower().endswith(".csv"):
                    df = pd.read_csv(z.open(fname))
                    total += upsert(year, parse_df(df, names, ids, unmatched))
                else:
                    try:
                        df = pd.read_excel(z.open(fname), sheet_name=0)
                        ins = upsert(year, parse_df(df, names, ids, unmatched))
                        total += ins
                        if ins == 0:
                            xls = pd.ExcelFile(z.open(fname))
                            for sh in xls.sheet_names:
                                df2 = pd.read_excel(z.open(fname), sheet_name=sh)
                                total += upsert(year, parse_df(df2, names, ids, unmatched))
                    except Exception:
                        xls = pd.ExcelFile(z.open(fname))
                        for sh in xls.sheet_names:
                            df2 = pd.read_excel(z.open(fname), sheet_name=sh)
                            total += upsert(year, parse_df(df2, names, ids, unmatched))
            except Exception:
                continue
    if unmatched:
        with open(os.path.join(LOGS_DIR, "unmatched_names.txt"), "a") as f:
            for nm in sorted(unmatched): f.write(nm + "\n")
    print(f"[{year}] Inserted rows: {total}")
    return total

def year_counts():
    conn = sqlite3.connect(DB)
    rows = conn.execute("""
        SELECT year,
               SUM(solvency_ratio IS NOT NULL),
               SUM(claim_settlement_ratio IS NOT NULL),
               SUM(claims_ratio IS NOT NULL),
               SUM(gross_premium_total IS NOT NULL),
               SUM(eom_ratio IS NOT NULL),
               SUM(commission_ratio IS NOT NULL),
               SUM(aum_total IS NOT NULL),
               SUM(grievances_received IS NOT NULL)
        FROM indicators
        GROUP BY year
        ORDER BY year
    """).fetchall()
    conn.close()
    if not rows:
        print("No indicator rows found."); return
    print("Non-null counts per metric by year [solv, csr, icr, premium, eom, comm, aum, grievances]:")
    for r in rows: print(r)

if __name__ == "__main__":
    ensure_schema()
    seed_insurers()
    names, ids = load_maps()
    links = fetch_handbook_zip_links()  # IRDAI listings[9][15][16]
    total = 0
    for yr in sorted(links.keys(), reverse=True):
        total += process_year(yr, links[yr], names, ids)
    print("\n=== SUMMARY ===")
    year_counts()
    if total == 0:
        print("No data inserted — check unmatched_names.txt and mappings.")
