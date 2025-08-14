import os, sqlite3, time, yaml

BASE = os.path.dirname(__file__)
SCHEMA = os.path.join(BASE, "schema.sql")
DB = os.path.join(BASE, "project_mermaid.db")
INSURERS_YAML = os.path.join(os.path.dirname(BASE), "etl", "insurers.yaml")

def seed():
    with open(INSURERS_YAML) as f:
        insurers = yaml.safe_load(f)
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    with open(SCHEMA) as f:
        cur.executescript(f.read())
    cur.executemany("INSERT OR IGNORE INTO insurers (id, name, type) VALUES (?, ?, ?)", insurers)
    conn.commit()
    conn.close()
    print(f"Seeded {len(insurers)} insurers into DB at {time.strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    seed()
