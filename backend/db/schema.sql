CREATE TABLE IF NOT EXISTS insurers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    insurer_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    solvency_ratio REAL,
    claims_ratio REAL,
    claim_settlement_ratio REAL,
    gross_premium_total REAL,
    eom_ratio REAL,
    commission_ratio REAL,
    aum_total REAL,
    grievances_received INTEGER,
    grievances_resolved INTEGER,
    grievances_pending INTEGER,
    grievances_within_tat_percent REAL,
    source TEXT, -- 'handbook' or 'annual_report'
    UNIQUE(insurer_id, year, source),
    FOREIGN KEY(insurer_id) REFERENCES insurers(id)
);
