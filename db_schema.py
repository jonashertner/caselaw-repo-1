"""
Canonical SQLite schema for Swiss court decisions.

Shared between build_fts5.py (ingestion) and mcp_server.py (search).
Single source of truth — edit here, both consumers pick it up.
"""

SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS decisions (
        decision_id TEXT PRIMARY KEY,
        court TEXT NOT NULL,
        canton TEXT NOT NULL,
        chamber TEXT,
        docket_number TEXT NOT NULL,
        decision_date TEXT,
        publication_date TEXT,
        language TEXT NOT NULL,
        title TEXT,
        legal_area TEXT,
        regeste TEXT,
        full_text TEXT,
        decision_type TEXT,
        outcome TEXT,
        source_url TEXT,
        pdf_url TEXT,
        cited_decisions TEXT,
        scraped_at TEXT,
        json_data TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_decisions_court ON decisions(court);
    CREATE INDEX IF NOT EXISTS idx_decisions_canton ON decisions(canton);
    CREATE INDEX IF NOT EXISTS idx_decisions_date ON decisions(decision_date);
    CREATE INDEX IF NOT EXISTS idx_decisions_language ON decisions(language);
    CREATE INDEX IF NOT EXISTS idx_decisions_docket ON decisions(docket_number);

    CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
        decision_id UNINDEXED,
        court,
        canton,
        docket_number,
        language,
        title,
        regeste,
        full_text,
        content=decisions,
        content_rowid=rowid,
        tokenize='unicode61 remove_diacritics 2'
    );

    -- Triggers to keep FTS in sync
    CREATE TRIGGER IF NOT EXISTS decisions_ai AFTER INSERT ON decisions BEGIN
        INSERT INTO decisions_fts(rowid, decision_id, court, canton,
            docket_number, language, title, regeste, full_text)
        VALUES (new.rowid, new.decision_id, new.court, new.canton,
            new.docket_number, new.language, new.title, new.regeste,
            new.full_text);
    END;

    CREATE TRIGGER IF NOT EXISTS decisions_ad AFTER DELETE ON decisions BEGIN
        INSERT INTO decisions_fts(decisions_fts, rowid, decision_id, court,
            canton, docket_number, language, title, regeste, full_text)
        VALUES ('delete', old.rowid, old.decision_id, old.court, old.canton,
            old.docket_number, old.language, old.title, old.regeste,
            old.full_text);
    END;

    CREATE TRIGGER IF NOT EXISTS decisions_au AFTER UPDATE ON decisions BEGIN
        INSERT INTO decisions_fts(decisions_fts, rowid, decision_id, court,
            canton, docket_number, language, title, regeste, full_text)
        VALUES ('delete', old.rowid, old.decision_id, old.court, old.canton,
            old.docket_number, old.language, old.title, old.regeste,
            old.full_text);
        INSERT INTO decisions_fts(rowid, decision_id, court, canton,
            docket_number, language, title, regeste, full_text)
        VALUES (new.rowid, new.decision_id, new.court, new.canton,
            new.docket_number, new.language, new.title, new.regeste,
            new.full_text);
    END;
"""

# Column order for INSERT statements (must match SCHEMA_SQL table definition)
INSERT_COLUMNS = (
    "decision_id", "court", "canton", "chamber", "docket_number",
    "decision_date", "publication_date", "language", "title",
    "legal_area", "regeste", "full_text", "decision_type",
    "outcome", "source_url", "pdf_url", "cited_decisions",
    "scraped_at", "json_data",
)

INSERT_SQL = f"""INSERT INTO decisions
    ({', '.join(INSERT_COLUMNS)})
    VALUES ({', '.join('?' for _ in INSERT_COLUMNS)})"""

INSERT_OR_IGNORE_SQL = f"""INSERT OR IGNORE INTO decisions
    ({', '.join(INSERT_COLUMNS)})
    VALUES ({', '.join('?' for _ in INSERT_COLUMNS)})"""
