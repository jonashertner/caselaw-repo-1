"""One-time data quality audit script. Run on VPS."""
import sqlite3
import sys

DB = sys.argv[1] if len(sys.argv) > 1 else "/opt/caselaw/repo/output/decisions.db"
conn = sqlite3.connect(DB)
c = conn.cursor()

# CHECK 3: SUSPICIOUS DATES
print("=== CHECK 3: SUSPICIOUS DATES ===")
c.execute("SELECT COUNT(*) FROM decisions WHERE decision_date < '1800-01-01'")
print(f"  Before 1800: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM decisions WHERE decision_date > '2026-12-31'")
print(f"  After 2026: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM decisions WHERE LENGTH(decision_date) > 0 AND LENGTH(decision_date) != 10")
print(f"  Non-standard format: {c.fetchone()[0]}")

# CHECK 4: TEXT QUALITY
print()
print("=== CHECK 4: TEXT QUALITY ===")
c.execute("SELECT COUNT(*) FROM decisions WHERE LENGTH(full_text) < 100")
print(f"  full_text < 100 chars: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM decisions WHERE LENGTH(full_text) < 500")
print(f"  full_text < 500 chars: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM decisions WHERE LENGTH(full_text) BETWEEN 100 AND 500")
short = c.fetchone()[0]
print(f"  full_text 100-500 chars: {short}")

# Top courts with short text
c.execute("""
    SELECT court, COUNT(*) FROM decisions
    WHERE LENGTH(full_text) < 500
    GROUP BY court ORDER BY 2 DESC LIMIT 10
""")
print("  Top courts with short text:")
for row in c.fetchall():
    print(f"    {row[1]:>6} -- {row[0]}")

# CHECK 5: HTML ARTIFACTS
print()
print("=== CHECK 5: HTML ARTIFACTS ===")
c.execute("SELECT COUNT(*) FROM decisions WHERE full_text LIKE '%<br%'")
print(f"  Contains <br> tags: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM decisions WHERE full_text LIKE '%<p>%' OR full_text LIKE '%<div>%'")
print(f"  Contains <p>/<div>: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM decisions WHERE full_text LIKE '%&nbsp;%'")
print(f"  Contains &nbsp;: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM decisions WHERE regeste LIKE '%<br%'")
print(f"  Regeste contains <br>: {c.fetchone()[0]}")

# CHECK 6: ENCODING ARTIFACTS
print()
print("=== CHECK 6: ENCODING ISSUES ===")
for pattern, label in [
    ("%\xc3\xa4%", "mojibake ae"),
    ("%\xc3\xb6%", "mojibake oe"),
    ("%\xc3\xbc%", "mojibake ue"),
    ("%\xc2\xa7%", "mojibake section"),
]:
    c.execute("SELECT COUNT(*) FROM decisions WHERE full_text LIKE ?", (pattern,))
    print(f"  {label}: {c.fetchone()[0]}")

# CHECK 7: REGESTE BY COURT
print()
print("=== CHECK 7: EMPTY REGESTE BY COURT (top 15) ===")
c.execute("""
    SELECT court, COUNT(*) as empty, (SELECT COUNT(*) FROM decisions d2 WHERE d2.court = d1.court) as total
    FROM decisions d1
    WHERE regeste IS NULL OR LENGTH(TRIM(regeste)) = 0
    GROUP BY court ORDER BY empty DESC LIMIT 15
""")
for row in c.fetchall():
    pct = row[1] * 100 / row[2] if row[2] else 0
    print(f"  {row[1]:>7} / {row[2]:>7} ({pct:5.1f}%) -- {row[0]}")

conn.close()
