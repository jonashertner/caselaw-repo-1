"""DB consistency check — run on VPS via scp + ssh exec."""
import sqlite3, json

c = sqlite3.connect(
    "file:/opt/caselaw/repo/output/legal_scholarship.db?mode=ro", uri=True
)
total = c.execute("SELECT COUNT(*) FROM publications").fetchone()[0]
n_fts = c.execute("SELECT COUNT(*) FROM publications_fts").fetchone()[0]
no_title = c.execute(
    "SELECT COUNT(*) FROM publications WHERE title IS NULL OR title=''"
).fetchone()[0]
no_src = c.execute(
    "SELECT COUNT(*) FROM publications WHERE source IS NULL OR source=''"
).fetchone()[0]
no_pubid = c.execute(
    "SELECT COUNT(*) FROM publications WHERE pub_id IS NULL OR pub_id=''"
).fetchone()[0]
dup = c.execute(
    "SELECT COUNT(*) FROM (SELECT pub_id, COUNT(*) cc FROM publications "
    "GROUP BY pub_id HAVING cc > 1)"
).fetchone()[0]
bad_year = c.execute(
    "SELECT COUNT(*) FROM publications WHERE year IS NOT NULL "
    "AND (year < 1500 OR year > 2027)"
).fetchone()[0]
broken_url = c.execute(
    "SELECT COUNT(*) FROM publications WHERE url IS NOT NULL "
    "AND url != '' AND url NOT LIKE 'http%'"
).fetchone()[0]
fts_match = c.execute(
    "SELECT COUNT(*) FROM publications_fts WHERE publications_fts MATCH 'Aktienrecht'"
).fetchone()[0]
stat_edges = c.execute("SELECT COUNT(*) FROM pub_citations_statutes").fetchone()[0]
# Per-source license sanity: ex_ante and cognitio should have CC licenses now
exante_cc = c.execute(
    "SELECT COUNT(*) FROM publications "
    "WHERE source='ex_ante' AND license LIKE 'CC-%'"
).fetchone()[0]
exante_total = c.execute(
    "SELECT COUNT(*) FROM publications WHERE source='ex_ante'"
).fetchone()[0]
cognitio_cc = c.execute(
    "SELECT COUNT(*) FROM publications WHERE source='cognitio' "
    "AND license LIKE 'CC-%'"
).fetchone()[0]
cognitio_total = c.execute(
    "SELECT COUNT(*) FROM publications WHERE source='cognitio'"
).fetchone()[0]
eperiodica_total = c.execute(
    "SELECT COUNT(*) FROM publications WHERE source='e_periodica_law'"
).fetchone()[0]
print(json.dumps({
    "total": total, "n_fts": n_fts,
    "no_title": no_title, "no_src": no_src, "no_pubid": no_pubid,
    "dup": dup, "bad_year": bad_year, "broken_url": broken_url,
    "fts_match_aktienrecht": fts_match, "stat_edges": stat_edges,
    "exante_cc": exante_cc, "exante_total": exante_total,
    "cognitio_cc": cognitio_cc, "cognitio_total": cognitio_total,
    "eperiodica_total": eperiodica_total,
}))
