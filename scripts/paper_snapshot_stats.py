"""Regenerate the paper's frozen snapshot (corpus_graph_stats.json) from the
live production databases.

The 2026-05-21 snapshot was produced ad hoc on the VPS; this commits the
generator so the repro capsule is complete: every headline number in
docs/paper/p1-resource is the output of this script against the serving
databases, and `make verify` gates against the same file.

Run ON THE VPS (reads the serving DBs read-only/immutable):
    nice -n 19 ionice -c3 python3 scripts/paper_snapshot_stats.py \
        --out /tmp/corpus_graph_stats.json

Adds, relative to the 05-21 schema, the layers that did not exist then:
scholarship (OA publications + the decision/statute citation bridges),
administrative practice, the expanded Botschaft link layer.
(Usage telemetry is reconstructed separately by scripts/metrics_report.py.)
"""
from __future__ import annotations

import argparse
import datetime
import json
import sqlite3
from pathlib import Path

BASE = Path("/opt/caselaw/repo/output")


def ro(path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
    c.row_factory = sqlite3.Row
    return c


def one(c, sql, *p):
    return c.execute(sql, p).fetchone()[0]


def rows(c, sql, *p):
    return [dict(r) for r in c.execute(sql, p)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--snapshot-date",
                    default=datetime.date.today().isoformat())
    a = ap.parse_args()
    s: dict = {"snapshot_date": a.snapshot_date}

    # ── decisions.db ─────────────────────────────────────────────
    d = ro(BASE / "decisions.db")
    s["total_decisions"] = one(d, "SELECT COUNT(*) FROM decisions")
    s["total_courts"] = one(d, "SELECT COUNT(DISTINCT court) FROM decisions")
    s["total_cantons"] = one(d, "SELECT COUNT(DISTINCT canton) FROM decisions")
    s["languages"] = rows(d, "SELECT language AS lang, COUNT(*) AS n FROM decisions "
                             "GROUP BY 1 ORDER BY 2 DESC")
    s["date_min"] = one(d, "SELECT MIN(decision_date) FROM decisions "
                           "WHERE decision_date >= '1800'")
    s["date_max"] = one(d, "SELECT MAX(decision_date) FROM decisions")
    s["top20_courts"] = rows(d, "SELECT court, COUNT(*) AS n FROM decisions "
                                "GROUP BY 1 ORDER BY 2 DESC LIMIT 20")
    s["per_canton"] = rows(d, "SELECT canton, COUNT(*) AS n FROM decisions "
                              "GROUP BY 1 ORDER BY 2 DESC")
    ec = ("'ecthr_chamber','ecthr_committee','ecthr_grand_chamber',"
          "'hudoc_ch','bge_egmr'")
    s["echr"] = {
        "total": one(d, f"SELECT COUNT(*) FROM decisions WHERE court IN ({ec})"),
        "by_court": rows(d, f"SELECT court, COUNT(*) AS n FROM decisions "
                            f"WHERE court IN ({ec}) GROUP BY 1 ORDER BY 2 DESC"),
        "recent_years": rows(d, f"SELECT substr(decision_date,1,4) AS year, "
                                f"COUNT(*) AS n FROM decisions WHERE court IN ({ec}) "
                                f"GROUP BY 1 ORDER BY 1 DESC LIMIT 5"),
    }
    d.close()

    # ── reference_graph.db ───────────────────────────────────────
    g = ro(BASE / "reference_graph.db")
    s["rg_decisions"] = one(g, "SELECT COUNT(*) FROM decisions")
    # Every extracted (source, citation-token) pair; target_type is the
    # token family ('bge' | 'docket'), not a resolution flag.
    s["rg_citation_edges"] = one(g, "SELECT COUNT(*) FROM decision_citations")
    s["rg_resolved_citations"] = one(
        g, "SELECT COUNT(DISTINCT source_decision_id || '|' || target_ref) "
           "FROM citation_targets")
    s["rg_citation_target_links"] = one(g, "SELECT COUNT(*) FROM citation_targets")
    s["rg_citing_decisions"] = one(
        g, "SELECT COUNT(DISTINCT source_decision_id) FROM citation_targets")
    s["rg_distinct_targets"] = one(
        g, "SELECT COUNT(DISTINCT target_decision_id) FROM citation_targets")
    s["rg_statute_edges"] = one(g, "SELECT COUNT(*) FROM decision_statutes")
    s["rg_distinct_statutes"] = one(
        g, "SELECT COUNT(DISTINCT statute_id) FROM decision_statutes")
    s["match_types"] = rows(g, "SELECT match_type, COUNT(*) AS n FROM "
                               "citation_targets GROUP BY 1 ORDER BY 2 DESC")
    s["cross_lang_matrix"] = rows(
        g, "SELECT ds.language AS source_lang, dt.language AS target_lang, "
           "COUNT(*) AS n FROM citation_targets ct "
           "JOIN decisions ds ON ds.decision_id = ct.source_decision_id "
           "JOIN decisions dt ON dt.decision_id = ct.target_decision_id "
           "WHERE ds.language IN ('de','fr','it') "
           "AND dt.language IN ('de','fr','it') GROUP BY 1, 2")
    s["in_degree_buckets"] = rows(
        g, "SELECT bucket, COUNT(*) AS n FROM (SELECT CASE "
           "WHEN c >= 10000 THEN '10000+' WHEN c >= 1000 THEN '1000-9999' "
           "WHEN c >= 100 THEN '100-999' WHEN c >= 10 THEN '10-99' "
           "ELSE '1-9' END AS bucket FROM (SELECT COUNT(*) AS c FROM "
           "citation_targets GROUP BY target_decision_id)) GROUP BY 1")
    # The most-cited decision exists under two stored id variants; the paper
    # reports the aggregated distinct (source, token) pair count. Freeze it.
    s["top_cited_canonical"] = {
        "decision": "BGE 125 V 351",
        "variants": ["bge_125 V 351", "bge_BGE_125_V_351"],
        "distinct_citing_pairs": one(
            g, "SELECT COUNT(DISTINCT source_decision_id || '|' || target_ref) "
               "FROM citation_targets WHERE target_decision_id IN "
               "('bge_125 V 351','bge_BGE_125_V_351')"),
    }
    s["top30_cited"] = rows(
        g, "SELECT target_decision_id, COUNT(*) AS n FROM citation_targets "
           "GROUP BY 1 ORDER BY 2 DESC LIMIT 30")
    s["top30_statutes"] = rows(
        g, "SELECT st.law_code, st.article, COUNT(DISTINCT ds.decision_id) AS n "
           "FROM decision_statutes ds JOIN statutes st USING (statute_id) "
           "GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 30")
    g.close()

    # ── statutes (federal + cantonal) ────────────────────────────
    f = ro(BASE / "statutes.db")
    s["fed_laws"] = one(f, "SELECT COUNT(*) FROM laws")
    s["fed_articles"] = one(f, "SELECT COUNT(*) FROM articles")
    f.close()
    cl = ro(BASE / "cantonal_laws.db")
    s["cantonal_laws"] = one(cl, "SELECT COUNT(*) FROM laws")
    s["cantonal_articles"] = one(cl, "SELECT COUNT(*) FROM articles")
    s["cantonal_per_canton"] = rows(cl, "SELECT canton, COUNT(*) AS n FROM laws "
                                        "GROUP BY 1 ORDER BY 2 DESC")
    cl.close()

    # ── commentaries ─────────────────────────────────────────────
    ok = ro(BASE / "ok_commentaries.db")
    s["commentaries_total"] = one(ok, "SELECT COUNT(*) FROM commentaries")
    s["commentaries_per_lang"] = rows(ok, "SELECT language AS lang, COUNT(*) AS n "
                                          "FROM commentaries GROUP BY 1 ORDER BY 2 DESC")
    ok.close()

    # ── materialien / Botschaft layer ────────────────────────────
    m = ro(BASE / "materialien.db")
    s["materialien"] = {
        "botschaft_documents": one(m, "SELECT COUNT(*) FROM botschaft_documents"),
        "botschaft_paragraphs": one(m, "SELECT COUNT(*) FROM botschaft_paragraphs"),
        "article_botschaft_links": one(m, "SELECT COUNT(*) FROM article_botschaft_links"),
        "linked_sr_numbers": one(m, "SELECT COUNT(DISTINCT sr_number) FROM "
                                    "article_botschaft_links"),
        "linked_articles": one(m, "SELECT COUNT(DISTINCT sr_number || '|' || article) "
                                  "FROM article_botschaft_links"),
        "curated_digests": one(m, "SELECT COUNT(*) FROM materialien"),
        "amendment_refs": one(m, "SELECT COUNT(*) FROM amendment_refs"),
    }
    m.close()

    # ── layers that postdate the 05-21 snapshot ──────────────────
    sch = ro(BASE / "legal_scholarship.db")
    sch_tabs = {r[0] for r in sch.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    s["scholarship"] = {"publications": one(sch, "SELECT COUNT(*) FROM publications")}
    for tab, key in (("pub_citations_decisions", "cites_decision_edges"),
                     ("pub_citations_statutes", "cites_statute_edges")):
        if tab in sch_tabs:
            s["scholarship"][key] = one(sch, f"SELECT COUNT(*) FROM {tab}")
    s["scholarship"]["sources"] = one(
        sch, "SELECT COUNT(DISTINCT source) FROM publications")
    sch.close()

    p = ro(BASE / "practice.db")
    s["practice"] = {
        "documents": one(p, "SELECT COUNT(*) FROM practice"),
        "by_source": rows(p, "SELECT source, COUNT(*) AS n FROM practice "
                             "GROUP BY 1 ORDER BY 2 DESC"),
        "languages": rows(p, "SELECT language AS lang, COUNT(*) AS n FROM practice "
                             "GROUP BY 1 ORDER BY 2 DESC"),
    }
    p.close()

    out = Path(a.out)
    out.write_text(json.dumps(s, ensure_ascii=False, indent=1))
    print(f"wrote {out} ({out.stat().st_size:,} bytes), "
          f"snapshot {a.snapshot_date}, decisions {s['total_decisions']:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
