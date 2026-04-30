"""URL hygiene checks (König P2 + P3).

Every source_url should be absolute (scheme + host) and HTTPS where
available. Past bugs:
- GL/BS scrapers wrote bare `/cgi-bin/...` (no host) → 694 rows fixed
- Mixed http/https on same host → 779k upgraded to https
"""
from __future__ import annotations

import sqlite3

from quality.types import CheckResult, Severity


def check_no_relative_source_urls(conn: sqlite3.Connection, **_) -> CheckResult:
    """source_url must start with http:// or https:// — never a bare
    path. König 2026-04-29 fixed 694 GL/BS rows; this check ensures
    no new scraper introduces the same bug class."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE source_url IS NOT NULL AND source_url != '' "
        "AND source_url NOT LIKE 'http%'"
    ).fetchone()[0]
    sample = [
        dict(r) for r in conn.execute(
            "SELECT decision_id, court, source_url FROM decisions "
            "WHERE source_url IS NOT NULL AND source_url != '' "
            "AND source_url NOT LIKE 'http%' LIMIT 5"
        ).fetchall()
    ] if n else []
    return CheckResult(
        name="urls.no_relative_source_urls",
        severity=Severity.CRITICAL,
        passed=(n == 0),
        metric_value=n,
        threshold=0,
        message=f"{n} relative source_urls" if n else
                "all source_urls are absolute",
        sample_rows=sample,
        fix_advice="scraper must prefix host. See "
                   "scrapers/entscheidsuche_ingest.py for the GL/BS pattern.",
    )


def check_http_when_https_available(conn: sqlite3.Connection, **_) -> CheckResult:
    """Same-host http://... rows when https://... exists for the same
    host means http→https upgrade missed a row. Tolerance: ≤ 50 rows
    total across the corpus."""
    rows = conn.execute(
        "SELECT "
        "  CASE WHEN source_url LIKE 'http://%' THEN substr(source_url, 8) "
        "       ELSE substr(source_url, 9) END AS path_part, "
        "  CASE WHEN source_url LIKE 'http://%' THEN 'http' ELSE 'https' END AS scheme, "
        "  COUNT(*) AS n "
        "FROM decisions "
        "WHERE source_url IS NOT NULL AND source_url != '' "
        "GROUP BY scheme"
    ).fetchall()
    by_scheme = {r["scheme"]: r["n"] for r in rows}
    n_http = by_scheme.get("http", 0)
    return CheckResult(
        name="urls.http_count",
        severity=Severity.WARNING,
        passed=(n_http <= 50),
        metric_value=n_http,
        threshold=50,
        message=f"{n_http:,} rows still use http:// "
                f"(http {n_http:,} / https {by_scheme.get('https', 0):,})",
        extra=by_scheme,
        fix_advice="upgrade scraper to write https://, then run a "
                   "REPLACE(source_url, 'http://', 'https://') backfill",
    )


def check_decision_id_url_safe(conn: sqlite3.Connection, **_) -> CheckResult:
    """decision_id is used in /entscheid/{decision_id:path} URLs.

    The route uses :path-converter so `/` is permitted (legit dockets
    like 4A_321/2013 contain it). Real URL-breakers are characters
    that change the URL's structure: whitespace, `?` (query string),
    `#` (fragment), `&` (param sep). Those must be 0.
    """
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE decision_id LIKE '% %' OR decision_id LIKE '%' || char(9) || '%' "
        "OR decision_id LIKE '%?%' OR decision_id LIKE '%#%' "
        "OR decision_id LIKE '%&%'"
    ).fetchone()[0]
    sample = [
        dict(r) for r in conn.execute(
            "SELECT decision_id, court FROM decisions "
            "WHERE decision_id LIKE '% %' OR decision_id LIKE '%?%' "
            "OR decision_id LIKE '%#%' OR decision_id LIKE '%&%' LIMIT 5"
        ).fetchall()
    ] if n else []
    return CheckResult(
        name="urls.decision_id_url_safe",
        severity=Severity.CRITICAL,
        passed=(n == 0),
        metric_value=n,
        threshold=0,
        message=f"{n} decision_ids contain URL-breaking characters" if n
                else "all decision_ids URL-safe",
        sample_rows=sample,
        fix_advice="decision_ids appear in /entscheid/{id:path} URLs; "
                   "URL-breaking chars (space, tab, ?, #, &) must be sanitised "
                   "at scraper time. Slash (/) is OK.",
    )
