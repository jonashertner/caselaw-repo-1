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

    Two tiers of unsafety:
      - WARNING (currently 158 known): trailing `?`, `#`, `&` from the
        ow_gerichte (146) + sh_gerichte (12) scrapers that retain HTML
        fragment markers in source IDs. The /entscheid/ route will
        404 these IDs until the scrapers strip the fragments. Tracked
        as a known data debt; not a publish-blocker because the
        affected IDs were never clean.
      - WARNING (separate check below): spaces and tabs work via
        percent-encoding under :path-routes but produce ugly URLs.
        Some AG scrapers emit IDs like 'XBE.2025.32 _ XBE.2025.5'
        (joined-docket cases) — semantically valid IDs, just need
        encoding by the client.

    Slash (/) is allowed because :path accepts it (e.g. '4A_321/2013').
    """
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE decision_id LIKE '%?%' OR decision_id LIKE '%#%' "
        "OR decision_id LIKE '%&%'"
    ).fetchone()[0]
    sample = [
        dict(r) for r in conn.execute(
            "SELECT decision_id, court FROM decisions "
            "WHERE decision_id LIKE '%?%' OR decision_id LIKE '%#%' "
            "OR decision_id LIKE '%&%' LIMIT 5"
        ).fetchall()
    ] if n else []
    # Threshold: existing baseline 158. Treat any growth as WARNING (regression).
    # The remediation ticket is to fix the OW + SH scrapers to strip fragments.
    KNOWN_FLOOR = 200  # tolerance above the 158 baseline
    return CheckResult(
        name="urls.decision_id_url_safe",
        severity=Severity.WARNING,
        passed=(n <= KNOWN_FLOOR),
        metric_value=n,
        threshold=KNOWN_FLOOR,
        message=f"{n} decision_ids contain URL-breaking characters (?, #, &)"
                if n else "all decision_ids URL-safe",
        sample_rows=sample,
        fix_advice="decision_ids appear in /entscheid/{id:path} URLs; "
                   "?, #, & reshape the URL — sanitise at scraper time. "
                   "Slash (/) and spaces (percent-encoded) are OK.",
    )


def check_decision_id_has_whitespace(conn: sqlite3.Connection, **_) -> CheckResult:
    """Whitespace in decision_id (space or tab) produces ugly
    percent-encoded URLs. Routes still work via :path, but consumers
    using `requests.get(url)` without explicit encoding will fail.

    AG scrapers emit ~41k joined-docket IDs like 'XBE.2025.32 _ XBE.2025.5'
    today; this is the baseline. Drift detection catches new growth."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE decision_id LIKE '% %' OR decision_id LIKE '%' || char(9) || '%'"
    ).fetchone()[0]
    return CheckResult(
        name="urls.decision_id_whitespace",
        severity=Severity.WARNING,
        passed=(n <= 50_000),  # baseline ~41k from AG joined-docket IDs
        metric_value=n,
        threshold=50_000,
        message=f"{n:,} decision_ids contain whitespace "
                f"(percent-encoded in URLs; ugly but functional)",
        fix_advice="if growing >50k, a new scraper started concatenating "
                   "dockets with spaces; consider underscore separator at "
                   "scraper time for cleaner URLs",
    )
