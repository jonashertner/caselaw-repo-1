#!/usr/bin/env python3
"""Generate RSS 2.0 feeds for the OpenCaseLaw dashboard.

Static XML feeds for the newest decisions, generated from decisions.db and
written to docs/feed.xml + docs/feeds/*.xml. Hooked into publish.py Step 5b
(after stats are generated, before the early git push). GitHub Pages serves
the resulting files at https://opencaselaw.ch/feed.xml etc.

Outputs:
  docs/feed.xml             — top-50 latest across all Swiss courts (global)
  docs/feeds/bger.xml       — top-50 latest Bundesgericht
  docs/feeds/bvger.xml      — top-50 latest Bundesverwaltungsgericht
  docs/feeds/bge.xml        — top-50 latest BGE (Leitentscheide)
  docs/feeds/de.xml         — top-50 latest German-language
  docs/feeds/fr.xml         — top-50 latest French-language
  docs/feeds/it.xml         — top-50 latest Italian-language

Each <item> includes:
  - title:        court + docket + date
  - link:         https://mcp.opencaselaw.ch/entscheid/<id>
  - description:  regeste (or first 250 chars of full_text)
  - pubDate:      RFC 2822 (parsed from decision_date)
  - guid:         ECLI where derivable for federal courts, else decision_id
  - category:     court code + legal_area when present
"""
from __future__ import annotations

import argparse
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path

REPO_DIR = Path(__file__).parent.resolve()

ITEMS_PER_FEED = 50
SITE_URL = "https://opencaselaw.ch"
MCP_URL = "https://mcp.opencaselaw.ch"

COURT_NAMES = {
    "bger": "Bundesgericht",
    "bvger": "Bundesverwaltungsgericht",
    "bge": "BGE (Leitentscheide)",
    "bstger": "Bundesstrafgericht",
    "bpatger": "Bundespatentgericht",
    "bge_egmr": "EGMR (Schweiz)",
}

LANG_NAMES = {"de": "Deutsch", "fr": "Français", "it": "Italiano"}

# Federal courts publish official ECLIs; cantonal courts mostly do not. For
# courts in this map we synthesize an ECLI from docket + date.
ECLI_FEDERAL_COURTS = {
    "bger": "BGer",
    "bge": "BGer",  # BGE is published by BGer, ECLI prefix matches
    "bvger": "BVGer",
    "bstger": "BStGer",
    "bpatger": "BPatGer",
}


def derive_ecli(court: str, docket: str | None, date: str | None) -> str | None:
    if not docket or not date or len(date) < 4:
        return None
    year = date[:4]
    if not year.isdigit():
        return None
    prefix = ECLI_FEDERAL_COURTS.get(court)
    if not prefix:
        return None
    docket_norm = docket.replace("/", ".").replace(" ", "")
    return f"ECLI:CH:{prefix}:{year}:{docket_norm}"


def parse_iso_date(date: str | None) -> datetime | None:
    if not date:
        return None
    try:
        dt = datetime.fromisoformat(date.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = datetime.strptime(date[:10], "%Y-%m-%d")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def make_item(row: dict) -> ET.Element:
    item = ET.Element("item")

    court = row.get("court") or ""
    court_label = COURT_NAMES.get(court, court)
    docket = (row.get("docket_number") or "").strip() or row.get("decision_id", "")
    date = row.get("decision_date") or ""

    title = f"{court_label} {docket}".strip()
    if date:
        title += f" vom {date}"
    ET.SubElement(item, "title").text = title

    decision_id = row["decision_id"]
    ET.SubElement(item, "link").text = f"{MCP_URL}/entscheid/{decision_id}"

    desc = (row.get("regeste") or "").strip()
    if not desc:
        ft = (row.get("full_text") or "").strip()
        desc = (ft[:250] + "…") if len(ft) > 250 else ft
    if desc:
        ET.SubElement(item, "description").text = desc

    dt = parse_iso_date(date)
    if dt is not None:
        ET.SubElement(item, "pubDate").text = format_datetime(dt)

    ecli = derive_ecli(court, docket, date)
    guid = ET.SubElement(item, "guid", isPermaLink="false")
    guid.text = ecli or decision_id

    if court:
        ET.SubElement(item, "category").text = court
    legal_area = (row.get("legal_area") or "").strip()
    if legal_area:
        ET.SubElement(item, "category").text = legal_area

    return item


def make_feed(title: str, description: str, channel_link: str, items: list[dict]) -> str:
    ATOM = "http://www.w3.org/2005/Atom"
    ET.register_namespace("atom", ATOM)

    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = title
    ET.SubElement(channel, "link").text = channel_link
    ET.SubElement(channel, "description").text = description
    ET.SubElement(channel, "language").text = "de-CH"
    ET.SubElement(channel, "lastBuildDate").text = format_datetime(datetime.now(timezone.utc))
    ET.SubElement(channel, "generator").text = "OpenCaseLaw publish.py"

    atom_link = ET.SubElement(channel, f"{{{ATOM}}}link")
    atom_link.set("href", channel_link)
    atom_link.set("rel", "self")
    atom_link.set("type", "application/rss+xml")

    for row in items:
        channel.append(make_item(row))

    ET.indent(rss, space="  ")
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(rss, encoding="unicode")


def query_decisions(conn, where: str, params: tuple, limit: int = ITEMS_PER_FEED) -> list[dict]:
    sql = (
        "SELECT decision_id, court, docket_number, decision_date, language, "
        "regeste, full_text, legal_area "
        "FROM decisions "
        f"WHERE {where} AND decision_date IS NOT NULL AND decision_date != '' "
        "ORDER BY decision_date DESC, decision_id "
        "LIMIT ?"
    )
    cur = conn.execute(sql, params + (limit,))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(REPO_DIR / "output" / "decisions.db"))
    ap.add_argument("--out", default=str(REPO_DIR / "docs"))
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"db not found: {db_path}")

    out = Path(args.out)
    feeds_dir = out / "feeds"
    feeds_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(f"file:{db_path}?immutable=1", uri=True)
    conn.execute("PRAGMA busy_timeout=30000")

    written = []

    items = query_decisions(conn, "1=1", ())
    (out / "feed.xml").write_text(make_feed(
        "OpenCaseLaw — Latest Swiss Court Decisions",
        "Latest published decisions across all Swiss courts indexed by OpenCaseLaw.",
        f"{SITE_URL}/feed.xml",
        items,
    ))
    written.append(("feed.xml", len(items)))

    for court, label in [
        ("bger", "Bundesgericht"),
        ("bvger", "Bundesverwaltungsgericht"),
        ("bge", "BGE (Leitentscheide)"),
    ]:
        items = query_decisions(conn, "court = ?", (court,))
        (feeds_dir / f"{court}.xml").write_text(make_feed(
            f"OpenCaseLaw — {label} (Latest)",
            f"Latest published {label} decisions.",
            f"{SITE_URL}/feeds/{court}.xml",
            items,
        ))
        written.append((f"feeds/{court}.xml", len(items)))

    for lang, label in LANG_NAMES.items():
        items = query_decisions(conn, "language = ?", (lang,))
        (feeds_dir / f"{lang}.xml").write_text(make_feed(
            f"OpenCaseLaw — {label} (Latest)",
            f"Latest published Swiss court decisions in {label}.",
            f"{SITE_URL}/feeds/{lang}.xml",
            items,
        ))
        written.append((f"feeds/{lang}.xml", len(items)))

    conn.close()

    print(f"Generated {len(written)} feeds:")
    for path, count in written:
        print(f"  ✓ {path} ({count} items)")


if __name__ == "__main__":
    main()
