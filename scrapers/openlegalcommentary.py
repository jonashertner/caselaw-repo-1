#!/usr/bin/env python3
"""Scraper for OpenLegalCommentary.ch BV commentaries.

Fetches commentary on the Swiss Federal Constitution (BV/Cst./Cost.)
from openlegalcommentary.ch. Content is CC BY-SA 4.0.

Output: JSONL file compatible with build_ok_commentaries_db.py ingestion,
or direct SQLite insert into ok_commentaries.db.
"""

import json
import logging
import re
import sqlite3
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://openlegalcommentary.ch"
BV_SR = "101"
BV_ABBR = "BV"

# Languages to scrape
LANGUAGES = ["de", "fr", "it"]
LANG_ABBR = {"de": "BV", "fr": "Cst.", "it": "Cost."}

DELAY = 1.0  # seconds between requests


def _fetch_article_list(lang: str = "de") -> list[str]:
    """Fetch list of article numbers from the BV index page."""
    url = f"{BASE_URL}/{lang}/bv/"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    articles = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.match(rf"/{lang}/bv/art-(.+?)/?$", href)
        if m:
            articles.append(m.group(1))
    return sorted(set(articles), key=lambda x: (int(re.sub(r"[a-z]", "", x) or "0"), x))


def _fetch_article(art_num: str, lang: str = "de") -> dict | None:
    """Fetch and parse a single article commentary page."""
    url = f"{BASE_URL}/{lang}/bv/art-{art_num}"
    resp = requests.get(url, timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract title from h1 or main heading
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else f"Art. {art_num} BV"

    # Extract legal text (Gesetzestext section)
    legal_text = ""
    for heading in soup.find_all(["h2", "h3"]):
        text = heading.get_text(strip=True).lower()
        if any(kw in text for kw in ["gesetzestext", "texte légal", "testo legale", "legal text"]):
            # Collect content until next heading
            parts = []
            for sib in heading.find_next_siblings():
                if sib.name in ("h2", "h3"):
                    break
                parts.append(sib.get_text(separator="\n", strip=True))
            legal_text = "\n".join(parts).strip()
            break

    # Extract doctrine/commentary section
    doctrine_text = ""
    for heading in soup.find_all(["h2", "h3"]):
        text = heading.get_text(strip=True).lower()
        if any(kw in text for kw in ["doktrin", "doctrine", "dottrina"]):
            parts = []
            for sib in heading.find_next_siblings():
                if sib.name == "h2":
                    break
                parts.append(sib.get_text(separator="\n", strip=True))
            doctrine_text = "\n".join(parts).strip()
            break

    # Extract case law section
    caselaw_text = ""
    for heading in soup.find_all(["h2", "h3"]):
        text = heading.get_text(strip=True).lower()
        if any(kw in text for kw in ["rechtsprechung", "jurisprudence", "giurisprudenza", "case law"]):
            parts = []
            for sib in heading.find_next_siblings():
                if sib.name == "h2":
                    break
                parts.append(sib.get_text(separator="\n", strip=True))
            caselaw_text = "\n".join(parts).strip()
            break

    # Combine doctrine + case law as content_text
    content_parts = []
    if doctrine_text:
        content_parts.append(doctrine_text)
    if caselaw_text:
        content_parts.append(f"\n\nRechtsprechung:\n{caselaw_text}")
    content_text = "\n".join(content_parts).strip()

    if not content_text:
        # Try extracting main content area as fallback
        main = soup.find("main") or soup.find("article")
        if main:
            content_text = main.get_text(separator="\n", strip=True)[:5000]

    if not content_text or len(content_text) < 50:
        return None

    return {
        "article_num": art_num,
        "title": title,
        "language": lang,
        "legal_text": legal_text,
        "content_text": content_text,
        "html_link": url,
        "source": "OpenLegalCommentary.ch",
    }


def scrape_all(languages: list[str] | None = None, db_path: Path | None = None) -> list[dict]:
    """Scrape all BV articles across languages.

    If db_path is provided, inserts directly into ok_commentaries.db.
    Returns list of scraped entries.
    """
    langs = languages or LANGUAGES
    results = []

    # Get article list from German index (most complete)
    articles = _fetch_article_list("de")
    logger.info("Found %d BV articles to scrape", len(articles))

    for lang in langs:
        abbr = LANG_ABBR.get(lang, "BV")
        logger.info("Scraping %s (%s)...", lang, abbr)
        for art_num in articles:
            time.sleep(DELAY)
            try:
                entry = _fetch_article(art_num, lang)
                if entry:
                    entry["sr_number"] = BV_SR
                    entry["abbr"] = abbr
                    results.append(entry)
                    logger.debug("  Art. %s (%s): %d chars", art_num, lang, len(entry["content_text"]))
                else:
                    logger.debug("  Art. %s (%s): no content", art_num, lang)
            except Exception as e:
                logger.warning("  Art. %s (%s): error: %s", art_num, lang, e)

    logger.info("Scraped %d commentaries total", len(results))

    if db_path:
        _insert_into_db(results, db_path)

    return results


def _insert_into_db(entries: list[dict], db_path: Path):
    """Insert scraped entries into ok_commentaries.db."""
    conn = sqlite3.connect(str(db_path))
    inserted = 0
    try:
        for e in entries:
            # Use a deterministic ID to avoid duplicates
            olc_id = f"olc_{e['abbr']}_{e['article_num']}_{e['language']}"
            conn.execute(
                """INSERT OR REPLACE INTO commentaries
                   (ok_uuid, legislative_act_uuid, sr_number, abbr, article_num,
                    title, language, date, authors, editors, suggested_citation,
                    html_link, pdf_link, content_html, content_text, legal_text)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    olc_id,
                    "olc_bv_101",  # synthetic legislative_act_uuid
                    e["sr_number"],
                    e["abbr"],
                    e["article_num"],
                    e["title"],
                    e["language"],
                    None,  # date
                    json.dumps(["AI-generated"]),
                    json.dumps([]),
                    None,  # suggested_citation
                    e["html_link"],
                    None,  # pdf_link
                    None,  # content_html
                    e["content_text"],
                    e.get("legal_text", ""),
                ),
            )
            inserted += 1
        conn.execute("INSERT INTO commentaries_fts(commentaries_fts) VALUES('rebuild')")
        conn.commit()
        logger.info("Inserted %d commentaries into %s", inserted, db_path)
    except Exception as e:
        logger.error("DB insert error: %s", e)
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="Scrape OpenLegalCommentary.ch BV")
    parser.add_argument("--db", type=Path, help="Path to ok_commentaries.db for direct insert")
    parser.add_argument("--output", type=Path, help="Output JSONL file")
    parser.add_argument("--lang", nargs="+", default=LANGUAGES, help="Languages to scrape")
    parser.add_argument("--max", type=int, help="Max articles per language")
    args = parser.parse_args()

    results = []
    articles = _fetch_article_list("de")
    if args.max:
        articles = articles[:args.max]

    for lang in args.lang:
        abbr = LANG_ABBR.get(lang, "BV")
        logger.info("Scraping %s (%s)...", lang, abbr)
        for art_num in articles:
            time.sleep(DELAY)
            try:
                entry = _fetch_article(art_num, lang)
                if entry:
                    entry["sr_number"] = BV_SR
                    entry["abbr"] = abbr
                    results.append(entry)
                    logger.info("  Art. %s (%s): %d chars", art_num, lang, len(entry["content_text"]))
            except Exception as e:
                logger.warning("  Art. %s (%s): error: %s", art_num, lang, e)

    logger.info("Total: %d commentaries", len(results))

    if args.db:
        _insert_into_db(results, args.db)

    if args.output:
        with open(args.output, "w") as f:
            for r in results:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        logger.info("Written to %s", args.output)
