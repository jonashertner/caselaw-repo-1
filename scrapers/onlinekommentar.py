#!/usr/bin/env python3
"""
OnlineKommentar.ch scraper.

Downloads all CC-BY-4.0 legal commentaries from the OnlineKommentar JSON API
and saves them as a single JSON file for downstream DB building.

API:
    GET /api/commentaries?page=N       — paginated list (50/page)
    GET /api/commentaries/{uuid}       — full commentary with content + legal_text

Output: output/onlinekommentar/commentaries.json

Usage:
    python -m scrapers.onlinekommentar           # full scrape
    python -m scrapers.onlinekommentar --max 5   # test with 5 entries
"""

import argparse
import html as html_lib
import json
import logging
import os
import re
import time
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("onlinekommentar")

API_BASE = "https://onlinekommentar.ch/api"
OUTPUT_DIR = Path(os.environ.get("OK_OUTPUT_DIR", "output/onlinekommentar"))
OUTPUT_FILE = OUTPUT_DIR / "commentaries.json"

# Map legislative_act UUIDs to (sr_number, abbreviation)
LEGISLATIVE_ACT_MAP = {
    "8223e697-4ffc-4c9b-974b-96836bbbca4f": ("101", "BV"),
    "d2870610-6720-4037-be1c-d870b3189c0f": ("220", "OR"),
    "f04c23a0-391f-41c4-9385-35faf7230f90": ("210", "ZGB"),
    "9e7f5589-45b9-48c3-a19d-05ffe54f3e41": ("311.0", "StGB"),
    "191d45d8-ed6a-47ab-9fb9-17c0744effda": ("312.0", "StPO"),
    "2cdeaaed-30b6-416e-a6ca-7eaef78dfd69": ("272", "ZPO"),
    "8cc7e9b6-eff3-4400-8463-ff14db576ca7": ("955.0", "GwG"),
    "1ecd0f17-8299-4ab0-8e0c-42fd50fa526d": ("235.1", "DSG"),
    "1c7f2762-fc1b-4a51-9b40-3b2086197f87": ("351.1", "IRSG"),
    "cf1153b8-58b2-47eb-a7a3-ec280166bd0d": ("281.1", "SchKG"),
    "d1c89c53-4275-423b-9884-f99c9e136f51": ("812.213", "MepV"),
    "cf0dd38c-fb3a-4090-8794-b3a5e2fea1b3": ("0.311.43", "CCC"),
    "02b30208-85de-4c14-b5fb-0cb408145400": ("444.1", "KGTG"),
    "4512c1a0-c01a-49cb-8c2d-be3f87f796d0": ("161.1", "BPR"),
    "0bc52020-2c96-4c97-8410-8e44ac370dd5": ("251", "KG"),
    "0e999038-1e85-4b97-b912-4d216f850fdc": ("291", "IPRG"),
    "e2c3e574-433c-4f6e-bcc6-eafec7fd7125": ("0.275.12", "LugU"),
    "becaa5f2-8e13-483f-9073-6f7b497b729a": ("152.3", "BGÖ"),
    "4a0601f8-c727-4293-bb18-2585a92dd9fe": ("152.3", "BGÖ"),
}

REQUEST_TIMEOUT = 30
DELAY_BETWEEN_REQUESTS = 1.0
MAX_RETRIES = 3


def html_to_text(html: str) -> str:
    """Convert OK commentary HTML to structured plain text."""
    if not html:
        return ""

    text = html

    # Headings
    text = re.sub(r"<h2[^>]*>(.*?)</h2>", r"\n## \1\n", text, flags=re.DOTALL)
    text = re.sub(r"<h3[^>]*>(.*?)</h3>", r"\n### \1\n", text, flags=re.DOTALL)
    text = re.sub(r"<h[4-6][^>]*>(.*?)</h[4-6]>", r"\n#### \1\n", text, flags=re.DOTALL)

    # Paragraph numbers
    text = re.sub(
        r'<span[^>]*class="paragraph-nr"[^>]*>(.*?)</span>',
        r"[\1] ",
        text,
        flags=re.DOTALL,
    )

    # Footnote superscripts
    text = re.sub(r"<sup[^>]*>(.*?)</sup>", r"^\1", text, flags=re.DOTALL)

    # Line breaks
    text = re.sub(r"<br\s*/?>", "\n", text)

    # Paragraphs
    text = re.sub(r"<p[^>]*>", "\n", text)
    text = re.sub(r"</p>", "\n", text)

    # Lists
    text = re.sub(r"<li[^>]*>", "\n- ", text)
    text = re.sub(r"</li>", "", text)

    # Strip remaining HTML tags
    text = re.sub(r"<[^>]+>", "", text)

    # Unescape HTML entities
    text = html_lib.unescape(text)

    # Collapse whitespace (preserve newlines)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()

    return text


def extract_article_num(title: str) -> str:
    """Extract article number from title like 'Art. 41 OR'."""
    m = re.match(r"Art\.?\s*(\d+[a-z]*(?:bis|ter|quater|quinquies|sexies|septies|octies)?)", title, re.I)
    return m.group(1) if m else ""


def fetch_list_page(session: requests.Session, page: int, language: str = "en") -> list[dict]:
    """Fetch one page of the commentary list."""
    url = f"{API_BASE}/commentaries"
    resp = session.get(url, params={"page": page, "language": language}, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    if isinstance(data, dict):
        return data.get("data", data.get("results", []))
    return data if isinstance(data, list) else []


def fetch_commentary(session: requests.Session, uuid: str, language: str = "en") -> dict | None:
    """Fetch full commentary by UUID with retry on 429."""
    url = f"{API_BASE}/commentaries/{uuid}"
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, params={"language": language}, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                log.debug("Rate limited on %s, waiting %ds (attempt %d/%d)", uuid, wait, attempt + 1, MAX_RETRIES)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            payload = resp.json()
            # API wraps detail in {"data": {...}}
            if isinstance(payload, dict) and "data" in payload:
                return payload["data"]
            return payload
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait = 2 ** (attempt + 1)
                log.debug("Request error on %s: %s, retrying in %ds", uuid, e, wait)
                time.sleep(wait)
            else:
                log.warning("Failed to fetch %s after %d attempts: %s", uuid, MAX_RETRIES, e)
    return None


def scrape(max_items: int | None = None, languages: list[str] | None = None):
    """Main scrape pipeline."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if languages is None:
        languages = ["de", "en", "fr"]

    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "OpenCaseLaw/1.0 (legal research; contact@opencaselaw.ch)",
    })

    # Step 1: Get UUIDs from first language's list (same 362 entries in all languages)
    log.info("Fetching commentary list...")
    all_items: list[dict] = []
    page = 1
    while True:
        items = fetch_list_page(session, page, language=languages[0])
        if not items:
            break
        all_items.extend(items)
        log.info("  page %d: %d items (total: %d)", page, len(items), len(all_items))
        if max_items and len(all_items) >= max_items:
            all_items = all_items[:max_items]
            break
        page += 1
        time.sleep(DELAY_BETWEEN_REQUESTS)

    log.info("Found %d commentaries, fetching in %d languages: %s", len(all_items), len(languages), ", ".join(languages))

    # Step 2: Fetch full content for each UUID in each language
    commentaries: list[dict] = []
    unknown_acts: set[str] = set()
    fetched = 0

    for i, item in enumerate(all_items):
        uuid = item.get("id") or item.get("uuid", "")
        if not uuid:
            log.warning("Skipping item without UUID: %s", item.get("title", "?"))
            continue

        for lang in languages:
            time.sleep(DELAY_BETWEEN_REQUESTS)
            full = fetch_commentary(session, uuid, language=lang)
            if not full:
                continue

            # Resolve legislative act
            act = full.get("legislative_act") or {}
            act_uuid = act.get("id") or act.get("uuid", "")
            sr_info = LEGISLATIVE_ACT_MAP.get(act_uuid)
            if not sr_info and act_uuid:
                act_title = act.get("title", act.get("name", ""))
                if act_uuid not in unknown_acts:
                    log.warning("Unknown legislative_act UUID %s (%s)", act_uuid, act_title)
                    unknown_acts.add(act_uuid)

            sr_number = sr_info[0] if sr_info else ""
            abbr = sr_info[1] if sr_info else ""

            title = full.get("title", "")
            article_num = extract_article_num(title)

            content_html = full.get("content", "")
            content_text = html_to_text(content_html)

            legal_text_html = full.get("legal_text", "")
            legal_text = html_to_text(legal_text_html)

            authors = [
                a.get("name", "") or f"{a.get('first_name', '')} {a.get('last_name', '')}".strip()
                for a in (full.get("authors") or [])
            ]
            editors = [
                e.get("name", "") or f"{e.get('first_name', '')} {e.get('last_name', '')}".strip()
                for e in (full.get("editors") or [])
            ]

            commentary = {
                "ok_uuid": uuid,
                "legislative_act_uuid": act_uuid,
                "sr_number": sr_number,
                "abbr": abbr,
                "article_num": article_num,
                "title": title,
                "language": full.get("language", lang),
                "date": full.get("date") or full.get("published_at", ""),
                "authors": authors,
                "editors": editors,
                "suggested_citation": full.get("suggested_citation_long") or full.get("suggested_citation", ""),
                "html_link": full.get("html_link") or full.get("link", ""),
                "pdf_link": full.get("pdf_link", ""),
                "content_html": content_html,
                "content_text": content_text,
                "legal_text": legal_text,
            }

            commentaries.append(commentary)

        fetched += 1
        if fetched % 50 == 0:
            log.info("  fetched %d/%d commentaries (%d total with all languages)", fetched, len(all_items), len(commentaries))

    log.info("Fetched %d commentary entries total (%d unique x %d languages)", len(commentaries), len(all_items), len(languages))

    by_law: dict[str, int] = {}
    for c in commentaries:
        key = c["abbr"] or c["sr_number"] or "unknown"
        by_law[key] = by_law.get(key, 0) + 1
    for law, count in sorted(by_law.items(), key=lambda x: -x[1]):
        log.info("  %s: %d commentaries", law, count)

    by_lang: dict[str, int] = {}
    for c in commentaries:
        by_lang[c["language"]] = by_lang.get(c["language"], 0) + 1
    log.info("Languages: %s", ", ".join(f"{k}={v}" for k, v in sorted(by_lang.items())))

    if unknown_acts:
        log.warning("Unknown legislative act UUIDs (add to LEGISLATIVE_ACT_MAP):")
        for ua in sorted(unknown_acts):
            log.warning("  %s", ua)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(commentaries, f, ensure_ascii=False, indent=2)

    log.info("Saved to %s (%.1f KB)", OUTPUT_FILE, OUTPUT_FILE.stat().st_size / 1024)


def main():
    parser = argparse.ArgumentParser(
        description="Download commentaries from OnlineKommentar.ch"
    )
    parser.add_argument(
        "--max", type=int, default=None,
        help="Max commentaries to fetch (for testing)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    t0 = time.time()
    scrape(max_items=args.max)
    log.info("Done in %.1f seconds", time.time() - t0)


if __name__ == "__main__":
    main()
