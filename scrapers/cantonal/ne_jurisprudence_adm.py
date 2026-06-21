"""
Neuchâtel Administrative Jurisprudence Scraper (NE Jurisprudence administrative)
================================================================================
Scrapes the administrative-recourse decisions/avis of the Conseil d'État and the
cantonal departments (Service juridique de l'État, SJEN) from the FindinfoWeb/Omnis
platform at jurisprudenceadm.ne.ch.

This is DISTINCT from scrapers/cantonal/ne_gerichte.py, which targets the COURTS
portal jurisprudence.ne.ch (Tribunal cantonal, Schema NE_WEB, port 7000). The
administrative jurisprudence is the entire executive-recourse track that
entscheidsuche does NOT aggregate and we did not previously cover — a verified
beyond-es gap (~1,648 decisions to present).

Same Omnis machinery as ne_gerichte; only the Omnis library config differs:
  Schema=NE_JURWEB, Parametername=NEJURWEB, OmnisServer=JURISWEB,8000.

Note: like jurisprudence.ne.ch, ne.ch blocks Hetzner/datacenter IPs at TCP level.
Set NE_PROXY / SCRAPER_PROXY to a SOCKS5 tunnel when running from the VPS.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date
from typing import Iterator

from bs4 import BeautifulSoup

from base_scraper import BaseScraper
from models import Decision, detect_language, extract_citations, make_decision_id

logger = logging.getLogger(__name__)

HOST = "https://jurisprudenceadm.ne.ch"
CGI_PATH = "/scripts/omnisapi.dll"
CGI_URL = HOST + CGI_PATH

RESULTS_PER_PAGE = 20

# Fixed CGI parameters — NE_JURWEB library (admin jurisprudence), port 8000.
BASE_PARAMS = {
    "OmnisPlatform": "WINDOWS",
    "WebServerUrl": "",
    "WebServerScript": "/scripts/omnisapi.dll",
    "OmnisLibrary": "JURISWEB",
    "OmnisClass": "rtFindinfoWebHtmlService",
    "OmnisServer": "JURISWEB,8000",
    "Schema": "NE_JURWEB",
    "Parametername": "NEJURWEB",
}

RE_NF30_KEY = re.compile(r"nF30_KEY=(\d+)")
RE_W10_KEY = re.compile(r"W10_KEY=(\d+)")
RE_DATE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")
# NE admin dockets: REC.2025.12 / DECI.2025.3 / SJEN.2025.7
RE_DOCKET = re.compile(r"([A-Z]{2,5}[\._]\d{4}[\._]\d+(?:/\d+)?)")


def _parse_swiss_date(text):
    if not text:
        return None
    m = RE_DATE.search(text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    return None


def _extract_document_text(soup):
    content = soup.find("div", class_="WordSection1") or soup.find("div", class_="Section1")
    if not content:
        best, best_len = None, 0
        for div in soup.find_all("div"):
            tlen = len(div.get_text(strip=True))
            if tlen > best_len:
                best, best_len = div, tlen
        if best and best_len > 500:
            content = best
    if not content:
        return ""
    paragraphs = [p.get_text(strip=True) for p in content.find_all(["p", "div"])
                  if len(p.get_text(strip=True)) > 1]
    return "\n\n".join(paragraphs) if paragraphs else content.get_text(separator="\n", strip=True)


def _extract_labelled(soup, label):
    """Return the value following an EXACT label cell (e.g. 'Autorité:' -> 'DDTE').

    The NE-adm document repeats its metadata both inside one concatenated mega-cell AND as
    clean label/value pairs; matching the label EXACTLY (not as a substring) avoids the
    mega-cell and lands on the real value. Labels appear in <td> and <b> tags.
    """
    target = label.lower().rstrip(":").strip()
    for el in soup.find_all(["td", "th", "b", "strong", "span"]):
        if el.get_text(strip=True).rstrip(":").strip().lower() == target:
            nxt = el.find_next(["td", "th", "span", "div", "p"])
            if nxt:
                val = nxt.get_text(" ", strip=True)
                if val and not val.endswith(":") and val.lower() != target:
                    return val
    return None


class NEJurisprudenceAdmScraper(BaseScraper):
    """Scraper for NE administrative jurisprudence (Conseil d'État + departments)."""

    REQUEST_DELAY = 2.0
    TIMEOUT = 60
    MAX_ERRORS = 100
    PROXY = os.environ.get("NE_PROXY", "")

    _session_initialized = False

    @property
    def court_code(self):
        return "ne_jurisprudence_adm"

    def _init_session(self):
        if self._session_initialized:
            return
        try:
            self._rate_limit()
            self.session.get(
                CGI_URL,
                params={**BASE_PARAMS, "Aufruf": "loadTemplate",
                        "cTemplate": "search.html", "cSprache": "FRE"},
                timeout=self.TIMEOUT,
            )
            self._session_initialized = True
            logger.info("NE-adm: session initialized")
        except Exception as e:
            logger.warning(f"NE-adm: session init failed: {e}")

    def discover_new(self, since_date=None) -> Iterator[dict]:
        if since_date and isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)
        self._init_session()

        formdata = dict(BASE_PARAMS)
        formdata.update({
            "Aufruf": "validate",
            "cTemplate": "search_resulttable.html",
            "cTemplate_ValidationError": "search.html",
            "cSprache": "FRE",
            "nSeite": "1",
            "nAnzahlTrefferProSeite": str(RESULTS_PER_PAGE),
        })
        try:
            r = self.post(CGI_URL, data=formdata)
        except Exception as e:
            logger.error(f"NE-adm: initial search failed: {e}")
            return

        html = r.text
        total_hits = self._parse_total(html)
        if not total_hits:
            logger.warning("NE-adm: could not determine total hits")
            return
        self.portal_count = total_hits
        total_pages = (total_hits + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
        logger.info(f"NE-adm: {total_hits} total decisions, {total_pages} pages")

        session_key = self._extract_session_key(html)
        yielded = 0
        for stub in self._parse_result_page(html):
            if not self.state.is_known(stub["decision_id"]):
                if since_date and stub.get("decision_date") and stub["decision_date"] < since_date:
                    continue
                yielded += 1
                yield stub

        if total_hits > RESULTS_PER_PAGE and session_key:
            for page in range(2, total_pages + 1):
                try:
                    params = dict(BASE_PARAMS)
                    params.update({
                        "Aufruf": "validate",
                        "cTemplate": "search_resulttable.html",
                        "cSprache": "FRE",
                        "nSeite": str(page),
                        "nAnzahlTrefferProSeite": str(RESULTS_PER_PAGE),
                        "W10_KEY": session_key,
                        "nAnzahlTreffer": str(total_hits),
                    })
                    r = self.get(CGI_URL, params=params)
                    for stub in self._parse_result_page(r.text):
                        if not self.state.is_known(stub["decision_id"]):
                            if since_date and stub.get("decision_date") and stub["decision_date"] < since_date:
                                continue
                            yielded += 1
                            yield stub
                except Exception as e:
                    logger.error(f"NE-adm: page {page} failed: {e}")
                    break
                if page % 20 == 0:
                    logger.info(f"NE-adm: scanned {page}/{total_pages} pages, yielded {yielded}")
        logger.info(f"NE-adm: discovery complete: {yielded} new stubs")

    def _parse_total(self, html: str) -> int | None:
        m = re.search(r"de\s+(\d+)\s+fiche", html) or re.search(r"nAnzahlTreffer=(\d+)", html)
        return int(m.group(1)) if m else None

    def _extract_session_key(self, html: str) -> str | None:
        m = RE_W10_KEY.search(html)
        return m.group(1) if m else None

    def _parse_result_page(self, html: str) -> Iterator[dict]:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            m_key = RE_NF30_KEY.search(a.get("href", ""))
            if not m_key:
                continue
            nf30_key = m_key.group(1)
            link_text = a.get_text(strip=True)
            m_docket = RE_DOCKET.search(link_text) or RE_DOCKET.search(a.get("title", ""))
            docket = m_docket.group(1) if m_docket else f"NE-ADM-{nf30_key}"

            decision_date = None
            parent = a.find_parent("tr") or a.find_parent("div")
            if parent:
                decision_date = _parse_swiss_date(parent.get_text())

            yield {
                "decision_id": make_decision_id("ne_jurisprudence_adm", docket),
                "docket_number": docket,
                "nf30_key": nf30_key,
                "decision_date": decision_date,
                "title": link_text[:200] if link_text else None,
                "url": self._build_doc_url(nf30_key),
            }

    @staticmethod
    def _build_doc_url(nf30_key: str) -> str:
        return (
            f"{CGI_URL}?OmnisPlatform=WINDOWS&WebServerUrl="
            f"&WebServerScript=/scripts/omnisapi.dll&OmnisLibrary=JURISWEB"
            f"&OmnisClass=rtFindinfoWebHtmlService&OmnisServer=JURISWEB,8000"
            f"&Parametername=NEJURWEB&Schema=NE_JURWEB"
            f"&Aufruf=getMarkupDocument&cSprache=FRE&nF30_KEY={nf30_key}"
            f"&Template=search_result_document.html"
        )

    def fetch_decision(self, stub: dict) -> Decision | None:
        url = stub.get("url")
        if not url:
            return None
        try:
            r = self.get(url)
        except Exception as e:
            logger.warning(f"NE-adm: fetch failed for {stub['docket_number']}: {e}")
            return None
        html = r.text
        if len(html) < 500:
            logger.warning(f"NE-adm: short doc for {stub['docket_number']}: {len(html)} chars")
            return None

        soup = BeautifulSoup(html, "html.parser")
        full_text = _extract_document_text(soup)
        if not full_text or len(full_text) < 50:
            logger.warning(f"NE-adm: text too short for {stub['docket_number']}")
            if not full_text:
                full_text = f"[Text extraction failed for {stub['docket_number']}]"

        chamber = _extract_labelled(soup, "Autorité")        # issuing body / department (DDTE, …)
        legal_area = _extract_labelled(soup, "Domaine")
        regeste = _extract_labelled(soup, "Résumé")
        title = _extract_labelled(soup, "Titre") or stub.get("title")
        decision_date = stub.get("decision_date") or _parse_swiss_date(
            _extract_labelled(soup, "Date décision") or "")
        publication_date = _parse_swiss_date(_extract_labelled(soup, "Publié le") or "")
        language = detect_language(full_text) if len(full_text) > 100 else "fr"

        return Decision(
            decision_id=make_decision_id("ne_jurisprudence_adm", stub["docket_number"]),
            court="ne_jurisprudence_adm",
            canton="NE",
            chamber=chamber,
            docket_number=stub["docket_number"],
            decision_date=decision_date,
            publication_date=publication_date,
            language=language,
            title=title,
            regeste=regeste,
            legal_area=legal_area,
            decision_type="Verwaltungsentscheid",
            full_text=full_text,
            source_url=url,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape NE administrative jurisprudence")
    parser.add_argument("--since", type=str)
    parser.add_argument("--max", type=int, default=5)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    since = date.fromisoformat(args.since) if args.since else None
    scraper = NEJurisprudenceAdmScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    for d in decisions:
        print(f"  {d.decision_id}  {d.decision_date}  {len(d.full_text)} chars  {(d.title or '')[:50]}")
    print(f"\nScraped {len(decisions)} NE administrative decisions")
