"""
Weblaw Platform Base — Cantonal Court Scraper
===============================================
Base class for courts on Weblaw search portals (query_ticket model).
Covers: AG, ZG, NW, OW, LU, SG, FR, AR, GR, JU, AI, SZ, VS, BL, BE-partial.

Base scraper for Weblaw query_ticket platform.

To implement a new canton:
    class AGGerichteScraper(WeblawBaseScraper):
        CANTON = "AG"
        COURT_CODE_STR = "ag_gerichte"
        DOMAIN = "https://agve.weblaw.ch"
        SUCH_URL = "/de/recherche"
"""
from __future__ import annotations
import logging, re, time
from datetime import date, timedelta
from typing import Iterator
from bs4 import BeautifulSoup
from base_scraper import BaseScraper
from models import Decision, detect_language, extract_citations, make_decision_id, parse_date

logger = logging.getLogger(__name__)
_RE_QT = re.compile(r'<input type="hidden" name="query_ticket" value="(?P<qt>[^"]+)">')
_RE_TREFFER = re.compile(r"(?P<treffer>\d+)\s+(?:Treffer|Ergebnis|résultat)", re.I)

class WeblawBaseScraper(BaseScraper):
    CANTON: str = ""
    COURT_CODE_STR: str = ""
    DOMAIN: str = ""
    SUCH_URL: str = ""
    START_YEAR: int = 1990
    PAGE_SIZE: int = 10
    REQUEST_DELAY: float = 2.0
    HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}

    @property
    def court_code(self) -> str:
        return self.COURT_CODE_STR

    def discover_new(self, since_date=None) -> Iterator[dict]:
        qt = self._get_query_ticket()
        if not qt:
            logger.error(f"[{self.court_code}] No query ticket"); return
        url = f"{self.DOMAIN}{self.SUCH_URL}"
        body = f"s_word=&zips=&method=set_query&query_ticket={qt}"
        resp = self.post(url, data=body, headers=self.HEADERS)
        total = self._hit_count(resp.text)
        logger.info(f"[{self.court_code}] {total} results")
        yield from self._parse_listing(resp.text, since_date)
        offset = self.PAGE_SIZE
        while offset < total:
            body = f"offset={offset}&s_pos=1&method=reload_query&query_ticket={qt}"
            try:
                resp = self.post(url, data=body, headers=self.HEADERS)
                yield from self._parse_listing(resp.text, since_date)
            except Exception as e:
                logger.warning(f"[{self.court_code}] Page error at {offset}: {e}"); break
            offset += self.PAGE_SIZE

    def _get_query_ticket(self) -> str | None:
        try:
            resp = self.get(f"{self.DOMAIN}{self.SUCH_URL}")
            m = _RE_QT.search(resp.text)
            return m.group("qt") if m else None
        except Exception as e:
            logger.error(f"[{self.court_code}] Form fetch failed: {e}"); return None

    def _hit_count(self, html: str) -> int:
        m = _RE_TREFFER.search(html)
        return int(m.group("treffer")) if m else 0

    def _parse_listing(self, html: str, since_date=None) -> Iterator[dict]:
        soup = BeautifulSoup(html, "html.parser")
        for item in soup.select("ol li") or soup.select("div.result-item") or []:
            link = item.find("a")
            if not link: continue
            docket = link.get_text(strip=True)
            href = link.get("href", "")
            url = self.normalize_url(href, self.DOMAIN)
            date_match = re.search(r"\d{2}\.\d{2}\.\d{4}", item.get_text())
            date_str = date_match.group() if date_match else ""
            if since_date and date_str:
                d = parse_date(date_str)
                if d and d < since_date: continue
            did = make_decision_id(self.court_code, docket)
            if self.state.is_known(did): continue
            yield {"docket_number": docket, "decision_date": date_str, "url": url, "decision_id": did}

    def fetch_decision(self, stub: dict) -> Decision | None:
        try:
            resp = self.get(stub["url"])
            soup = BeautifulSoup(resp.text, "html.parser")
            el = soup.select_one("div.entscheid") or soup.select_one("div.content") or soup.select_one("article")
            text = self.clean_text(el.get_text(separator="\n")) if el else ""
            if len(text) < 50: return None
            pdf = soup.select_one("a[href$='.pdf']")
            dd = parse_date(stub.get("decision_date", "")) or date.today()
            return Decision(
                decision_id=stub["decision_id"], court=self.court_code, canton=self.CANTON,
                docket_number=stub["docket_number"], decision_date=dd,
                language=detect_language(text), full_text=text, source_url=stub["url"],
                pdf_url=self.normalize_url(pdf["href"], self.DOMAIN) if pdf else None,
                cited_decisions=extract_citations(text),
            )
        except Exception as e:
            logger.error(f"[{self.court_code}] Fetch error: {e}"); return None
