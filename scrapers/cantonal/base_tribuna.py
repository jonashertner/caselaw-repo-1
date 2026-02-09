"""
Tribuna Platform Base — GWT-RPC Cantonal Court Scraper
=======================================================
Base class for Tribuna Federal / GWT-AJAX cantonal portals.
Covers: TI, GE, BS, NE, GL, SO, VD (FindInfo variant), SH.

Base scraper for Tribuna GWT-RPC platform.

Architecture:
1. Optional cookie init
2. POST GWT-RPC body with page_nr, millis → //OK[count,[entries...]]
3. Some cantons XOR-encrypt responses (ENCRYPTED=True)
4. Paginate by incrementing page_nr
5. Build detail URLs from document IDs

To implement:
    class TIGerichteScraper(TribunaBaseScraper):
        CANTON = "TI"
        COURT_CODE_STR = "ti_gerichte"
        RESULT_PAGE_URL = "https://..."
        RESULT_QUERY_TPL = "..."  # with {page_nr}, {millis}
        DETAIL_URL_TPL = "https://.../{doc_id}"
"""
from __future__ import annotations
import logging, re, time
from datetime import date
from typing import Iterator
from base_scraper import BaseScraper
from models import Decision, detect_language, extract_citations, make_decision_id, parse_date

logger = logging.getLogger(__name__)
_RE_ALL = re.compile(r'(?<=,\\")[^\\"]*(?:\\\\\\\"[^\\"]*)*(?=\\",)')
_RE_ID = re.compile(r'[0-9a-f]{32}|[0-9]{15,17}')
_RE_DATUM = re.compile(r'\d{4}-\d{2}-\d{2}')
_RE_TREFFER = re.compile(r'(?<=^//OK\[)[0-9]+')
_RE_DECRYPT = re.compile(r'(?<=//OK\[1,\[")[0-9a-f]+')

class TribunaBaseScraper(BaseScraper):
    CANTON: str = ""
    COURT_CODE_STR: str = ""
    RESULT_PAGE_URL: str = ""
    RESULT_QUERY_TPL: str = ""
    RESULT_QUERY_TPL_AB: str = ""
    DETAIL_URL_TPL: str = ""
    COOKIE_INIT: str = ""
    HEADERS: dict = {}
    ENCRYPTED: bool = False
    VKAMMER: bool = True
    MINIMUM_PAGE_LEN: int = 148
    MAX_PAGES: int = 20000
    REQUEST_DELAY: float = 2.5

    @property
    def court_code(self) -> str:
        return self.COURT_CODE_STR

    def discover_new(self, since_date=None) -> Iterator[dict]:
        if self.COOKIE_INIT:
            try: self.get(self.COOKIE_INIT)
            except: pass
        page_nr, total = 0, None
        while page_nr < self.MAX_PAGES:
            millis = str(int(time.time() * 1000))
            if since_date and self.RESULT_QUERY_TPL_AB:
                body = self.RESULT_QUERY_TPL_AB.format(page_nr=page_nr, millis=millis,
                    datum=since_date.isoformat() if isinstance(since_date, date) else since_date)
            else:
                body = self.RESULT_QUERY_TPL.format(page_nr=page_nr, millis=millis)
            try: resp = self.post(self.RESULT_PAGE_URL, data=body, headers=self.HEADERS)
            except Exception as e:
                logger.error(f"[{self.court_code}] GWT fail page {page_nr}: {e}"); break
            text = resp.text
            if len(text) < self.MINIMUM_PAGE_LEN: break
            if total is None:
                m = _RE_TREFFER.search(text)
                if m: total = int(m.group()); logger.info(f"[{self.court_code}] {total} results")
            if self.ENCRYPTED: text = self._decrypt(text)
            entries = self._parse_gwt(text)
            if not entries: break
            for entry in entries:
                stub = self._entry_to_stub(entry)
                if not stub: continue
                if since_date and stub.get("decision_date"):
                    d = parse_date(stub["decision_date"])
                    if d and d < since_date: continue
                did = make_decision_id(self.court_code, stub["docket_number"])
                if self.state.is_known(did): continue
                stub["decision_id"] = did; yield stub
            page_nr += 1
            if total and page_nr >= total: break

    def _decrypt(self, text: str) -> str:
        m = _RE_DECRYPT.search(text)
        if not m: return text
        key = bytes.fromhex(m.group())
        start = text.find(m.group()) + len(m.group())
        enc = text[start:]
        try: return "".join(chr(ord(c) ^ key[i % len(key)]) for i, c in enumerate(enc))
        except: return text

    def _parse_gwt(self, text: str) -> list[list[str]]:
        fields = _RE_ALL.findall(text)
        if not fields: return []
        n = 6 if self.VKAMMER else 5
        entries, cur = [], []
        for f in fields:
            cur.append(f.replace('\\"', '"').replace('\\\\', '\\'))
            if len(cur) >= n: entries.append(cur); cur = []
        if cur: entries.append(cur)
        return entries

    def _entry_to_stub(self, fields: list[str]) -> dict | None:
        if len(fields) < 3: return None
        doc_id = fields[0] if _RE_ID.match(fields[0]) else None
        docket = fields[1] if len(fields) > 1 else ""
        date_str = next((f for f in fields if _RE_DATUM.match(f)), "")
        others = [f for f in fields if not _RE_DATUM.match(f) and not _RE_ID.match(f) and f != docket]
        title = max(others, key=len) if others else ""
        if not docket: return None
        url = self.DETAIL_URL_TPL.format(doc_id=doc_id) if doc_id and self.DETAIL_URL_TPL else ""
        return {"docket_number": docket, "decision_date": date_str, "url": url, "title": title, "doc_id": doc_id}

    def fetch_decision(self, stub: dict) -> Decision | None:
        url = stub.get("url", "")
        if not url: return None
        try:
            resp = self.get(url)
            ct = resp.headers.get("Content-Type", "")
            if "pdf" in ct.lower():
                text = self._pdf_text(resp.content)
            else:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "html.parser")
                el = soup.select_one("div.content") or soup.select_one("article") or soup.select_one("body")
                text = el.get_text(separator="\n") if el else resp.text
            text = self.clean_text(text)
            if len(text) < 50: return None
            dd = parse_date(stub.get("decision_date", "")) or date.today()
            return Decision(
                decision_id=stub["decision_id"], court=self.court_code, canton=self.CANTON,
                docket_number=stub["docket_number"], decision_date=dd,
                language=detect_language(text), title=stub.get("title"), full_text=text,
                source_url=url, cited_decisions=extract_citations(text),
            )
        except Exception as e:
            logger.error(f"[{self.court_code}] Fetch error: {e}"); return None

    def _pdf_text(self, data: bytes) -> str:
        try:
            import fitz; doc = fitz.open(stream=data, filetype="pdf")
            return "\n\n".join(p.get_text() for p in doc)
        except ImportError: pass
        try:
            from pdfminer.high_level import extract_text; from io import BytesIO
            return extract_text(BytesIO(data))
        except ImportError: pass
        return ""
