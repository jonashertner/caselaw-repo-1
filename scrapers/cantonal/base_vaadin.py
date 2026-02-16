"""
Weblaw Vaadin Platform Base — Cantonal Court Scraper
======================================================
Base class for Vaadin UIDL-based cantonal portals.
Covers: BE (VerwG, ZivilStraf), TG (OG), ZH (SozVG), SH (OG), SO.

Base scraper for Weblaw Vaadin platform.

Architecture:
1. GET search form → UIDL JSON → extract Vaadin-Security-Key
2. POST binary UIDL click event → results JSON (state + hierarchy)
3. Parse results from UIDL state text (HTML fragments with <a> tags)
4. Paginate via layout click events (find highest-ID listener)

To implement:
    class BEVerwaltungsgerichtScraper(WeblawVaadinBaseScraper):
        CANTON = "BE"
        COURT_CODE_STR = "be_verwaltungsgericht"
        HOST = "https://be-entscheide.weblaw.ch"
        SUCHFORM = "/le/?v-{}"
"""

from __future__ import annotations

import json
import logging
import random
import re
from typing import Iterator

from bs4 import BeautifulSoup

from base_scraper import BaseScraper
from models import Decision, detect_language, extract_citations, make_decision_id, parse_date

logger = logging.getLogger(__name__)
_RE_TREFFER = re.compile(r"Resultat\s+(?P<von>\d+)-(?P<bis>\d+)\s+von\s+(?P<gesamt>\d+)")
_RE_NUM = re.compile(r' href="[^"]+">(?P<Num>[^<]+)</a>')


class WeblawVaadinBaseScraper(BaseScraper):
    CANTON: str = ""
    COURT_CODE_STR: str = ""
    HOST: str = ""
    SUCHFORM: str = ""
    TREFFERLISTE_URL: str = "/le/UIDL/?v-uiId=0"
    REQUEST_DELAY: float = 2.5
    HEADER = {"Content-Type": "text/plain;charset=UTF-8"}

    # Binary UIDL fragments for Vaadin RPC
    _SEARCH_P1 = (
        b'\x1d[["0","com.vaadin.shared.ui.ui.UIServerRpc","resize",["793","1429","1429","793"]],["'
    )
    _SEARCH_P2 = (
        b'","com.vaadin.shared.ui.button.ButtonServerRpc","click",'
        b'[{"metaKey":false,"altKey":false,"shiftKey":false,"ctrlKey":false,'
        b'"relativeX":"10","clientX":"728","relativeY":"17","clientY":"47",'
        b'"button":"LEFT","type":"1"}]]]'
    )
    _NEXT_P1 = b'\x1d[["0","com.vaadin.shared.ui.ui.UIServerRpc","scroll",["535","0"]],["'
    _NEXT_P2 = (
        b'","com.vaadin.shared.ui.orderedlayout.AbstractOrderedLayoutServerRpc","layoutClick",'
        b'[{"metaKey":false,"altKey":false,"shiftKey":false,"ctrlKey":false,'
        b'"relativeX":"66","clientX":"284","relativeY":"13","clientY":"716",'
        b'"button":"LEFT","type":"8"},null]]]'
    )

    @property
    def court_code(self) -> str:
        return self.COURT_CODE_STR

    def _init_session(self) -> dict | None:
        rid = str(random.randint(10**15, 10**16 - 1))
        try:
            resp = self.post(self.HOST + self.SUCHFORM.format(rid))
            outer = json.loads(resp.text)
            uidl = json.loads(outer["uidl"])
            vk = uidl["Vaadin-Security-Key"]
            state = uidl["state"]
            btn = next((k for k in state if "clickShortcutKeyCode" in state[k]), None)
            cid = next(
                (
                    k
                    for k in state
                    if state[k].get("spacing") and state[k].get("width") == "100.0%"
                ),
                None,
            )
            if not vk or not btn:
                return None
            return {"vk": vk, "btn": btn, "cid": cid}
        except Exception as e:
            logger.error(f"[{self.court_code}] Vaadin init: {e}")
            return None

    def discover_new(self, since_date=None) -> Iterator[dict]:
        sess = self._init_session()
        if not sess:
            return

        url = self.HOST + self.TREFFERLISTE_URL
        body = (
            sess["vk"].encode()
            + self._SEARCH_P1
            + str(sess["btn"]).encode()
            + self._SEARCH_P2
        )
        try:
            resp = self.post(url, data=body, headers=self.HEADER)
        except Exception as e:
            logger.error(f"[{self.court_code}] Search fail: {e}")
            return

        _, stubs = self._parse_vaadin(resp.text, sess)
        for stub in stubs:
            if self._ok(stub, since_date):
                yield stub

        next_key = self._next_key(resp.text)
        while next_key and stubs:
            body = (
                sess["vk"].encode()
                + self._NEXT_P1
                + str(next_key).encode()
                + self._NEXT_P2
            )
            try:
                resp = self.post(url, data=body, headers=self.HEADER)
                _, stubs = self._parse_vaadin(resp.text, sess)
                for stub in stubs:
                    if self._ok(stub, since_date):
                        yield stub
                next_key = self._next_key(resp.text)
            except Exception:
                break

    def _ok(self, stub: dict, since_date) -> bool:
        if since_date and stub.get("decision_date"):
            decision_date = parse_date(stub["decision_date"])
            if decision_date and decision_date < since_date:
                return False

        decision_id = make_decision_id(self.court_code, stub["docket_number"])
        if self.state.is_known(decision_id):
            return False
        stub["decision_id"] = decision_id
        return True

    def _parse_vaadin(self, text: str, sess: dict) -> tuple[int, list[dict]]:
        stubs: list[dict] = []
        total = 0
        try:
            raw = text[8:] if text.startswith("for(;;);") else text
            data = json.loads(raw)
            chunk = data[0] if isinstance(data, list) else data
            state = chunk.get("state", {})

            for value in state.values():
                if "text" in value:
                    match = _RE_TREFFER.search(value["text"])
                    if match:
                        total = int(match.group("gesamt"))

            hierarchy = chunk.get("hierarchy", {})
            cid = sess.get("cid")
            if cid and str(cid) in hierarchy:
                for mid in hierarchy[str(cid)]:
                    state_member = state.get(str(mid), {})
                    if "text" in state_member:
                        stub = self._parse_entry(state_member["text"])
                        if stub:
                            stubs.append(stub)
        except Exception:
            return total, stubs

        return total, stubs

    def _parse_entry(self, html: str) -> dict | None:
        match = _RE_NUM.search(html)
        if not match:
            return None

        soup = BeautifulSoup(html, "html.parser")
        link = soup.find("a")
        url = (self.HOST + link["href"]) if link and link.get("href") else ""
        date_match = re.search(r"\d{2}\.\d{2}\.\d{4}", html)
        return {
            "docket_number": match.group("Num"),
            "decision_date": date_match.group() if date_match else "",
            "url": url,
        }

    def _next_key(self, text: str) -> str | None:
        try:
            raw = text[8:] if text.startswith("for(;;);") else text
            chunk = json.loads(raw)
            if isinstance(chunk, list):
                chunk = chunk[0]
            state = chunk.get("state", {})
            candidates = [
                key
                for key, value in state.items()
                if "registeredEventListeners" in value
            ]
            return max(candidates, key=int) if candidates else None
        except Exception:
            return None

    def fetch_decision(self, stub: dict) -> Decision | None:
        url = stub.get("url", "")
        if not url:
            return None

        try:
            resp = self.get(url)
            soup = BeautifulSoup(resp.text, "html.parser")
            el = (
                soup.select_one("div.entscheid")
                or soup.select_one("div.content")
                or soup.select_one("article")
            )
            text = self.clean_text(el.get_text(separator="\n")) if el else ""
            if len(text) < 50:
                return None

            decision_date = parse_date(stub.get("decision_date", ""))
            if not decision_date:
                logger.warning(
                    f"[{self.court_code}] No date for {stub['docket_number']}"
                )
            pdf = soup.select_one("a[href$='.pdf']")
            return Decision(
                decision_id=stub["decision_id"],
                court=self.court_code,
                canton=self.CANTON,
                docket_number=stub["docket_number"],
                decision_date=decision_date,
                language=detect_language(text),
                full_text=text,
                source_url=url,
                pdf_url=(self.HOST + pdf["href"]) if pdf else None,
                cited_decisions=extract_citations(text),
            )
        except Exception as e:
            logger.error(f"[{self.court_code}] Fetch error: {e}")
            return None
