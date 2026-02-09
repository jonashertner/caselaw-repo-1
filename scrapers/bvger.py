"""
Bundesverwaltungsgericht (BVGer) — Federal Administrative Court Scraper
========================================================================

DUAL-MODE ARCHITECTURE (Feb 2026 rewrite):

  Mode A (PRIMARY):  Weblaw Lawsearch v4 JSON API at bvger.weblaw.ch
    - Same API pattern as BStGer (bstger.weblaw.ch/api/getDocuments)
    - POST JSON with date-range filters → paginated JSON results
    - Full text via /api/getDocumentContent/{leid}
    - PDF via /api/getDocumentFile/{leid}

  Mode B (FALLBACK): jurispub.admin.ch ICEfaces
    - Legacy ICEfaces (JSF) — still alive as of Feb 2026
    - Requires JSESSIONID + ICE session for stateful interaction
    - PDF downloads: /publiws/download?decisionId={uuid}

Coverage: 2007–present (~50,000+ decisions)
Rate limiting: 3 seconds
"""
from __future__ import annotations
import json, logging, random, re, time
from datetime import date, datetime, timedelta
from typing import Iterator
from base_scraper import BaseScraper
from models import (Decision, detect_language, extract_citations,
                    make_decision_id, parse_date)

logger = logging.getLogger(__name__)

# ── Mode A: Weblaw Lawsearch v4 API ─────────────────────────
WEBLAW_HOST = "https://bvger.weblaw.ch"
WEBLAW_DOCS_URL = f"{WEBLAW_HOST}/api/getDocuments?withAggregations=false"
WEBLAW_CONTENT_URL = f"{WEBLAW_HOST}/api/getDocumentContent/"
WEBLAW_PDF_URL = f"{WEBLAW_HOST}/api/getDocumentFile/"
WEBLAW_HEADERS = {
    "Content-Type": "application/json", "Accept": "*/*",
    "Origin": WEBLAW_HOST,
    "Referer": f"{WEBLAW_HOST}/?sort-field=relevance&sort-direction=relevance",
}
WEBLAW_BASE_JSON = {
    "guiLanguage": "de",
    "aggs": {"fields": ["year","language","court","rulingDate",
                         "publicationDate","sortRulingDate"], "size":"10"},
}

# ── Mode B: jurispub.admin.ch ICEfaces ──────────────────────
JP_HOST = "https://jurispub.admin.ch"
JP_SESSION_URL = f"{JP_HOST}/publiws/?lang=de"
JP_SUCH_URL = f"{JP_HOST}/publiws/block/send-receive-updates;jsessionid="
JP_PDF_URL = f"{JP_HOST}/publiws/download?decisionId="
JP_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Accept": "*/*", "Origin": JP_HOST,
    "Referer": f"{JP_HOST}/publiws/publiws/?lang=de",
}
RE_TREFFER = re.compile(
    r'return iceSubmitPartial\(form,this,event\);" onfocus="setFocus\(this\.id\);">'
    r"(?P<Raw>(?P<Num>[^<]+)</a>[^C]+C[^<]+<a class=\"iceOutLnk\" href=\""
    r'(?P<PDFUrl>[^"]+jsessionid=[0-9A-F]+\?decisionId=(?P<DocId>[0-9a-f-]+))" '
    r'id="form:resultTable:(?P<Pos>[^"]+):[^C]+Col1">[^>]+>'
    r"(?P<EDatum>[^<]+)[^O]+[^>]+>(?P<VKammer>[^<]+)<[^O]+O[^>]+>"
    r"(?P<Titel>[^<]+)[^G]+G[^O]+O[^>]+>(?P<LeitsatzKurz>[^<]*)<[^O]+O[^>]+>"
    r"(?P<Leitsatz>[^<]*))")
RE_JSESSION = re.compile(r"(?<=JSESSIONID=)[0-9A-F]+(?=;)")
RE_ICE_SESSION = re.compile(r'(?<=script id=")[^:]+(?=:1:configuration-script)')
RE_TREFFERZAHL = re.compile(
    r'<span class="iceOutFrmt standard">([0-9]+(?:,[0-9]+)?) Entscheide gefunden, '
    r"zeige ([0-9]+(?:,[0-9]+)?) bis ([0-9]+(?:,[0-9]+)?)\. "
    r"Seite ([0-9]+(?:,[0-9]+)?) von ([0-9]+(?:,[0-9]+)?)\. Resultat sortiert")

# ── Common ───────────────────────────────────────────────────
START_YEAR = 2007
WINDOW_DAYS = 64
MAX_PER_WINDOW = 100

BVGER_ABTEILUNGEN = {
    "I": "Abteilung I (Infrastruktur, Umwelt, Abgaben, Personal)",
    "II": "Abteilung II (Wirtschaft, Wettbewerb, Bildung)",
    "III": "Abteilung III (Sozialversicherungen, Gesundheit)",
    "IV": "Abteilung IV (Asylrecht)",
    "V": "Abteilung V (Asylrecht)",
    "VI": "Abteilung VI (Ausländer- und Bürgerrecht)",
}
_PREFIX_MAP = {"A":"I","B":"II","C":"III","D":"IV","E":"V","F":"VI"}

def _detect_abteilung(docket: str, raw: str|None=None) -> str|None:
    if raw:
        for k, v in BVGER_ABTEILUNGEN.items():
            if k in raw or v in raw:
                return v
    if docket:
        k = _PREFIX_MAP.get(docket[0].upper())
        if k: return BVGER_ABTEILUNGEN[k]
    return None

def _rand_uid() -> str:
    return "_"+"".join(random.choice("0123456789abcdefghijklmnopqrstuvwxyz") for _ in range(8))


class BVGerScraper(BaseScraper):
    REQUEST_DELAY = 3.0
    TIMEOUT = 60

    @property
    def court_code(self) -> str:
        return "bvger"

    # ── Main entry ───────────────────────────────────────────
    def discover_new(self, since_date=None) -> Iterator[dict]:
        try:
            results = list(self._discover_weblaw(since_date))
            if results:
                logger.info(f"BVGer Weblaw: {len(results)} stubs")
                yield from results; return
            logger.warning("BVGer Weblaw: 0 results, trying jurispub fallback")
        except Exception as e:
            logger.warning(f"BVGer Weblaw failed ({e}), falling back to jurispub")
        yield from self._discover_icefaces(since_date)

    def fetch_decision(self, stub: dict) -> Decision|None:
        try:
            docket = stub["docket_number"]
            dd = parse_date(stub.get("decision_date","")) or date(stub.get("year",date.today().year),1,1)
            full_text = stub.get("full_text","")
            src = stub.get("_source","unknown")
            leid = stub.get("doc_id","")
            if not full_text and leid and src == "weblaw":
                full_text = self._fetch_weblaw_content(leid)
            title = stub.get("title","")
            headnote = stub.get("headnote","")
            if not full_text:
                full_text = f"{title}\n\n{headnote}" if headnote else title
            lang = detect_language(full_text) if full_text else "de"
            chamber = _detect_abteilung(docket, stub.get("chamber"))
            if src == "weblaw" and leid:
                source_url = f"{WEBLAW_HOST}/cache?id={leid}&guiLanguage={lang}"
            else:
                source_url = stub.get("source_url", JP_SESSION_URL)
            pdf_url = stub.get("pdf_url")
            if not pdf_url and leid:
                pdf_url = (f"{WEBLAW_PDF_URL}{leid}?locale=de" if src=="weblaw"
                           else f"{JP_PDF_URL}{leid}")
            return Decision(
                decision_id=make_decision_id("bvger", docket),
                court="bvger", canton="CH", chamber=chamber,
                docket_number=docket, decision_date=dd,
                publication_date=parse_date(stub.get("publication_date","")),
                language=lang, title=title or None,
                regeste=stub.get("headnote_short") or headnote or None,
                full_text=self.clean_text(full_text) if full_text.strip() else "(metadata only)",
                source_url=source_url, pdf_url=pdf_url,
                cited_decisions=extract_citations(full_text) if full_text else [],
                scraped_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.error(f"BVGer fetch error {stub.get('docket_number','?')}: {e}", exc_info=True)
            return None

    # ── Mode A: Weblaw ───────────────────────────────────────
    def _discover_weblaw(self, since_date=None) -> Iterator[dict]:
        start = date(START_YEAR,1,1)
        if since_date:
            if isinstance(since_date,str): since_date = parse_date(since_date) or start
            start = max(since_date, start)
        wd = WINDOW_DAYS; cur = start; today = date.today()
        while cur < today:
            docs, total, more, bis = self._wl_window(cur, wd)
            if total > MAX_PER_WINDOW:
                wd = max(1, wd//2)
                logger.info(f"Window {cur}–{bis}: {total} results → {wd}d")
                continue
            logger.info(f"BVGer Weblaw {cur}–{bis}: {total} decisions")
            for d in docs:
                s = self._wl_parse(d)
                if s and not self.state.is_known(make_decision_id("bvger",s["docket_number"])):
                    yield s
            off = len(docs)
            while more and off < total:
                docs, _, more, _ = self._wl_window(cur, wd, off)
                for d in docs:
                    s = self._wl_parse(d)
                    if s and not self.state.is_known(make_decision_id("bvger",s["docket_number"])):
                        yield s
                off += len(docs)
            cur = bis + timedelta(days=1)

    def _wl_window(self, ab: date, wd: int, off: int=0) -> tuple[list,int,bool,date]:
        bis = ab + timedelta(days=wd)
        body = dict(WEBLAW_BASE_JSON)
        body["userID"] = _rand_uid()
        body["sessionDuration"] = str(int(time.time()))
        body["metadataDateMap"] = {"rulingDate": {
            "from": ab.strftime("%Y-%m-%dT00:00:00.000Z"),
            "to": bis.strftime("%Y-%m-%dT23:59:59.999Z"),
        }}
        if off > 0: body["from"] = off
        resp = self.post(WEBLAW_DOCS_URL, headers=WEBLAW_HEADERS, json=body)
        data = resp.json()
        if data.get("status") != "success":
            logger.error(f"Weblaw API error: {json.dumps(data)[:500]}")
            return [], 0, False, bis
        r = data["data"]
        return r.get("documents",[]), r.get("totalNumberOfDocuments",0), r.get("hasMoreResults",False), bis

    def _wl_parse(self, doc: dict) -> dict|None:
        try:
            kw = doc.get("metadataKeywordTextMap",{})
            dt = doc.get("metadataDateMap",{})
            titles = kw.get("title",[])
            if not titles: return None
            nums = titles[0].split(", ")
            return {
                "docket_number": nums[0], "docket_numbers": nums,
                "decision_date": dt.get("rulingDate","")[:10] if "rulingDate" in dt else None,
                "publication_date": dt.get("publicationDate","")[:10] if "publicationDate" in dt else None,
                "headnote": doc.get("content",""),
                "chamber": kw.get("court",[""])[0] if "court" in kw else None,
                "doc_id": doc.get("leid",""),
                "pdf_url": f"{WEBLAW_PDF_URL}{doc.get('leid','')}?locale=de",
                "_source": "weblaw",
            }
        except Exception as e:
            logger.error(f"Weblaw parse error: {e}"); return None

    def _fetch_weblaw_content(self, leid: str) -> str:
        try:
            resp = self.get(f"{WEBLAW_CONTENT_URL}{leid}")
            if resp.status_code == 200 and len(resp.text) > 100: return resp.text
        except Exception as e:
            logger.debug(f"Weblaw content fetch failed for {leid}: {e}")
        return ""

    # ── Mode B: ICEfaces ─────────────────────────────────────
    def _discover_icefaces(self, since_date=None) -> Iterator[dict]:
        cy = date.today().year; sy = START_YEAR
        if since_date:
            if isinstance(since_date,str): since_date = parse_date(since_date) or date(sy,1,1)
            sy = max(since_date.year, sy)
        for year in range(cy, sy-1, -1):
            sess = self._ice_session(year)
            if not sess: continue
            js, ice = sess
            html = self._ice_search(js, ice, f"01.01.{year}", f"31.12.{year}")
            if not html: continue
            tz = RE_TREFFERZAHL.search(html)
            if not tz:
                if "Kein Suchtreffer" in html: logger.info(f"No results {year}")
                else: logger.warning(f"Cannot parse hit count {year}")
                continue
            total = int(tz.group(1).replace(",",""))
            pages = int(tz.group(5).replace(",",""))
            logger.info(f"BVGer ICEfaces {year}: {total} decisions, {pages} pages")
            for s in self._ice_parse(html):
                s["year"]=year; s["_source"]="icefaces"
                did = make_decision_id("bvger",s["docket_number"])
                if not self.state.is_known(did): yield s
            if pages > 1:
                logger.warning(f"BVGer {year}: {pages} pages, only page 1 scraped")

    def _ice_session(self, year: int) -> tuple[str,str]|None:
        try:
            r = self.get(f"{JP_SESSION_URL}&{year}")
            if r.status_code != 200 or len(r.content) < 148: return None
            m = RE_ICE_SESSION.search(r.text)
            if not m: return None
            ice = m.group(0)
            js = None
            for c in r.cookies:
                if c.name == "JSESSIONID": js = c.value; break
            if not js:
                for h in r.headers.get("Set-Cookie","").split(","):
                    m2 = RE_JSESSION.search(h)
                    if m2: js = m2.group(0); break
            if not js: return None
            return (js, ice)
        except Exception as e:
            logger.error(f"ICEfaces session error {year}: {e}"); return None

    def _ice_search(self, js: str, ice: str, ab: str, bis: str) -> str|None:
        rand = f"0.{random.randint(10**15,10**16-1)}"
        url = f"{JP_SUCH_URL}{js}"
        b1 = (f"ice.submit.partial=true&ice.event.target=form%3AcalFrom&"
              f"ice.event.captured=form%3AcalFrom&ice.event.type=onblur&"
              f"form%3A_idform%3AcalTosp=&form%3A_idform%3AcalFromsp=&"
              f"form%3A_idcl=&form%3Aform%3Atree_idtn=&form%3Aform%3Atree_idta=&"
              f"form%3AcalTo={bis}&form%3AcalFrom={ab}&form%3AsearchQuery=&"
              f"javax.faces.RenderKitId=&javax.faces.ViewState=1&"
              f"icefacesCssUpdates=&form=&"
              f"ice.session={ice}&ice.view=1&ice.focus=&rand={rand}")
        try: self.post(url, data=b1, headers=JP_HEADERS)
        except: return None
        rand2 = f"0.{random.randint(10**15,10**16-1)}"
        b2 = (f"ice.submit.partial=true&ice.event.target=form%3AsearchSubmitButton&"
              f"ice.event.captured=form%3AsearchSubmitButton&ice.event.type=onclick&"
              f"ice.event.alt=false&ice.event.ctrl=false&ice.event.shift=false&"
              f"ice.event.meta=false&ice.event.x=72&ice.event.y=252&"
              f"ice.event.left=false&ice.event.right=false&"
              f"form%3A_idform%3AcalTosp=&form%3A_idform%3AcalFromsp=&"
              f"form%3A_idcl=&form%3Aform%3Atree_idtn=&form%3Aform%3Atree_idta=&"
              f"form%3AcalTo={bis}&form%3AcalFrom={ab}&form%3AsearchQuery=&"
              f"javax.faces.RenderKitId=&javax.faces.ViewState=1&"
              f"icefacesCssUpdates=&form=&form%3AsearchSubmitButton=suchen&"
              f"ice.session={ice}&ice.view=1&"
              f"ice.focus=form%3AsearchSubmitButton&rand={rand2}")
        try:
            r = self.post(url, data=b2, headers=JP_HEADERS)
            return r.text
        except: return None

    def _ice_parse(self, html: str) -> list[dict]:
        stubs = []
        for m in RE_TREFFER.finditer(html):
            stubs.append({
                "docket_number": m.group("Num"),
                "decision_date": m.group("EDatum"),
                "chamber": m.group("VKammer"),
                "title": m.group("Titel"),
                "headnote_short": m.group("LeitsatzKurz"),
                "headnote": m.group("Leitsatz"),
                "pdf_url": f"{JP_HOST}{m.group('PDFUrl')}",
                "doc_id": m.group("DocId"),
                "source_url": JP_SESSION_URL,
            })
        return stubs


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scrape BVGer decisions")
    parser.add_argument("--since", type=str, help="Start date YYYY-MM-DD or year")
    parser.add_argument("--max", type=int, default=20)
    parser.add_argument("--verbose","-v", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    since = None
    if args.since:
        since = date(int(args.since),1,1) if len(args.since)==4 else date.fromisoformat(args.since)
    scraper = BVGerScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    print(f"Scraped {len(decisions)} BVGer decisions")
