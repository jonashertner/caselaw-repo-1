"""
Tribuna Platform Base — GWT-RPC Cantonal Court Scraper
=======================================================
Base class for Tribuna VTPlus cantonal court portals.

Architecture (reverse-engineered Feb 2026):
1. GET base URL → set session cookies
2. POST readConfigFile() → get DLAConfig with encrypted credentials
3. POST getBerechtigungen("","") → get permissions
4. POST search(...) → paginated results (20 per page)
   - Old Tribuna (GR, ZG): 46 params (20 search fields)
   - New Tribuna (FR, BE VG): 47 params (21 search fields)
5. GET ServletDownload/{docket}_{enc_path}?path=...&pathIsEncrypted=1 → PDF

To implement a new Tribuna scraper:
    class GRGerichteScraper(TribunaBaseScraper):
        CANTON = "GR"
        COURT_CODE_STR = "gr_gerichte"
        BASE_URL = "https://entscheidsuche.gr.ch"
        LOCALE = "de"

Covered portals (as of Feb 2026):
  - GR: https://entscheidsuche.gr.ch (old Tribuna version)
  - ZG VGR: https://verwaltungsgericht.zg.ch
  - BE ZSG: https://www.zsg-entscheide.apps.be.ch/tribunapublikation
  - BE VGR: https://www.vg-urteile.apps.be.ch/tribunapublikation (new Tribuna version)
  - FR: https://publicationtc.fr.ch (new Tribuna version)

Note: Different portals run different Tribuna compilations. The GWT permutation
is auto-discovered from each portal's tribunavtplus.nocache.js at runtime.
"""
from __future__ import annotations

import logging
import re
from datetime import date
from typing import Iterator

from base_scraper import BaseScraper
from models import Decision, detect_language, extract_citations, make_decision_id, parse_date

logger = logging.getLogger(__name__)

# GWT-RPC serialization policy hashes (shared across Tribuna VTPlus versions)
_CONFIG_HASH = "7225438C30B96853F589E2336CAF98F1"
_LOADTABLE_HASH = "CAC80118FB77794F1FDFC1B51371CC63"
# Fallback permutation (for portals where auto-discovery fails)
_GWT_PERMUTATION_FALLBACK = "C91406E3C064F0230BE12F3EF5EDF1D6"

# Regex patterns for parsing search responses
_RE_TOTAL = re.compile(r"^//OK\[(\d+)")
_RE_DOC_ID = re.compile(r"^[0-9a-f]{32}$")
_RE_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_RE_DOCKET = re.compile(r"^[A-Z0-9]{1,4}\s+\d{4}\s+\d+$")
_RE_ENC_PATH = re.compile(r"^[0-9a-f]{60,}$")
_RE_HEX = re.compile(r"^[0-9a-f]{60,}$")

# Column definitions for the search request (field key, display label)
_COLUMNS = [
    ("decisionDate", "Entscheiddatum"),
    ("dossierNumber", "Dossier"),
    ("classification", "Zusatzeigenschaft"),
    ("indexCode", "Quelle"),
    ("dossierObject", "Betreff"),
    ("law", "Rechtsgebiet"),
    ("shortText", "Vorschautext"),
    ("department", "Abteilung"),
    ("createDate", "Erfasst am"),
    ("creater", "Ersteller"),
    ("judge", "Richter"),
    ("executiontype", "Erledigungsart"),
    ("legalDate", "Rechtskraftdatum"),
    ("objecttype", "Objekttyp"),
    ("typist", "Schreiber"),
    ("description", "Beschreibung"),
    ("reference", "Referenz"),
    ("relevance", None),  # no label for last column
]


class TribunaBaseScraper(BaseScraper):
    """Base scraper for Tribuna VTPlus GWT-RPC court portals."""

    CANTON: str = ""
    COURT_CODE_STR: str = ""
    BASE_URL: str = ""           # e.g., "https://entscheidsuche.gr.ch"
    COURT_FILTER: str = ""       # e.g., "OG" — single court filter
    COURT_FILTERS: list[str] = []  # Multiple courts — iterates over each
    LOCALE: str = "de"           # "de", "fr", "it"
    REQUEST_DELAY: float = 2.5
    MAX_PAGES: int = 1000        # 20 results/page = 20,000 max
    PAGE_SIZE: int = 20          # Fixed by Tribuna server
    # Number of search field strings in the GWT-RPC search method.
    # Old Tribuna (GR, ZG): 20 fields → 46-param search()
    # New Tribuna (FR, BE VG): 21 fields → 47-param search()
    SEARCH_FIELD_COUNT: int = 20

    # Overridable: the tribunavtplus subpath (usually "tribunavtplus")
    TRIBUNA_PATH: str = "tribunavtplus"

    @property
    def court_code(self) -> str:
        return self.COURT_CODE_STR

    @property
    def _gwt_base(self) -> str:
        return f"{self.BASE_URL}/{self.TRIBUNA_PATH}"

    def _discover_permutation(self) -> str:
        """Auto-discover GWT permutation from the portal's nocache.js."""
        try:
            r = self.get(f"{self._gwt_base}/tribunavtplus.nocache.js")
            # Extract 32-char uppercase hex hashes (GWT permutation candidates)
            hashes = re.findall(r"[A-F0-9]{32}", r.text)
            if hashes:
                perm = hashes[0]
                logger.info(f"[{self.court_code}] Auto-discovered GWT permutation: {perm}")
                return perm
        except Exception as e:
            logger.debug(f"[{self.court_code}] Permutation discovery failed: {e}")
        logger.warning(f"[{self.court_code}] Using fallback GWT permutation")
        return _GWT_PERMUTATION_FALLBACK

    def _gwt_headers_with(self, permutation: str) -> dict:
        return {
            "Content-Type": "text/x-gwt-rpc; charset=utf-8",
            "X-GWT-Permutation": permutation,
            "X-GWT-Module-Base": f"{self._gwt_base}/",
        }

    @property
    def _gwt_headers(self) -> dict:
        # Use cached permutation if available, otherwise fallback
        perm = getattr(self, "_cached_permutation", _GWT_PERMUTATION_FALLBACK)
        return self._gwt_headers_with(perm)

    def _init_session(self) -> str:
        """Initialize session: GET base, discover permutation, readConfig, getBerechtigungen.

        Returns the encrypted credential string from config.
        """
        # Step 1: Cookie init
        try:
            self.get(f"{self.BASE_URL}/")
        except Exception:
            pass

        # Step 1b: Discover the correct GWT permutation for this portal
        self._cached_permutation = self._discover_permutation()

        # Step 2: readConfigFile
        config_body = (
            f"7|0|4|{self._gwt_base}/|{_CONFIG_HASH}|"
            "tribunavtplus.client.zugriff.ConfigService|readConfigFile|"
            "1|2|3|4|0|"
        )
        resp = self.post(
            f"{self._gwt_base}/config",
            data=config_body,
            headers=self._gwt_headers,
        )

        # Extract the ~96-char encrypted credential from config response
        config_strings = re.findall(r'"([^"]*)"', resp.text)
        hex_strings = [s for s in config_strings if _RE_HEX.match(s)]
        # The credential is the last hex string around 96 chars (varies per portal)
        # Sort by length and pick the one closest to 96 chars
        credential = ""
        if hex_strings:
            # Prefer ~96 char string, fallback to last
            candidates = sorted(hex_strings, key=lambda s: abs(len(s) - 96))
            credential = candidates[0]
        logger.info(f"[{self.court_code}] Config loaded, credential len={len(credential)}")

        # Step 3: getBerechtigungen
        berech_body = (
            f"7|0|6|{self._gwt_base}/|{_LOADTABLE_HASH}|"
            "tribunavtplus.client.zugriff.LoadTableService|getBerechtigungen|"
            "java.lang.String/2004016611||"
            "1|2|3|4|2|5|5|6|6|"
        )
        self.post(
            f"{self._gwt_base}/loadTable",
            data=berech_body,
            headers=self._gwt_headers,
        )

        return credential

    def _build_search_body(self, credential: str, page: int, total: int | None,
                           court_filter: str | None = None) -> str:
        """Build the GWT-RPC search request body.

        Args:
            credential: Encrypted credential string from config.
            page: Page number (0-indexed).
            total: Total results from previous response (None for first page).
            court_filter: Court filter code (defaults to self.COURT_FILTER).
        """
        base = self._gwt_base
        court = court_filter if court_filter is not None else self.COURT_FILTER
        locale = self.LOCALE

        # Build string table
        strings = [
            f"{base}/",                                    # 0
            _LOADTABLE_HASH,                               # 1
            "tribunavtplus.client.zugriff.LoadTableService",  # 2
            "search",                                      # 3
            "java.lang.String/2004016611",                 # 4
            "java.util.ArrayList/4159755760",              # 5
            "Z",                                           # 6
            "I",                                           # 7
            "java.lang.Integer/3438268394",                # 8
            "java.util.Map",                               # 9
            "",                                            # 10 (empty)
            "0",                                           # 11 (always "0" — page is a literal in values)
            court,                                         # 12
            "0;false",                                     # 13
            "5;true",                                      # 14
            credential,                                    # 15
            "1",                                           # 16
            "java.util.HashMap/1797211028",                # 17
        ]
        # Add column pairs
        for key, label in _COLUMNS:
            strings.append(key)
            if label:
                strings.append(label)
        strings.append(locale)

        num_strings = len(strings)

        # Build string table section (pipe-delimited)
        st = "|".join(strings)

        # Type descriptors — dynamic based on SEARCH_FIELD_COUNT
        # Params: String, String, ArrayList, Z, ArrayList,
        #         N×String (search fields), 3×I, 2×String, 3×Integer,
        #         4×String, Z, Map, 7×String
        # N=20 → 46 params (old Tribuna), N=21 → 47 params (new Tribuna)
        nf = self.SEARCH_FIELD_COUNT
        num_params = 5 + nf + 21
        field_types = "|".join(["5"] * nf)
        types = (
            f"5|5|6|7|6|"           # params 1-5
            f"{field_types}|"        # params 6-(5+nf): search field strings
            f"8|8|8|5|5|"           # I×3, String×2
            f"9|9|9|5|5|5|5|"       # Integer×3, String×4
            f"7|10|"                # Z, Map
            f"5|5|5|5|5|5|5"        # String×7
        )

        # Value section — parameterized for page and total
        # String refs are 1-based: ref N → strings[N-1]
        # str[10] = "" → ref 11, str[11] = "0" → ref 12
        # Page number is NOT in the string table — it's a literal in values (20|{page}|-1|)
        empty_ref = 11  # 1-based ref to "" (str[10])
        zero_ref = 12   # 1-based ref to "0" (str[11], always "0")

        # N empty strings for search field values (20 or 21)
        empties = "|".join(["11"] * nf)

        # Column definition map refs (HashMap<String,String>: key→label)
        col_refs = []
        idx = 19  # 1-based index starting after "java.util.HashMap/1797211028" (str[17] = ref 18)
        for key, label in _COLUMNS:
            col_refs.append(f"5|{idx}")  # key
            idx += 1
            if label:
                col_refs.append(f"5|{idx}")  # label value
                idx += 1
            else:
                col_refs.append(f"5|{empty_ref}")  # empty string value
        col_section = "|".join(col_refs)

        locale_ref = idx  # Last string = locale

        if total is None:
            # Page 0: no total count
            values = (
                f"{empty_ref}|{zero_ref}|6|0|0|6|1|5|13|"
                f"{empties}|"
                f"20|{page}|-1|"
                f"{empty_ref}|{empty_ref}|"
                f"0|9|0|9|-1|"
                f"14|15|16|17|0|18|18|"
                f"{col_section}|"
                f"{empty_ref}|{locale_ref}|"
                f"{empty_ref}|{empty_ref}|{zero_ref}|{zero_ref}|0|"
            )
        else:
            # Page N>0: include total count
            values = (
                f"{empty_ref}|{zero_ref}|6|0|0|6|1|5|13|"
                f"{empties}|"
                f"20|{page}|-1|"
                f"{empty_ref}|{empty_ref}|"
                f"9|{total}|9|0|9|-1|"
                f"14|15|16|17|0|18|18|"
                f"{col_section}|"
                f"{empty_ref}|{locale_ref}|"
                f"{empty_ref}|{empty_ref}|{zero_ref}|{zero_ref}|0|"
            )

        return f"7|0|{num_strings}|{st}|1|2|3|4|{num_params}|{types}|{values}"

    def _parse_search_response(self, text: str) -> tuple[int, list[dict]]:
        """Parse GWT-RPC search response.

        Returns (total_count, list_of_stubs).
        Each stub has: doc_id, docket_number, decision_date, enc_path, title.
        """
        if not text.startswith("//OK"):
            logger.warning(f"[{self.court_code}] Bad response: {text[:200]}")
            return 0, []

        m = _RE_TOTAL.match(text)
        total = int(m.group(1)) if m else 0

        # Extract all strings from the response
        all_strings = re.findall(r'"([^"]*)"', text)

        # Group fields into decisions
        # Each decision in the response has: doc_id (32-char hex), docket_number,
        # decision_date, enc_path (long hex), title/subject, court, etc.
        decisions = []
        doc_ids = []
        dockets = []
        dates_list = []
        enc_paths = []
        titles = []

        for s in all_strings:
            if _RE_DOC_ID.match(s):
                doc_ids.append(s)
            elif _RE_DOCKET.match(s):
                dockets.append(s)
            elif _RE_DATE.match(s):
                dates_list.append(s)
            elif _RE_ENC_PATH.match(s):
                enc_paths.append(s)

        # Find titles: strings that are >10 chars, not hex, not dates, not types
        skip_prefixes = ("java.", "tribuna", "[B/", "[L", "com.", "viewtype",
                         "reportpath", "reportexport", "reporttitle", "reportname")
        for s in all_strings:
            if (len(s) > 10
                and not _RE_DOC_ID.match(s)
                and not _RE_DATE.match(s)
                and not _RE_DOCKET.match(s)
                and not _RE_ENC_PATH.match(s)
                and not _RE_HEX.match(s)
                and not any(s.startswith(p) for p in skip_prefixes)):
                titles.append(s)

        # Match doc_ids with dockets and dates by position proximity
        # The response lists entries sequentially — doc_ids, dockets, dates appear
        # in the same order and can be zipped
        n = min(len(doc_ids), len(dockets))
        for i in range(n):
            stub = {
                "doc_id": doc_ids[i],
                "docket_number": dockets[i],
                "decision_date": dates_list[i] if i < len(dates_list) else "",
                "enc_path": enc_paths[i] if i < len(enc_paths) else "",
                "title": titles[i] if i < len(titles) else "",
            }
            decisions.append(stub)

        return total, decisions

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Discover decisions via Tribuna GWT-RPC search."""
        credential = self._init_session()
        if not credential:
            logger.error(f"[{self.court_code}] Failed to get credential from config")
            return

        # Determine which court filters to iterate over
        filters = self.COURT_FILTERS if self.COURT_FILTERS else [self.COURT_FILTER]

        for court_filter in filters:
            yield from self._search_court(credential, court_filter, since_date)

    def _search_court(self, credential: str, court_filter: str, since_date=None) -> Iterator[dict]:
        """Search a single court filter and yield decision stubs."""
        total = None
        for page in range(self.MAX_PAGES):
            try:
                body = self._build_search_body(credential, page, total, court_filter)
                resp = self.post(
                    f"{self._gwt_base}/loadTable",
                    data=body,
                    headers=self._gwt_headers,
                )
            except Exception as e:
                logger.error(f"[{self.court_code}] Search page {page} failed: {e}")
                break

            page_total, decisions = self._parse_search_response(resp.text)

            if total is None:
                total = page_total
                logger.info(f"[{self.court_code}] Total results for '{court_filter}': {total}")

            if not decisions:
                logger.info(f"[{self.court_code}] No more results at page {page}")
                break

            for stub in decisions:
                if since_date and stub.get("decision_date"):
                    d = parse_date(stub["decision_date"])
                    if d and d < since_date:
                        continue

                did = make_decision_id(self.court_code, stub["docket_number"])
                if self.state.is_known(did):
                    continue

                stub["decision_id"] = did
                yield stub

            # Check if we've exhausted all pages
            if total and (page + 1) * self.PAGE_SIZE >= total:
                logger.info(f"[{self.court_code}] All {total} results covered in {page+1} pages")
                break

    def _build_download_url(self, stub: dict) -> str:
        """Build the PDF download URL from stub data."""
        docket_url = stub["docket_number"].replace(" ", "_")
        enc_path = stub.get("enc_path", "")
        if not enc_path:
            return ""
        return (
            f"{self._gwt_base}/ServletDownload/{docket_url}_{enc_path}"
            f"?path={enc_path}&pathIsEncrypted=1&dossiernummer={docket_url}"
        )

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch the PDF and extract text for a decision."""
        url = self._build_download_url(stub)
        if not url:
            logger.warning(f"[{self.court_code}] No download URL for {stub['docket_number']}")
            return None

        try:
            resp = self.get(url)
            ct = resp.headers.get("Content-Type", "")
            if "pdf" in ct.lower() and resp.content[:4] == b"%PDF":
                text = self._pdf_text(resp.content)
            else:
                logger.warning(
                    f"[{self.court_code}] Non-PDF response for {stub['docket_number']}: {ct}"
                )
                return None

            text = self.clean_text(text)
            if len(text) < 50:
                logger.warning(f"[{self.court_code}] Too short text for {stub['docket_number']}")
                return None

            dd = parse_date(stub.get("decision_date", ""))
            if not dd:
                logger.warning(f"[{self.court_code}] No date for {stub['docket_number']}")
            return Decision(
                decision_id=stub["decision_id"],
                court=self.court_code,
                canton=self.CANTON,
                docket_number=stub["docket_number"],
                decision_date=dd,
                language=detect_language(text),
                title=stub.get("title"),
                full_text=text,
                source_url=url,
                cited_decisions=extract_citations(text),
            )
        except Exception as e:
            logger.error(f"[{self.court_code}] Fetch error {stub['docket_number']}: {e}")
            return None

    @staticmethod
    def _pdf_text(data: bytes) -> str:
        """Extract text from PDF bytes."""
        try:
            import fitz
            doc = fitz.open(stream=data, filetype="pdf")
            return "\n\n".join(p.get_text() for p in doc)
        except ImportError:
            pass
        try:
            from pdfminer.high_level import extract_text
            from io import BytesIO
            return extract_text(BytesIO(data))
        except ImportError:
            pass
        return ""
