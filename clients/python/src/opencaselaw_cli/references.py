"""How lawyers write references, parsed for identity checks.

The parser never produces a citation string for display: printed citations
come from the service (R1). It only tells the resolver which label to query
and which parts of a reference must be carried by the decision the service
returns: the collection label (BGE/ATF/DTF), a docket, the court, a date. An
inline pinpoint (", E. 2.3", "consid. 3b") is separated out so that it can be
verified instead of silently folded away.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

_MONTHS = {
    "januar": 1, "jänner": 1, "februar": 2, "märz": 3, "maerz": 3, "april": 4, "mai": 5, "juni": 6,
    "juli": 7, "august": 8, "september": 9, "oktober": 10, "november": 11, "dezember": 12,
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "juin": 6, "juillet": 7,
    "août": 8, "aout": 8, "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4, "maggio": 5, "giugno": 6, "luglio": 7,
    "agosto": 8, "settembre": 9, "ottobre": 10, "dicembre": 12,
    "january": 1, "february": 2, "march": 3, "may": 5, "june": 6, "july": 7, "october": 10, "december": 12,
}

# An Erwägung marker must start a token: the "E." inside WBE.2026.33 is part
# of a docket, "de" in "Cour de justice" is a word.
_MARK = r"(?:[Ee]\.|[Ee]rw\.|[Ee]rwägung|[Ee]rwaegung|[Cc]onsid\.|[Cc]onsid|[Cc]ons\.|[Cc]onsiderando|c\.)"
_PINPOINT_INLINE = re.compile(
    r"(?:(?<=[\s,;(])|^)" + _MARK + r"\s*(?P<pin>\d+(?:\.\d+)*(?:[a-z]{1,2})?(?:/[a-z]{1,2})*)"
    r"(?:\s*(?:ff?\.|ss?\.))?")
_PAGE = re.compile(r"(?:(?<=[\s,;(])|^)(?:S\.|SS\.|p\.|pp\.|pag\.|pagg\.)\s*\d{1,5}(?:\s*(?:ff?\.|ss?\.))?")
_FF = re.compile(r"(?:(?<=[\s,;])|^)(?:ff?|ss?)\.?(?=\s|$)")
_PINPOINT_VALID = re.compile(r"^\d+(?:\.\d+)*(?:[a-z]{1,2})?(?:/[a-z]{1,2})*$")

_DATE_WORD = re.compile(r"(?<![\d/.])(?P<d>\d{1,2})(?:\.|er|re|º|°)?\s+(?P<m>[A-Za-zÀ-ÿ]+)\s+(?P<y>\d{4})(?![\d/])")
_DATE_NUM = re.compile(r"(?<![\d/.])(?P<d>\d{1,2})\.(?P<m>\d{1,2})\.(?P<y>\d{4})(?![\d/])")

_BGE = re.compile(r"(?<![A-Za-z0-9])(?P<coll>BGE|ATF|DTF)\s*(?P<vol>\d{1,3})\s+(?P<part>Ia|Ib|III|II|IV|I|V)\s+(?P<page>\d{1,4})(?![0-9])")
_BGE_BARE = re.compile(r"^(?P<vol>\d{1,3})\s+(?P<part>Ia|Ib|III|II|IV|I|V)\s+(?P<page>\d{1,4})$")

# Docket shapes, most specific first. Federal files (4A_747/2012) are also
# written with a space or, before 2007, a dot (4C.230/2006); the corpus stores
# all three, so the federal shape yields query variants.
_FEDERAL = re.compile(r"(?<![A-Za-z0-9])(?P<ch>\d[A-Z]{1,2})[ _.](?P<n>\d{1,5})/(?P<y>\d{4})(?![0-9])")
_DOCKET_SHAPES = (
    _FEDERAL,
    re.compile(r"(?<![A-Za-z0-9])([A-Z]{1,2}-\d{1,5}/\d{4})(?![0-9])"),                       # BVGer A-4843/2020
    re.compile(r"(?<![A-Za-z0-9])([A-Z]{1,6}\.\d{4}\.\d{1,6}(?:-[A-Z0-9]+)?)(?![A-Za-z0-9])"),  # AG/ZH admin WBE.2026.33, VB.2023.00538
    re.compile(r"(?<![A-Za-z0-9])([A-Z]{2}\d{6}(?:-[A-Z](?:_U\d+)?)?)(?![A-Za-z0-9])"),         # ZH courts LA210005, NG190020
    re.compile(r"(?<![A-Za-z0-9])([A-Za-zÀ-ÿ]{1,8} ?/ ?\d{1,6} ?/ ?\d{1,6})(?![0-9])"),          # GE/VD ACJC/123/2024, C/11532/2013, HC / 2020 / 38
    re.compile(r"(?<![A-Za-z0-9])([A-Z]{1,3} \d{4}/\d{1,4}|\d{3} \d{2} \d{1,4}|ZK \d{2} \d{1,4})(?![0-9])"),  # SG K 2015/3, BL 810 16 9, BE ZK 20 1
)

_FEDERAL_COURT = re.compile(r"(?<![A-Za-z_])(?:BGer|BGE|ATF|DTF|TF|Bundesgericht(?:s|es)?|Tribunal f[ée]d[ée]ral|Tribunale federale|Federal Supreme Court)(?![A-Za-z_])")
_BVGER_COURT = re.compile(r"(?<![A-Za-z_])(?:BVGer|BVGE|TAF|Bundesverwaltungsgericht(?:s|es)?|Tribunal administratif f[ée]d[ée]ral|Tribunale amministrativo federale)(?![A-Za-z_])")
_BSTGER_COURT = re.compile(r"(?<![A-Za-z_])(?:BStGer|TPF|Bundesstrafgericht(?:s|es)?|Tribunal p[ée]nal f[ée]d[ée]ral|Tribunale penale federale)(?![A-Za-z_])")
_CANTON_CODE = re.compile(r"(?<![A-Za-z_])(AG|AI|AR|BE|BL|BS|FR|GE|GL|GR|JU|LU|NE|NW|OW|SG|SH|SO|SZ|TG|TI|UR|VD|VS|ZG|ZH)(?![A-Za-z_])")
_CANTON_NAMES = {
    "zürich": "ZH", "zurich": "ZH", "zurigo": "ZH", "bern": "BE", "berne": "BE", "berna": "BE", "genève": "GE", "geneve": "GE",
    "genf": "GE", "geneva": "GE", "ginevra": "GE", "vaud": "VD", "vaudois": "VD", "vaudoise": "VD", "waadt": "VD",
    "aargau": "AG", "argovie": "AG", "basel-stadt": "BS", "bâle-ville": "BS", "basel-landschaft": "BL", "baselland": "BL",
    "bâle-campagne": "BL", "luzern": "LU", "lucerne": "LU", "lucerna": "LU", "st. gallen": "SG", "st.gallen": "SG",
    "saint-gall": "SG", "san gallo": "SG", "tessin": "TI", "ticino": "TI", "tessin.": "TI", "wallis": "VS", "valais": "VS",
    "vallese": "VS", "neuchâtel": "NE", "neuchatel": "NE", "neuenburg": "NE", "fribourg": "FR", "freiburg": "FR",
    "friburgo": "FR", "solothurn": "SO", "soleure": "SO", "thurgau": "TG", "thurgovie": "TG", "graubünden": "GR",
    "grisons": "GR", "grigioni": "GR", "schaffhausen": "SH", "schaffhouse": "SH", "zug": "ZG", "zoug": "ZG",
    "schwyz": "SZ", "jura": "JU", "glarus": "GL", "glaris": "GL", "uri": "UR", "nidwalden": "NW", "obwalden": "OW",
}


@dataclass
class Reference:
    text: str
    core: str
    pinpoint: str | None = None
    pages: list = field(default_factory=list)
    date: str | None = None
    bge_label: str | None = None
    dockets: list = field(default_factory=list)
    courts: set = field(default_factory=set)
    canton: str | None = None
    court_words: bool = False

    @property
    def long_form(self) -> bool:
        """More than a bare label: court words, a date, pages or an inline pinpoint."""
        return bool(self.date or self.pages or self.pinpoint or self.court_words or self.canton
                    or (self.dockets and label_key(self.core) != label_key(self.dockets[0]))
                    or (self.bge_label and label_key(self.core) != label_key(self.bge_label)))

    def queries(self) -> list[str]:
        """Labels to ask the service for, most specific first, without duplicates."""
        out = []
        if self.bge_label:
            out.append(self.bge_label)
        elif self.dockets:
            out.extend(docket_variants(self.dockets[0]))
        if self.core and self.core not in out and not (self.bge_label or self.dockets):
            out.append(self.core)
        return out

    def in_scope(self, candidate: dict) -> bool:
        """Whether a candidate decision is at a court the reference names (unknown court: kept)."""
        court = str(candidate.get("court") or "")
        canton = str(candidate.get("canton") or "")
        if self.courts and court:
            return court in self.courts
        if self.canton and (court or canton):
            return canton.upper() == self.canton or court.lower().startswith(self.canton.lower() + "_")
        return True


def normalise_pinpoint(value) -> str | None:
    """"E. 2.3", "consid. 3b", "3c/aa" -> "2.3", "3b", "3c/aa"; None for empty; ValueError otherwise."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("A pinpoint must be a string such as 2.3 or 3b")
    text = re.sub(r"^\s*" + _MARK + r"\s*", "", value.strip()).strip().rstrip(".)").strip()
    if not text:
        return None
    text = re.sub(r"\s+", "", text)
    text = text[:1] + text[1:].lower() if text else text
    if not _PINPOINT_VALID.match(text.lower()):
        raise ValueError(f"{value!r} is not an Erwägung number such as 2.3, 3b or 3c/aa")
    return text.lower()


def pinpoint_parent(pinpoint: str) -> str | None:
    """The indexed level above a lettered or slashed pinpoint: 3c/aa -> 3, 2a -> 2, 4.2.1 -> None."""
    base = pinpoint.split("/", 1)[0]
    stripped = re.sub(r"[a-z]+$", "", base)
    return stripped if stripped and stripped != pinpoint else None


def _unwrap(text: str) -> str:
    text = text.strip()
    while len(text) > 2 and text[0] in "([«\"'" and text[-1] in ")]»\"'":
        text = text[1:-1].strip()
    return text


def _iso_date(match) -> str | None:
    try:
        day, year = int(match.group("d")), int(match.group("y"))
        month = match.group("m")
        month = int(month) if month.isdigit() else _MONTHS.get(month.casefold().rstrip("."))
        if not month or not 1 <= day <= 31 or not 1 <= month <= 12:
            return None
        return f"{year:04d}-{month:02d}-{day:02d}"
    except (TypeError, ValueError):
        return None


def parse_reference(text: str) -> Reference:
    core = _unwrap(text or "")
    pinpoint = None
    match = _PINPOINT_INLINE.search(core)
    if match:
        pinpoint = match.group("pin").lower()
        core = (core[:match.start()] + " " + core[match.end():]).strip()
    pages = [m.group(0).strip(" ,;(") for m in _PAGE.finditer(core)]
    core = _PAGE.sub(" ", core)
    core = _FF.sub(" ", core)
    core = re.sub(r"\s+", " ", core).strip(" ,;:(").rstrip(" .,;:")
    date = None
    for pattern in (_DATE_WORD, _DATE_NUM):
        for m in pattern.finditer(core):
            date = _iso_date(m)
            if date:
                break
        if date:
            break
    bge = _BGE.search(core) or _BGE_BARE.match(core)
    bge_label = f"BGE {bge.group('vol')} {bge.group('part')} {bge.group('page')}" if bge else None
    dockets: list[str] = []
    scan = core if not bge else core.replace(bge.group(0), " ")
    for shape in _DOCKET_SHAPES:
        for m in shape.finditer(scan):
            found = m.group(0) if shape is _FEDERAL else m.group(1)
            found = re.sub(r"\s*/\s*", "/", found.strip())
            if found and found not in dockets and not any(found in d for d in dockets):
                dockets.append(found)
    courts = set()
    court_words = False
    if bge_label:
        courts.add("bge")
    if _FEDERAL_COURT.search(scan if bge_label else core):
        courts.update({"bger", "bge"})
        court_words = True
    if _BVGER_COURT.search(core):
        courts.add("bvger")
        court_words = True
    if _BSTGER_COURT.search(core):
        courts.add("bstger")
        court_words = True
    canton = None
    code = _CANTON_CODE.search(core)
    if code:
        canton = code.group(1)
    else:
        lowered = core.casefold()
        for name, value in _CANTON_NAMES.items():
            if re.search(r"(?<![a-zà-ÿ])" + re.escape(name) + r"(?![a-zà-ÿ])", lowered):
                canton = value
                break
    return Reference(text=text or "", core=core, pinpoint=pinpoint, pages=pages, date=date,
                     bge_label=bge_label, dockets=dockets, courts=courts, canton=canton, court_words=court_words)


def docket_variants(docket: str) -> list[str]:
    """Query forms of a federal docket: the underscore form (the service folds it to a
    stored space itself), then the pre-2007 dot form, then a written space form."""
    m = _FEDERAL.fullmatch(docket.strip())
    if not m:
        return [docket.strip()]
    ch, n, y = m.group("ch"), m.group("n"), m.group("y")
    out = [f"{ch}_{n}/{y}", f"{ch}.{n}/{y}"]
    if docket.strip() not in out:
        out.append(docket.strip())
    return out


def fold_docket(text: str) -> str:
    """Comparison form of a docket or of a reference that contains one."""
    folded = (text or "").casefold()
    folded = re.sub(r"(\d[a-z]{1,2})[ _.](\d{1,5}/\d{4})", r"\1_\2", folded)
    folded = re.sub(r"\s*/\s*", "/", folded)
    return re.sub(r"\s+", " ", folded).strip()


def docket_in_reference(reference: str, docket) -> bool:
    """Whether the decision's own docket label appears whole in the reference."""
    if not isinstance(docket, str) or not docket.strip():
        return False
    haystack = fold_docket(reference)
    needle = fold_docket(docket)
    return re.search(r"(?<![a-z0-9])" + re.escape(needle) + r"(?![a-z0-9])", haystack) is not None


def label_key(value) -> str | None:
    """Comparison key for source-supplied labels; never used to generate a citation.

    Folds case, whitespace, the federal docket separators (4A_747/2012 =
    4A 747/2012 = 4A.747/2012), wrapping punctuation, an inline pinpoint and
    page references, and the French/Italian collection labels (ATF/DTF = BGE).
    """
    if not isinstance(value, str):
        return None
    parsed = parse_reference(value)
    key = fold_docket(parsed.core)
    key = re.sub(r"[\s_]+", "", key).rstrip(".,;:")
    if key.startswith(("atf", "dtf")) and len(key) > 3 and key[3].isdigit():
        key = "bge" + key[3:]
    elif _BGE_BARE.match(parsed.core):
        key = "bge" + key
    return key
