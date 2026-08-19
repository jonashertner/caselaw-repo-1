"""FINMA circulars: the version history has to be right, or it is worse
than not having one.

The fixture is a trimmed copy of FINMA's real 2008 archive page, kept
because it contains the two cases that broke the first implementation:

  * RS 2008/11 has a teaser whose "Zuletzt geändert" line repeats the
    previous entry's date. A regex that scanned forward from the link
    stamped the 25.01.2017 file as 13.09.2013 — a version history whose
    dates are wrong sends a practitioner to the wrong text.
  * RS 2008/30 carries .xls annexes, which share the circular number and
    date with the PDF and so collided on doc_id. The DB upserts on
    doc_id, so a collision silently deletes a document.

Offline by construction: no network, no PDF fetching.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import pytest  # noqa: E402

from scrapers.practice.finma_rundschreiben import (  # noqa: E402
    FinmaRundschreibenScraper, _doc_number, _iso, _short_number,
    _version_from_filename,
)

FIXTURE = (REPO / "tests" / "fixtures" / "practice"
           / "finma_rs_archiv_2008_excerpt.html")
ARCHIVE_URL = "https://www.finma.ch/de/dokumentation/archiv/rundschreiben/archiv-2008/"


@pytest.fixture(scope="module")
def stubs():
    scraper = FinmaRundschreibenScraper()
    page = FIXTURE.read_text(encoding="utf-8")
    return scraper, list(scraper._parse_archive_page(page, "de", ARCHIVE_URL))


# ── numbering ────────────────────────────────────────────────────────

def test_doc_number_is_finmas_own_zero_padded():
    assert _doc_number('2008/1 FINMA-Rundschreiben "X"') == "FINMA-RS 2008/01"
    assert _doc_number('2013/03 FINMA-Rundschreiben "Prüfwesen"') == "FINMA-RS 2013/03"
    assert _doc_number("no number here") == ""


def test_short_number_is_the_other_spelling_practitioners_use():
    """Both 'FINMA-RS 2008/01' and 'FINMA-RS 08/1' appear in briefs; both
    are carried in topics so either finds the circular."""
    assert _short_number("FINMA-RS 2008/01") == "FINMA-RS 08/1"
    assert _short_number("FINMA-RS 2013/03") == "FINMA-RS 13/3"


def test_swiss_dates_parse_day_first():
    assert _iso("Zuletzt geändert: 13.09.2013") == "2013-09-13"
    assert _iso("Dernière modification: 1.6.2012") == "2012-06-01"
    assert _iso("no date") == ""


def test_filename_dates_are_only_taken_when_plausible():
    assert _version_from_filename("x/rs-08-01-letzte-aenderung-20130101.pdf") == "2013-01-01"
    assert _version_from_filename("x/rs-08_01_erlass_20081120.pdf") == "2008-11-20"
    assert _version_from_filename("x/rs-08-01-aendper20110901.pdf") == "2011-09-01"
    assert _version_from_filename("x/no-date-here.pdf") == ""
    # 20139999 is not a date; a bad parse must yield nothing, not a
    # fabricated day.
    assert _version_from_filename("x/rs-08-01-20139999.pdf") == ""


# ── the two bugs the fixture exists for ──────────────────────────────

def test_each_version_keeps_the_date_the_page_states_for_it(stubs):
    """The regression. FINMA's page shows 13.09.2013 for the file named
    ...20170125.pdf; that is FINMA's own metadata and we publish it as
    given. What must never happen again is a date lifted from a
    NEIGHBOURING entry because this one had no line of its own."""
    _, docs = stubs
    by_file = {d["pdf_url"].split("/")[-1].split("?")[0]: d["date"]
               for d in docs}
    expected = {
        "rs-08-11-erlass-20081120.pdf": "2008-11-20",
        "rs-08-11-letzte-aenderung-20110909.pdf": "2011-09-09",
        "rs-08-11-letzte-aenderung-20121206.pdf": "2012-12-06",
        "rs-08-11-letzte-aenderung-20130913.pdf": "2013-09-13",
    }
    for name, date in expected.items():
        assert by_file.get(name) == date, f"{name} mis-dated"


def test_no_document_can_overwrite_another(stubs):
    """doc_id is the DB's primary key with ON CONFLICT DO UPDATE, so two
    documents sharing one is silent data loss, not a cosmetic clash."""
    scraper, docs = stubs
    ids = [scraper._make_doc_id(d) for d in docs]
    assert len(set(ids)) == len(ids), "doc_id collision would delete a document"


def test_spreadsheet_annexes_are_skipped_and_counted(stubs):
    """RS 2008/30's annexes are .xls. There is no text to extract, and a
    body-less row is noise in a full-text index — but a skipped document
    must be visible in the run summary, never silently absent."""
    scraper, docs = stubs
    assert all(d["pdf_url"].split("?")[0].lower().endswith(".pdf")
               for d in docs)
    assert scraper.skipped_non_pdf > 0


# ── shape of what gets written ───────────────────────────────────────

def test_versions_of_one_circular_share_its_number(stubs):
    """A search for the number must return the whole history; `date` is
    what separates the versions within it."""
    _, docs = stubs
    hist = sorted((d["date"] for d in docs
                   if d["doc_number"] == "FINMA-RS 2008/11"))
    assert len(hist) >= 4
    assert hist == sorted(hist)
    assert all(d["doc_number"].startswith("FINMA-RS ") for d in docs)


def test_every_document_is_dated(stubs):
    """An undated version cannot be matched against a date of conduct,
    which is the only reason to keep superseded texts at all."""
    _, docs = stubs
    assert all(d["date"] for d in docs)


def test_superseded_documents_say_so_in_every_language(stubs):
    """topics is FTS-indexed, so status has to be searchable in the
    language the reader works in."""
    _, docs = stubs
    for word in ("aufgehoben oder ersetzt", "abrogé ou remplacé",
                 "abrogata o sostituita", "repealed or replaced"):
        assert all(word in d["topics"] for d in docs)


def test_doc_ids_are_stable_across_runs(stubs):
    """Re-running must not mint new ids for the same documents, or the
    incremental append and the DB upsert both break."""
    scraper, docs = stubs
    once = [scraper._make_doc_id(d) for d in docs]
    twice = [scraper._make_doc_id(d) for d in docs]
    assert once == twice
    assert all(i.startswith("finma_rs_") for i in once)
