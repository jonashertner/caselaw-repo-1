"""Statute references in a draft: found in DE/FR/IT prose, checked online and offline, reported."""
import json
import socket
import sqlite3
from pathlib import Path

import pytest
from opencaselaw_cli import cli, workflows
from opencaselaw_cli.client import APIError
from opencaselaw_cli.documents import find_statutes, read_document
from opencaselaw_cli.local import LocalClient
from opencaselaw_cli.statutes import classify_law_response, local_law, statute_label, statute_state

from test_check_document import make_docx
from test_local_pack import _build_pack
from test_workflows import FakeClient

ZGB_8 = "Wo das Gesetz es nicht anders bestimmt, hat derjenige das Vorhandensein einer behaupteten Tatsache zu beweisen, der aus ihr Rechte ableitet."
CO_335 = "Le contrat de durée indéterminée peut être résilié par chacune des parties."
OR_335 = "Ein unbefristetes Arbeitsverhältnis kann von jeder Vertragspartei gekündigt werden."
REPEALED = "Aufgehoben durch Ziff. I 1 des BG vom 5. Okt. 1984, mit Wirkung seit 1. Jan. 1988."


def statutes_fixture(path: Path) -> Path:
    """A statutes database with the schema of search_stack/build_statutes_db.py (laws, articles)."""
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE laws (sr_number TEXT PRIMARY KEY, title_de TEXT, title_fr TEXT, title_it TEXT, abbr_de TEXT, abbr_fr TEXT, abbr_it TEXT,
                           consolidation_date TEXT, work_uri TEXT);
        CREATE TABLE articles (id INTEGER PRIMARY KEY AUTOINCREMENT, sr_number TEXT NOT NULL, article_num TEXT NOT NULL, heading TEXT, footnote TEXT,
                               text TEXT NOT NULL, xml TEXT, lang TEXT NOT NULL, section TEXT NOT NULL DEFAULT '', section_heading TEXT, eid TEXT,
                               FOREIGN KEY (sr_number) REFERENCES laws(sr_number));
        CREATE INDEX idx_articles_sr_art ON articles(sr_number, article_num);
    """)
    con.executemany("INSERT INTO laws VALUES (?,?,?,?,?,?,?,?,?)", [
        ("210", "Schweizerisches Zivilgesetzbuch", "Code civil suisse", "Codice civile svizzero", "ZGB", "CC", "CC", "2026-01-01", "https://fedlex/210"),
        ("220", "Obligationenrecht", "Code des obligations", "Codice delle obbligazioni", "OR", "CO", "CO", "2026-01-01", "https://fedlex/220"),
        ("101", "Bundesverfassung", "Constitution fédérale", "Costituzione federale", "BV", "Cst.", "Cost.", "2026-01-01", "https://fedlex/101"),
    ])
    con.executemany("INSERT INTO articles (sr_number, article_num, heading, footnote, text, xml, lang, section, section_heading, eid) VALUES (?,?,?,?,?,?,?,?,?,?)", [
        ("210", "8", "A. Beweislast", None, ZGB_8, None, "de", "", None, "art_8"),
        ("210", "8", "A. Fardeau de la preuve", None, "Chaque partie doit, si la loi ne prescrit le contraire, prouver les faits qu'elle allègue pour en déduire son droit.", None, "fr", "", None, "art_8"),
        ("210", "10", None, REPEALED, REPEALED, None, "de", "", None, "art_10"),          # repealed stub built before 2026-09-05
        ("210", "1", "Übergangsbestimmung", None, "Die Bestimmungen des Schlusstitels gelten.", None, "de", "disp_u2", "Schlusstitel", "disp_u2/art_1"),
        ("220", "335", "Kündigung", None, OR_335, None, "de", "", None, "art_335"),
        ("220", "335", "Congé", None, CO_335, None, "fr", "", None, "art_335"),
        ("101", "85 a", "Nationalstrassenabgabe", None, "Der Bund erhebt für die Benützung der Nationalstrassen eine Abgabe.", None, "de", "", None, "art_85_a"),
    ])
    con.commit(); con.close()
    return path


PROSE = [
    "Nach Art. 8 Abs. 1 ZGB trägt die Beweislast, wer Rechte ableitet: «Wo das Gesetz es nicht anders bestimmt, hat derjenige das Vorhandensein einer behaupteten Tatsache zu beweisen, der aus ihr Rechte ableitet.»",
    "Die Kündigung ist missbräuchlich (Art. 336 Abs. 1 lit. d OR); vgl. Art. 29 Abs. 2 BV und Art. 41 ff. OR sowie Art. 8 ZGB i.V.m. Art. 2 ZGB.",
    "Selon l'art. 335 al. 1 CO: «Le contrat de durée déterminée peut être résilié par chacune des parties»; voir aussi art. 8 cpv. 1 CC e art. 41 e 97 CO.",
    "Massgebend sind Art. 8, 9 und 10 ZGB sowie Art. 85 a BV und Art. 8bis ZGB. Das ZGB (SR 210) gilt.",
    "Kantonal: § 12 Abs. 2 StG/ZH sowie § 18 VRG (ZH) und § 5 EG SchKG; ferner Art. 12 GOG/ZH und Art. 3 Abs. 1 bzw. 2 DSG e contrario.",
    "Keine Statuten: Art. 5 Abs. 1 und BGE 136 III 513; Art. 12 der Verordnung; RS 2019; Art. 6 Ziff. 1 EMRK; art. 190 al. 2 let. e LDIP.",
]


def test_finder_reads_german_french_italian_and_cantonal_forms():
    rows = find_statutes(PROSE)
    seen = [(r["reference"], r["law"], r["article"], r.get("paragraph"), r.get("letter"), r.get("canton")) for r in rows]
    assert seen[:6] == [("Art. 8 Abs. 1 ZGB", "ZGB", "8", "1", None, None), ("Art. 336 Abs. 1 lit. d OR", "OR", "336", "1", "d", None),
                        ("Art. 29 Abs. 2 BV", "BV", "29", "2", None, None), ("Art. 41 ff. OR", "OR", "41", None, None, None),
                        ("Art. 8 ZGB", "ZGB", "8", None, None, None), ("Art. 2 ZGB", "ZGB", "2", None, None, None)]
    assert ("art. 335 al. 1 CO", "CO", "335", "1", None, None) in seen and ("art. 8 cpv. 1 CC", "CC", "8", "1", None, None) in seen
    assert [s for s in seen if s[0] == "art. 41 e 97 CO"] == [("art. 41 e 97 CO", "CO", "41", None, None, None), ("art. 41 e 97 CO", "CO", "97", None, None, None)]
    assert [s[2] for s in seen if s[0] == "Art. 8, 9 und 10 ZGB"] == ["8", "9", "10"]
    assert ("Art. 85 a BV", "BV", "85a", None, None, None) in seen and ("Art. 8bis ZGB", "ZGB", "8bis", None, None, None) in seen
    sr = next(r for r in rows if r["reference"] == "SR 210")
    assert sr["sr_number"] == "210" and sr["article"] is None
    assert ("§ 12 Abs. 2 StG/ZH", "StG", "12", "2", None, "ZH") in seen and ("§ 18 VRG (ZH)", "VRG", "18", None, None, "ZH") in seen
    assert ("§ 5 EG SchKG", "EG SchKG", "5", None, None, None) in seen and ("Art. 12 GOG/ZH", "GOG", "12", None, None, "ZH") in seen
    assert ("Art. 3 Abs. 1 bzw. 2 DSG", "DSG", "3", "1", None, None) in seen
    assert ("Art. 6 Ziff. 1 EMRK", "EMRK", "6", None, None, None) in seen and ("art. 190 al. 2 let. e LDIP", "LDIP", "190", "2", "e", None) in seen
    assert not any("BGE" in s[0] or "Verordnung" in s[0] or "RS 2019" in s[0] or s[1] == "BGE" for s in seen)
    assert all(r.get("section_sign") for r in rows if r["reference"].startswith("§"))
    quoted = {r["reference"]: r["quote"] for r in rows if r.get("quote")}
    assert quoted["Art. 8 Abs. 1 ZGB"].startswith("Wo das Gesetz") and quoted["art. 335 al. 1 CO"].startswith("Le contrat")
    assert "Art. 8 ZGB" not in quoted  # the same article in another paragraph keeps its own row, without that quotation
    assert rows[0]["paragraph_index"] == 1 and "Beweislast" in rows[0]["context"]


def law_service(params_by_path=None):
    """A /api/laws answering the way the service does: articles [] for a missing article, an error dict for an unknown act."""
    def zgb(params):
        art = params.get("article")
        base = {"sr_number": "210", "title": "Schweizerisches Zivilgesetzbuch", "abbreviation": "ZGB", "canton": "CH", "level": "federal",
                "consolidation_date": "2026-01-01", "language": params.get("language", "de"), "source_url": "https://fedlex/210"}
        if art == "8":
            return {**base, "articles": [{"article_num": "8", "heading": "A. Beweislast", "text": ZGB_8}]}
        if art == "10":
            return {**base, "articles": [{"article_num": "10", "text": "", "footnote": REPEALED, "empty_body": True}]}
        if art == "4":
            return {**base, "articles": [{"article_num": "40", "text": "x"}, {"article_num": "41", "text": "y"}]}  # the server's LIKE fallback
        return {**base, "articles": []}
    def co(params):
        return {"sr_number": "220", "title": "Code des obligations", "abbreviation": "CO", "language": params.get("language"),
                "articles": [{"article_num": "335", "text": CO_335}]}
    def stg(params):
        assert params.get("canton") == "ZH"
        return {"sr_number": "631.1", "title": "Steuergesetz", "abbreviation": "", "canton": "ZH", "level": "cantonal",
                "articles": [{"article_num": "12", "text": "Steuerpflichtig sind natürliche Personen."}]}
    def by_sr(params):
        assert params.get("sr_number") == "210" and "article" not in params
        return {"sr_number": "210", "title": "Schweizerisches Zivilgesetzbuch", "abbreviation": "ZGB", "articles": [{"article_num": "1", "heading": "A."}]}
    overrides = {"/api/laws/ZGB": zgb, "/api/laws/CO": co, "/api/laws/StG": stg, "/api/laws/_": by_sr,
                 "/api/laws/XYZ": {"error": "No law found with abbreviation 'XYZ'."},
                 "/api/laws/VRG": {"error": "No cantonal law found for ZH with abbreviation 'VRG'.", "candidates": [{"sr_number": "175.2", "title": "Verwaltungsrechtspflegegesetz"}]},
                 "/api/laws/BV": APIError(None, "connection refused")}
    overrides.update(params_by_path or {})
    return FakeClient(overrides)


def test_online_rows_are_classified_from_the_service_answer(monkeypatch):
    monkeypatch.delenv("OCL_CANTON", raising=False)
    text = ["Art. 8 Abs. 1 ZGB: «Wo das Gesetz es nicht anders bestimmt, hat derjenige das Vorhandensein einer behaupteten Tatsache zu beweisen, der aus ihr Rechte ableitet.»",
            "Ebenso Art. 8 ZGB und Art. 999 ZGB, Art. 10 ZGB, Art. 4 ZGB sowie Art. 1 XYZ und Art. 29 BV.",
            "Selon l'art. 335 CO, «le contrat de durée déterminée peut être résilié par chacune des parties».",
            "Kantonal § 12 StG/ZH, § 18 VRG (ZH) und § 7 GOG; ferner SR 210."]
    rows = find_statutes(text)
    client = law_service()
    checked = workflows.check_statute_rows(client, rows, language="de", jobs=2)
    co_client = law_service()
    co_rows = workflows.check_statute_rows(co_client, [r for r in rows if r["law"] == "CO"], language="fr")
    assert co_client.calls == [("/api/laws/CO", {"article": "335", "language": "fr"})]  # the language asked for reaches the service
    by_ref = {(r["reference"], r["article"]): r for r in checked}
    assert by_ref[("Art. 8 Abs. 1 ZGB", "8")]["status"] == "statute_found"
    assert by_ref[("Art. 8 Abs. 1 ZGB", "8")]["quote_check"]["quote_status"] == "exact"
    assert by_ref[("Art. 8 Abs. 1 ZGB", "8")]["article_text"] == ZGB_8 and by_ref[("Art. 8 Abs. 1 ZGB", "8")]["provenance"]["sr_number"] == "210"
    assert by_ref[("Art. 8 ZGB", "8")]["status"] == "statute_found"
    assert by_ref[("Art. 999 ZGB", "999")]["status"] == "article_missing"
    assert by_ref[("Art. 10 ZGB", "10")]["status"] == "article_empty" and REPEALED in by_ref[("Art. 10 ZGB", "10")]["reason"]
    assert by_ref[("Art. 4 ZGB", "4")]["status"] == "article_missing" and "40, 41" in by_ref[("Art. 4 ZGB", "4")]["reason"]
    assert by_ref[("Art. 1 XYZ", "1")]["status"] == "law_unknown"
    assert by_ref[("Art. 29 BV", "29")]["status"] == "error" and by_ref[("Art. 29 BV", "29")]["error"]["status"] is None
    assert by_ref[("§ 12 StG/ZH", "12")]["status"] == "statute_found" and by_ref[("§ 12 StG/ZH", "12")]["canton"] == "ZH"
    assert by_ref[("§ 18 VRG (ZH)", "18")]["status"] == "law_unknown" and by_ref[("§ 18 VRG (ZH)", "18")]["candidates"][0]["sr_number"] == "175.2"
    assert by_ref[("§ 7 GOG", "7")]["status"] == "unverifiable" and "OCL_CANTON" in by_ref[("§ 7 GOG", "7")]["reason"]
    assert by_ref[("SR 210", None)]["status"] == "law_found" and by_ref[("SR 210", None)]["provenance"]["title"].startswith("Schweizerisches")
    co = co_rows[0]
    assert co["status"] == "statute_found" and co["quote_check"]["quote_status"] == "near"
    assert any(d["served"] == "in" and not d["quote"] for d in co["quote_check"]["differences"])  # déterminée vs indéterminée
    # one request per distinct (canton, act, article): Art. 8 ZGB was cited twice and asked once
    zgb_8_calls = [c for c in client.calls if c[0] == "/api/laws/ZGB" and c[1].get("article") == "8"]
    assert len(zgb_8_calls) == 1
    # the canton reaches the service as a parameter, never as part of the abbreviation
    assert ("/api/laws/StG", {"article": "12", "language": "de", "canton": "ZH"}) in client.calls
    # a default canton (OCL_CANTON or --canton) routes bare § references
    with_canton = workflows.check_statute_rows(law_service(), [r for r in rows if r["law"] == "GOG"], default_canton="zh")
    assert with_canton[0]["status"] == "law_unknown" or with_canton[0]["status"] == "error"  # GOG is not in the fake; it was asked with canton=ZH
    assert law_service().calls == []
    labels = {r["reference"]: statute_label(r)[0] for r in checked}
    assert labels["Art. 999 ZGB"] == "article not in the act" and labels["Art. 1 XYZ"] == "act not found" and labels["§ 7 GOG"] == "not checked"
    states = {r["reference"]: statute_state(r) for r in checked}
    assert states["Art. 8 ZGB"] == "verified" and states["Art. 999 ZGB"] == "attention" and states["§ 7 GOG"] == "unverifiable" and states["Art. 29 BV"] == "attention"


def test_classification_prefers_the_main_body_and_normalises_numbers():
    answer = {"sr_number": "220", "articles": [{"article_num": "1", "text": "transitional", "section": "disp_u2"}, {"article_num": "1", "text": "main"}]}
    assert classify_law_response(answer, "1")["article_text"] == "main"
    assert classify_law_response({"articles": [{"article_num": "85 a", "text": "t"}]}, "85a")["status"] == "statute_found"
    assert classify_law_response({"articles": "no"}, "1")["status"] == "error"
    assert classify_law_response({"sr_number": "210", "title": "ZGB"}, None)["status"] == "law_found"


def test_offline_statutes_next_to_the_pack(tmp_path, monkeypatch):
    monkeypatch.delenv("OCL_STATUTES", raising=False); monkeypatch.delenv("OCL_CANTON", raising=False)
    pack, _, _ = _build_pack(tmp_path)
    client = LocalClient(pack)
    rows = find_statutes(["Art. 8 ZGB und Art. 999 ZGB sowie Art. 1 XYZ; § 12 StG/ZH; SR 220."])
    # no statutes database: not checked, never an error
    checked = workflows.check_statute_rows(client, rows, jobs=1)
    assert {r["status"] for r in checked} == {"unverifiable"}
    assert all("statutes not available offline" in r["reason"] for r in checked if r["law"] != "StG")
    assert "cantonal" in next(r for r in checked if r["law"] == "StG")["reason"]
    # the database next to the pack answers /api/laws
    statutes_fixture(tmp_path / "statutes.sqlite")
    answer = client.get("/api/laws/ZGB", {"article": "8", "language": "de"})
    assert answer["offline"] is True and answer["sr_number"] == "210" and answer["articles"][0]["text"] == ZGB_8
    assert client.get("/api/laws/zgb", {"article": "8"})["articles"][0]["text"] == ZGB_8            # abbreviations compare case-insensitively
    assert client.get("/api/laws/CC", {"article": "8", "language": "fr"})["articles"][0]["text"].startswith("Chaque partie")
    assert client.get("/api/laws/BV", {"article": "85a"})["articles"][0]["article_num"] == "85 a"  # "85 a" as older builds stored it
    fallback = client.get("/api/laws/OR", {"article": "335", "language": "it"})
    assert fallback["articles"][0]["text"] == OR_335 and fallback["article_language_fallback"]["served"] == "de"
    repealed = client.get("/api/laws/ZGB", {"article": "10"})["articles"][0]
    assert repealed["empty_body"] is True and repealed["text"] == "" and repealed["footnote"] == REPEALED
    transitional = client.get("/api/laws/ZGB", {"article": "1"})
    assert transitional["articles"][0]["section"] == "disp_u2" and "transitional" in transitional["article_section_note"]
    assert client.get("/api/laws/ZGB", {"article": "999"})["articles"] == []
    assert client.get("/api/laws/_", {"sr_number": "220"})["abbreviation"] == "OR" and client.get("/api/laws/_", {"sr_number": "220"})["article_count"] == 1
    assert client.get("/api/laws/XYZ", {"article": "1"})["error"].startswith("No law found")
    assert client.get("/api/laws/StG", {"article": "12", "canton": "ZH"})["unavailable"] is True
    assert client.get("/api/laws/ZH/StG", {"article": "12"})["unavailable"] is True
    assert client.get("/api/laws/OR", {"article": "1", "as_of": "2015-01-01"})["unavailable"] is True
    checked = {(r["reference"], r["article"]): r for r in workflows.check_statute_rows(client, rows, jobs=4)}
    assert checked[("Art. 8 ZGB", "8")]["status"] == "statute_found" and checked[("Art. 8 ZGB", "8")]["provenance"]["offline"] is True
    assert checked[("Art. 999 ZGB", "999")]["status"] == "article_missing"
    assert checked[("Art. 1 XYZ", "1")]["status"] == "law_unknown"
    assert checked[("§ 12 StG/ZH", "12")]["status"] == "unverifiable"
    assert checked[("SR 220", None)]["status"] == "law_found"
    # OCL_STATUTES names the file explicitly; a wrong path is "not available", not a crash
    elsewhere = statutes_fixture(tmp_path / "elsewhere.sqlite")
    (tmp_path / "statutes.sqlite").unlink()
    assert local_law(pack, "ZGB", {"article": "8"})["unavailable"] is True
    monkeypatch.setenv("OCL_STATUTES", str(elsewhere))
    assert local_law(pack, "ZGB", {"article": "8"})["articles"][0]["text"] == ZGB_8
    monkeypatch.setenv("OCL_STATUTES", str(tmp_path / "missing.sqlite"))
    assert local_law(pack, "ZGB", {"article": "8"})["unavailable"] is True


MEMO = [
    "Nach ständiger Rechtsprechung ist die Kündigung missbräuchlich (BGE 136 III 513 E. 2.3).",
    "Nach Art. 8 Abs. 1 ZGB trägt die Beweislast, wer Rechte ableitet: «Wo das Gesetz es nicht anders bestimmt, hat derjenige das Vorhandensein einer behaupteten Tatsache zu beweisen, der aus ihr Rechte ableitet.»",
    "Ebenso Art. 999 ZGB und Art. 1 XYZ; kantonal § 7 GOG.",
]


def case_overrides():
    record = {"decision_id": "bge_BGE_136_III_513", "citation_string_de": "BGE 136 III 513", "court": "bge", "decision_date": "2010-10-07"}
    return {"/api/cite": lambda params: {"exists": True, "decision_id": "bge_BGE_136_III_513", "citation_string_de": "BGE 136 III 513"} if params["reference"] == "BGE 136 III 513" else {"exists": False, "close_matches": []},
            "/api/lookup": {"is_case_number": True, "exact": True, "results": []},
            "/api/decisions/bge_BGE_136_III_513": lambda params: {**record, "full_text": "text"} if params and params.get("full_text") else dict(record),
            "/api/erwaegung/bge_BGE_136_III_513/2.3": {"decision_id": "bge_BGE_136_III_513", "e_number": "2.3", "text": "Streitig ist ..."}}


def test_check_reports_statutes_online_end_to_end(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("OCL_CANTON", raising=False)
    path = make_docx(tmp_path / "memo.docx", MEMO)
    client = law_service(case_overrides())
    monkeypatch.setattr(cli, "create_client", lambda args: client)
    monkeypatch.setenv("OCL_CONFIG", "/nonexistent/ocl-config"); monkeypatch.setenv("OCL_JOBS", "1")
    code = cli.main(["check", str(path), "--format", "json"])
    out = json.loads(capsys.readouterr().out)
    assert code == 4 and out["summary"]["checked"] == 1 and out["summary"]["attention"] == 0
    assert out["summary"]["statutes_checked"] == 4 and out["summary"]["statutes_attention"] == 2 and out["summary"]["statutes_unverifiable"] == 1
    statuses = {r["reference"]: r["status"] for r in out["statutes"]}
    assert statuses == {"Art. 8 Abs. 1 ZGB": "statute_found", "Art. 999 ZGB": "article_missing", "Art. 1 XYZ": "law_unknown", "§ 7 GOG": "unverifiable"}
    assert out["statutes"][0]["quote_check"]["quote_status"] == "exact" and out["statutes_found"][0]["law"] == "ZGB"
    html = Path(out["report_path"]).read_text(encoding="utf-8")
    assert "<h2>Statutes</h2>" in html and "article not in the act" in html and "act not found" in html and "not checked" in html
    assert "Wo das Gesetz es nicht anders bestimmt" in html and "4 statute references found, 1 retrieved, 2 need attention, 1 not checked." in html
    code = cli.main(["check", str(path), "--report", str(tmp_path / "memo-check.md"), "--format", "text", "--color", "never"])
    text = capsys.readouterr().out
    assert code == 4 and "statutes" in text and "article not in the act" in text and "Art. 999 ZGB" in text and "act not found" in text
    md = (tmp_path / "memo-check.md").read_text(encoding="utf-8")
    assert "## Statutes" in md and "| Art. 999 ZGB | article not in the act |" in md and "| As written | Finding | What to do | Article text (excerpt) |" in md
    # only rows that cannot be checked: nothing needs attention, exit 0
    clean = make_docx(tmp_path / "clean.docx", [MEMO[0], "Art. 8 ZGB gilt; § 7 GOG ebenso."])
    code = cli.main(["check", str(clean), "--format", "json", "--no-report"])
    out = json.loads(capsys.readouterr().out)
    assert code == 0 and out["summary"]["statutes_attention"] == 0 and out["summary"]["statutes_unverifiable"] == 1 and out["report_path"] is None


def test_check_runs_offline_against_the_pack_and_the_statutes_database(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("OCL_STATUTES", raising=False); monkeypatch.delenv("OCL_CANTON", raising=False)
    pack, _, _ = _build_pack(tmp_path)
    statutes_fixture(tmp_path / "statutes.sqlite")
    path = make_docx(tmp_path / "memo.docx", MEMO)
    monkeypatch.setenv("OCL_CONFIG", "/nonexistent/ocl-config"); monkeypatch.setenv("OCL_JOBS", "2")

    def no_network(*args, **kwargs):
        raise AssertionError("network access during an offline check")
    monkeypatch.setattr(socket, "socket", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)
    code = cli.main(["--local", str(pack), "check", str(path), "--format", "json", "--no-report"])
    out = json.loads(capsys.readouterr().out)
    assert code == 4
    statuses = {r["reference"]: r["status"] for r in out["statutes"]}
    assert statuses == {"Art. 8 Abs. 1 ZGB": "statute_found", "Art. 999 ZGB": "article_missing", "Art. 1 XYZ": "law_unknown", "§ 7 GOG": "unverifiable"}
    assert out["statutes"][0]["quote_check"]["quote_status"] == "exact" and out["statutes"][0]["provenance"]["offline"] is True
    assert {r["reference"]: r["status"] for r in out["results"]} == {"BGE 136 III 513 E. 2.3": "resolved"}
    # without the database every statute row is "not checked" and the decisions still resolve
    (tmp_path / "statutes.sqlite").unlink()
    code = cli.main(["--local", str(pack), "check", str(path), "--format", "json", "--no-report"])
    out = json.loads(capsys.readouterr().out)
    assert code == 0 and {r["status"] for r in out["statutes"]} == {"unverifiable"} and out["summary"]["statutes_attention"] == 0
    assert all("statutes not available offline" in r["reason"] for r in out["statutes"] if r["law"] != "GOG")
