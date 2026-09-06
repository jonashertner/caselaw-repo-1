"""Offline mode: the verification pack answers the verification endpoints without the service."""
import gzip
import importlib.util
import io
import json
import sqlite3
from pathlib import Path

import pytest
from opencaselaw_cli import cli
from opencaselaw_cli.local import LocalClient, pull

ROOT = Path(__file__).resolve().parents[3]


def _fixture_dbs(tmp_path):
    dec = tmp_path / "decisions.db"; con = sqlite3.connect(dec)
    con.execute("CREATE TABLE decisions (decision_id TEXT PRIMARY KEY, court TEXT, canton TEXT, language TEXT, decision_date TEXT, docket_number TEXT, docket_number_2 TEXT, source_url TEXT, content_hash TEXT, collection TEXT, bge_reference TEXT, full_text TEXT)")
    con.executemany("INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [
        ("bge_BGE_136_III_513", "bge", "CH", "fr", "2010-10-07", "136 III 513", None, "https://bger/1", "h1", "BGE", "136 III 513", "text"),
        ("bger_4A_747_2012", "bger", "CH", "2013-04-05", "2013-04-05", "4A_747/2012", None, "https://bger/2", "h2", None, None, "text"),
        ("zh_obergericht_LA210005", "zh_obergericht", "ZH", "de", "2021-06-15", "LA210005", None, "https://zh/1", "h3", None, None, "text"),
        ("bger_4P.166_2006", "bger", "CH", "de", "2006-11-09", "4P.166/2006", None, "https://bger/3", "h4", None, None, "text"),
    ])
    con.execute("CREATE TABLE decision_docket_aliases (court TEXT, alias_docket TEXT, alias_docket_norm TEXT, canonical_decision_id TEXT, extraction_method TEXT)")
    con.execute("INSERT INTO decision_docket_aliases VALUES ('bger', '4C.230/2006', '4C_230/2006', 'bger_4P.166_2006', 'caption')")
    con.commit(); con.close()
    st = tmp_path / "structure.db"; con = sqlite3.connect(st)
    con.execute("CREATE TABLE erwaegungen_paragraph (decision_id TEXT, e_number TEXT, depth INTEGER, parent TEXT, text TEXT)")
    con.executemany("INSERT INTO erwaegungen_paragraph VALUES (?,?,?,?,?)", [
        ("bge_BGE_136_III_513", "2", 1, None, "2. Streitig ist ..."),
        ("bge_BGE_136_III_513", "2.3", 2, "2", "Selon l'art. 335 al. 1 CO, le contrat de travail conclu pour une durée indéterminée peut être résilié."),
        ("bger_4A_747_2012", "3", 1, None, "Die Beschwerdegegner hatten ..."),
    ])
    con.commit(); con.close()
    return dec, st


def _build_pack(tmp_path):
    spec = importlib.util.spec_from_file_location("build_verification_pack", ROOT / "scripts" / "build_verification_pack.py")
    builder = importlib.util.module_from_spec(spec); spec.loader.exec_module(builder)
    dec, st = _fixture_dbs(tmp_path)
    out = tmp_path / "pack.sqlite"
    meta = builder.build(dec, st, out, repo_dir=tmp_path / "no-repo")  # no mcp_server: strings absent, everything else present
    return out, meta, builder


def test_pack_builds_and_answers_offline(tmp_path):
    out, meta, builder = _build_pack(tmp_path)
    assert meta["decisions"] == "4" and meta["paragraphs"] == "3" and out.stat().st_size > 0
    client = LocalClient(out)
    health = client.get("/health")
    assert health["offline"] is True and health["decisions"] == 4
    cite = client.get("/api/cite", {"reference": "4A 747/2012", "language": "de"})
    assert cite["exists"] is True and cite["decision_id"] == "bger_4A_747_2012"
    assert client.get("/api/cite", {"reference": "BGE 136 III 513"})["decision_id"] == "bge_BGE_136_III_513"
    assert client.get("/api/cite", {"reference": "Obergericht ZH LA210005 vom 15. Juni 2021"})["decision_id"] == "zh_obergericht_LA210005"
    assert client.get("/api/cite", {"reference": "4C_230/2006"})["decision_id"] == "bger_4P.166_2006"  # joined docket
    assert client.get("/api/cite", {"reference": "BGE 999 III 1"})["exists"] is False
    passage = client.get("/api/erwaegung/bge_BGE_136_III_513/2.3")
    assert passage["text"].startswith("Selon l'art. 335") and passage["offline"] is True
    assert client.get("/api/erwaegung/bge_BGE_136_III_513/9")["available_e_numbers"] == ["2", "2.3"]
    record = client.get("/api/decisions/bger_4P.166_2006", {"full_text": False})
    assert record["joined_dockets"] == ["4C_230/2006"] and record["is_canonical"] is True
    with pytest.raises(Exception) as excinfo:
        client.get("/api/decisions", {"q": "x"})
    assert "not available offline" in str(excinfo.value)


def test_resolve_and_quotes_run_against_the_pack(tmp_path, monkeypatch, capsys):
    out, _, _ = _build_pack(tmp_path)
    monkeypatch.setenv("OCL_CONFIG", "/nonexistent/ocl-config"); monkeypatch.setenv("OCL_JOBS", "1")
    code = cli.main(["--local", str(out), "citations", "resolve", "BGE 136 III 513 E. 2.3", "4A_747/2012", "BGE 999 III 1", "--format", "jsonl", "--fields", "reference,status,decision_id"])
    rows = [json.loads(l) for l in capsys.readouterr().out.splitlines()]
    statuses = {r["reference"]: r["status"] for r in rows if not r.get("_type")}
    assert code == 4 and statuses == {"BGE 136 III 513 E. 2.3": "resolved", "4A_747/2012": "resolved", "BGE 999 III 1": "missing"}
    code = cli.main(["--local", str(out), "quotes", "check", "BGE 136 III 513 E. 2.3", "--quote", "le contrat de travail conclu pour une durée indéterminée", "--format", "json"])
    assert code == 0 and json.loads(capsys.readouterr().out)["results"][0]["quote_status"] == "exact"
    code = cli.main(["--local", str(out), "pack", "info", "--path", str(out), "--format", "json"])
    assert code == 0 and json.loads(capsys.readouterr().out)["decisions"] == "4"
    code = cli.main(["--local", str(out), "decisions", "search", "x", "--format", "json"])
    assert code == 4 and "not available offline" in capsys.readouterr().err


def test_pull_downloads_and_unpacks(tmp_path):
    out, _, _ = _build_pack(tmp_path)
    payload = gzip.compress(out.read_bytes())
    class Resp(io.BytesIO):
        headers = {"Content-Length": str(len(payload))}
        def __enter__(self): return self
        def __exit__(self, *a): return False
    report = pull(tmp_path / "dl" / "pack.sqlite", url="https://example.invalid/pack.gz", opener=lambda request, timeout: Resp(payload))
    assert report["decisions"] == "4" and (tmp_path / "dl" / "pack.sqlite").stat().st_size == out.stat().st_size
