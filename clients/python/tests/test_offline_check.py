"""The offline path under load and on odd machines: `ocl --local check` on a thread pool,
the --local/--pack grammar, the offline doctor, the Windows data directory, pack paths with
spaces and percent signs, and pack failures that must become row errors, not tracebacks."""
import importlib.util
import json
import shutil
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from opencaselaw_cli import cli, local, workflows
from opencaselaw_cli.client import APIError
from opencaselaw_cli.local import LocalClient, default_pack_dir, default_pack_path, pack_uri

from test_check_document import make_docx
from test_local_pack import _build_pack

ROOT = Path(__file__).resolve().parents[3]
N_BGE, N_BGER = 60, 60


def _pack_from_rows(tmp_path: Path, rows: list[tuple], paragraphs: list[tuple]) -> Path:
    dec = tmp_path / "decisions.db"; con = sqlite3.connect(dec)
    con.execute("CREATE TABLE decisions (decision_id TEXT PRIMARY KEY, court TEXT, canton TEXT, language TEXT, decision_date TEXT, docket_number TEXT, docket_number_2 TEXT, source_url TEXT, content_hash TEXT, collection TEXT, bge_reference TEXT, full_text TEXT)")
    con.executemany("INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.execute("CREATE TABLE decision_docket_aliases (court TEXT, alias_docket TEXT, alias_docket_norm TEXT, canonical_decision_id TEXT, extraction_method TEXT)")
    con.commit(); con.close()
    st = tmp_path / "structure.db"; con = sqlite3.connect(st)
    con.execute("CREATE TABLE erwaegungen_paragraph (decision_id TEXT, e_number TEXT, depth INTEGER, parent TEXT, text TEXT)")
    con.executemany("INSERT INTO erwaegungen_paragraph VALUES (?,?,?,?,?)", paragraphs)
    con.commit(); con.close()
    spec = importlib.util.spec_from_file_location("build_verification_pack", ROOT / "scripts" / "build_verification_pack.py")
    builder = importlib.util.module_from_spec(spec); spec.loader.exec_module(builder)
    out = tmp_path / "big pack.sqlite"
    builder.build(dec, st, out, repo_dir=tmp_path / "no-repo")
    return out


def _big_pack(tmp_path: Path):
    """A pack with 120 decisions and one indexed Erwägung each, and the references a draft would write for them."""
    rows, refs = [], []
    for v in range(100, 100 + N_BGE):
        rows.append((f"bge_BGE_{v}_II_1", "bge", "CH", "de", f"{1874 + v}-01-15", f"{v} II 1", None, f"https://bger/bge/{v}", f"h{v}", "BGE", f"{v} II 1", "text"))
        refs.append(f"BGE {v} II 1" + (" E. 2" if v % 2 == 0 else ""))
    for n in range(1, 1 + N_BGER):
        rows.append((f"bger_4A_{n}_2020", "bger", "CH", "de", "2020-04-05", f"4A_{n}/2020", None, f"https://bger/{n}", f"g{n}", None, None, "text"))
        refs.append(f"BGer 4A_{n}/2020 vom 5. April 2020" + (", E. 2" if n % 2 else ""))
    paragraphs = [(r[0], "2", 1, None, f"Erwägung zwei von {r[0]}: der Vertrag wurde gekündigt.") for r in rows]
    return _pack_from_rows(tmp_path, rows, paragraphs), refs


def _draft(tmp_path: Path, refs: list[str], per_paragraph: int = 4) -> Path:
    paragraphs = ["Die Vorinstanz stützt sich auf " + "; ".join(refs[i:i + per_paragraph]) + "."
                  for i in range(0, len(refs), per_paragraph)]
    return make_docx(tmp_path / "entwurf.docx", paragraphs)


def _small_draft(tmp_path: Path) -> Path:
    return make_docx(tmp_path / "memo.docx", ["Nach BGE 136 III 513 E. 2.3 und BGer 4A_747/2012 vom 5. April 2013, E. 3 ist die Kündigung missbräuchlich."])


@pytest.fixture
def quiet(monkeypatch):
    monkeypatch.setenv("OCL_CONFIG", "/nonexistent/ocl-config")
    monkeypatch.delenv("OCL_JOBS", raising=False); monkeypatch.delenv("OCL_LOCAL", raising=False); monkeypatch.delenv("OCL_PACK", raising=False)


# ── 1. thread safety ─────────────────────────────────────────────────────────
def test_local_client_answers_correctly_from_eight_threads(tmp_path):
    pack, refs = _big_pack(tmp_path)
    client = LocalClient(pack)
    expected = {r: ("bge_BGE_%s_II_1" % r.split()[1]) if r.startswith("BGE") else ("bger_4A_%s_2020" % r.split()[1].split("_")[1].split("/")[0]) for r in refs}

    def one(reference):
        cite = client.get("/api/cite", {"reference": reference, "language": "de"})
        record = client.get("/api/decisions/" + cite["decision_id"], {"full_text": False})
        passage = client.get("/api/erwaegung/" + cite["decision_id"] + "/2")
        return cite["decision_id"], record["decision_id"], passage["text"].endswith(cite["decision_id"] + ": der Vertrag wurde gekündigt.")

    for _ in range(3):  # the shared-connection bug reproduced 3/3 with InterfaceError or a wrong "not found"
        with ThreadPoolExecutor(max_workers=8) as pool:
            answers = list(pool.map(one, refs))
        assert [a[0] for a in answers] == [expected[r] for r in refs]
        assert all(a[0] == a[1] and a[2] for a in answers)
    assert client.requests == 3 * 3 * len(refs)


def test_check_runs_offline_with_eight_jobs_over_a_long_draft(tmp_path, quiet, capsys):
    pack, refs = _big_pack(tmp_path)
    draft = _draft(tmp_path, refs)
    assert len(refs) == N_BGE + N_BGER >= 100 and len(set(refs)) == len(refs)
    code = cli.main(["--local", "--pack", str(pack), "check", str(draft), "--jobs", "8", "--format", "json"])
    out = json.loads(capsys.readouterr().out)
    assert code == 0
    assert out["summary"] == {**out["summary"], "checked": len(refs), "exists": len(refs), "attention": 0}
    assert out["counts"] == {"resolved": len(refs)} and out["base_url"].startswith("file://")
    by_reference = {r["reference"]: r for r in out["results"]}
    assert set(by_reference) == set(refs)
    pinpointed = [r for r in out["results"] if r["pinpoint"]]
    assert len(pinpointed) == len(refs) // 2 and all(r["pinpoint_status"] == "retrieved" and r["passage"]["offline"] for r in pinpointed)
    assert all(r["decision_id"].startswith(("bge_BGE_", "bger_4A_")) for r in out["results"])
    report = Path(out["report_path"])
    assert report.is_file() and report.name == "entwurf.check.html" and "BGer 4A_60/2020 vom 5. April 2020" in report.read_text(encoding="utf-8")


# ── 2. the --local / --pack grammar ──────────────────────────────────────────
def _run_local(capsys, argv):
    code = cli.main(argv)
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    return code, captured


def test_every_documented_local_form_parses_and_runs(tmp_path, quiet, monkeypatch, capsys):
    pack, _, _ = _build_pack(tmp_path)
    draft = _small_draft(tmp_path)
    monkeypatch.setenv("OCL_PACK", str(pack))
    for argv in (["--local", "check", str(draft), "--format", "json"],
                 ["check", str(draft), "--local", "--format", "json"],
                 ["--local", "--pack", str(pack), "check", str(draft), "--format", "json"],
                 ["--local", "--pack", str(pack), "citations", "resolve", "BGE 136 III 513 E. 2.3", "--format", "json"]):
        code, captured = _run_local(capsys, argv)
        out = json.loads(captured.out)
        assert code == 0, argv
        assert out["base_url"] == pack.resolve().as_uri() and all(r["status"] == "resolved" for r in out["results"]), argv
    # --pack beats OCL_PACK; OCL_PACK alone does not switch to offline
    monkeypatch.setenv("OCL_PACK", str(tmp_path / "absent.sqlite"))
    code, captured = _run_local(capsys, ["--local", "--pack", str(pack), "check", str(draft), "--format", "json"])
    assert code == 0 and json.loads(captured.out)["base_url"] == pack.resolve().as_uri()
    args = cli.build_parser().parse_args(["check", str(draft)])
    assert args.local is False and args.pack == str(tmp_path / "absent.sqlite")
    # the 0.6/0.7 environment grammar keeps working: OCL_LOCAL=<pack> switches on and names the pack
    monkeypatch.delenv("OCL_PACK"); monkeypatch.setenv("OCL_LOCAL", str(pack))
    code, captured = _run_local(capsys, ["check", str(draft), "--format", "json"])
    assert code == 0 and json.loads(captured.out)["base_url"] == pack.resolve().as_uri()
    monkeypatch.setenv("OCL_LOCAL", "1"); monkeypatch.setenv("OCL_PACK", str(pack))
    code, captured = _run_local(capsys, ["check", str(draft), "--format", "json"])
    assert code == 0 and json.loads(captured.out)["base_url"] == pack.resolve().as_uri()
    monkeypatch.setenv("OCL_LOCAL", "0")
    assert cli.build_parser().parse_args(["check", str(draft)]).local is False


def test_offline_defaults_from_config_values():
    assert cli.offline_defaults({}) == (False, None)
    assert cli.offline_defaults({"pack": "/p/pack.sqlite"}) == (False, "/p/pack.sqlite")
    assert cli.offline_defaults({"local": "true"}) == (True, None)
    assert cli.offline_defaults({"local": "1", "pack": "/p/pack.sqlite"}) == (True, "/p/pack.sqlite")
    assert cli.offline_defaults({"local": "/legacy/pack.sqlite"}) == (True, "/legacy/pack.sqlite")
    assert cli.offline_defaults({"local": "/legacy/pack.sqlite", "pack": "/p/pack.sqlite"}) == (True, "/p/pack.sqlite")
    assert cli.offline_defaults({"local": "off", "pack": "/p"}) == (False, "/p")


def test_missing_or_unreadable_pack_is_exit_2_with_advice(tmp_path, quiet, monkeypatch, capsys):
    draft = _small_draft(tmp_path)
    absent = tmp_path / "none.sqlite"
    code, captured = _run_local(capsys, ["--local", "--pack", str(absent), "check", str(draft), "--format", "json"])
    assert code == 2 and captured.out == "" and "ocl pack pull" in captured.err and "none.sqlite" in captured.err
    # the default location is read at run time; nothing pulled there yet
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data")); monkeypatch.setattr(sys, "platform", "linux")
    code, captured = _run_local(capsys, ["--local", "doctor", "--format", "json"])
    assert code == 2 and "ocl pack pull" in captured.err and str(tmp_path / "data" / "ocl" / "verification_pack.sqlite") in captured.err
    bad = tmp_path / "bad.sqlite"; bad.write_bytes(b"this is not a database")
    code, captured = _run_local(capsys, ["--local", "--pack", str(bad), "doctor", "--format", "json"])
    assert code == 2 and "not a verification pack" in captured.err


# ── 3. the offline doctor ────────────────────────────────────────────────────
def test_local_doctor_reports_the_pack_and_exits_0(tmp_path, quiet, capsys):
    pack, _, _ = _build_pack(tmp_path)
    code, captured = _run_local(capsys, ["--local", "--pack", str(pack), "doctor", "--format", "json"])
    report = json.loads(captured.out)
    assert code == 0 and report["ok"] is True and report["mode"] == "offline" and "warnings" not in report
    assert report["pack"] == str(pack.resolve()) and report["pack_bytes"] == pack.stat().st_size
    assert report["schema_version"] == "2" and report["built_at"] and report["db_generation"]
    assert report["decisions"] == 4 and report["paragraphs"] == 3 and report["pack_age_days"] == 0
    assert report["sqlite_version"] == sqlite3.sqlite_version and report["cite_ok"] is True and report["cite_ms"] >= 0
    assert "tools" not in report and "health" not in report
    # an old snapshot is a warning naming `ocl pack pull`, not a failure
    con = sqlite3.connect(pack)
    con.execute("UPDATE meta SET value = ? WHERE key = 'built_at'", ((datetime.now(timezone.utc) - timedelta(days=30, hours=1)).isoformat(),))
    con.commit(); con.close()
    code, captured = _run_local(capsys, ["--local", "--pack", str(pack), "doctor", "--format", "json"])
    report = json.loads(captured.out)
    assert code == 0 and report["ok"] is True and report["pack_age_days"] == 30
    assert len(report["warnings"]) == 1 and "30 days" in report["warnings"][0] and "ocl pack pull" in report["warnings"][0]
    code, captured = _run_local(capsys, ["--local", "--pack", str(pack), "doctor", "--format", "text", "--color", "never"])
    assert code == 0 and "offline" in captured.out and "pack_age_days" in captured.out


def test_pack_age_parsing_is_lenient():
    assert cli._pack_age_days(None) is None and cli._pack_age_days("yesterday") is None
    assert cli._pack_age_days(datetime.now(timezone.utc).isoformat()) == 0
    assert cli._pack_age_days((datetime.now(timezone.utc) - timedelta(days=15)).strftime("%Y-%m-%dT%H:%M:%SZ")) == 15
    assert cli._pack_age_days((datetime.now(timezone.utc) + timedelta(days=3)).isoformat()) == 0  # a clock ahead of the build host


# ── 4. Windows: data directory and file URIs ─────────────────────────────────
def test_default_pack_dir_follows_the_platform(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setenv("LOCALAPPDATA", r"C:\Users\jh\AppData\Local"); monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    assert default_pack_dir() == Path(r"C:\Users\jh\AppData\Local") / "ocl"
    monkeypatch.delenv("LOCALAPPDATA")
    assert default_pack_dir() == Path.home() / "AppData" / "Local" / "ocl"
    monkeypatch.setattr(sys, "platform", "linux")
    assert default_pack_dir() == tmp_path / "xdg" / "ocl"
    monkeypatch.delenv("XDG_DATA_HOME")
    assert default_pack_dir() == Path.home() / ".local" / "share" / "ocl"
    monkeypatch.setattr(sys, "platform", "darwin")
    assert default_pack_dir() == Path.home() / ".local" / "share" / "ocl"
    assert default_pack_path().name == "verification_pack.sqlite" and default_pack_path().parent == default_pack_dir()
    assert local.DEFAULT_PACK_DIR.name == "ocl"  # the 0.6/0.7 constant survives for importers


def test_pack_path_with_spaces_percent_and_hash_opens(tmp_path, quiet, monkeypatch, capsys):
    pack, _, _ = _build_pack(tmp_path)
    odd = tmp_path / "pack dir 100% ok" / "verification pack #1.sqlite"
    odd.parent.mkdir(); shutil.copyfile(pack, odd)
    uri = pack_uri(odd)
    assert uri.startswith("file://") and uri.endswith("?mode=ro&immutable=1")
    assert "%20" in uri and "%25" in uri and "%23" in uri and " " not in uri
    client = LocalClient(odd)
    assert client.get("/health")["decisions"] == 4 and client.get("/api/cite", {"reference": "BGE 136 III 513"})["exists"] is True
    monkeypatch.chdir(tmp_path)
    assert LocalClient("pack dir 100% ok/verification pack #1.sqlite").pack_path == odd.resolve()  # relative paths resolve
    code, captured = _run_local(capsys, ["--local", "--pack", str(odd), "check", str(_small_draft(tmp_path)), "--format", "json"])
    assert code == 0 and json.loads(captured.out)["base_url"] == odd.resolve().as_uri()


# ── 5. failures inside the pack become APIError ──────────────────────────────
def test_pack_failures_surface_as_apierror_rows(tmp_path, quiet, monkeypatch, capsys):
    pack, _, _ = _build_pack(tmp_path)
    client = LocalClient(pack)

    def broken():
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(client, "_connection", broken)
    with pytest.raises(APIError) as excinfo:
        client.get("/api/cite", {"reference": "BGE 136 III 513"})
    assert excinfo.value.status is None and "OperationalError: disk I/O error" in excinfo.value.message and "ocl pack pull" in excinfo.value.message
    with pytest.raises(APIError):  # the "not available offline" answer keeps its own status
        client.get("/api/decisions", {"q": "x"})
    assert client.requests == 2
    report = workflows.resolve_rows(client, [{"reference": "BGE 136 III 513"}], jobs=1)
    row = report["results"][0]
    assert row["status"] == "error" and "disk I/O error" in row["error"]["message"] and report["status"] == "partial"
    monkeypatch.setattr(cli, "create_client", lambda args: client)
    code, captured = _run_local(capsys, ["--local", "check", str(_small_draft(tmp_path)), "--format", "json"])
    out = json.loads(captured.out)
    assert code == 4 and all(r["status"] == "error" for r in out["results"]) and out["summary"]["attention"] == 2


# ── help text ────────────────────────────────────────────────────────────────
def test_every_parser_help_renders_and_documents_the_new_grammar(capsys):
    """argparse %-formats help strings: a literal % (as in %LOCALAPPDATA%) must be escaped."""
    parser = cli.build_parser(config={})
    for argv in (["pack", "pull", "--help"], ["pack", "--help"], ["doctor", "--help"], ["check", "--help"], ["--help"]):
        with pytest.raises(SystemExit) as excinfo:
            parser.parse_args(argv)
        assert excinfo.value.code == 0, argv
    out = capsys.readouterr().out
    assert "%LOCALAPPDATA%\\ocl" in out and "~/.local/share/ocl" in out
    assert "ocl --local check memo.docx" in out and "--pack PATH" in out and "OCL_PACK" in out and "OCL_LOCAL=1" in out
