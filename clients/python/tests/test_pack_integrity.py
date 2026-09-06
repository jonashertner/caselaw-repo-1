"""Pack integrity: the sha256 sidecar the builder writes, resumable and verified pulls
(a loopback http.server in a thread: full, interrupted, resumed, Range ignored, wrong
digest, no sidecar), file:// and share sources, the schema gate on open, and the
Windows default location. No network beyond 127.0.0.1."""
import gzip
import hashlib
import http.server
import importlib.util
import json
import os
import re
import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest
from opencaselaw_cli import cli, local
from opencaselaw_cli._version import __version__
from opencaselaw_cli.client import APIError
from opencaselaw_cli.local import LocalClient, PackIntegrityError, pull

ROOT = Path(__file__).resolve().parents[3]


# ── fixtures ─────────────────────────────────────────────────────────────
def _tiny_pack(path: Path, schema_version="1", with_meta=True, filler_rows=400) -> Path:
    """A pack with the builder's schema, one real row and incompressible filler so the gzip has some size."""
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE decisions (decision_id TEXT PRIMARY KEY, court TEXT, canton TEXT, language TEXT, decision_date TEXT,
            docket_number TEXT, docket_number_2 TEXT, citation_string_de TEXT, citation_string_fr TEXT, citation_string_it TEXT,
            canonical_url TEXT, source_url TEXT, content_hash TEXT, canonical_decision_id TEXT, has_full_text INTEGER);
        CREATE TABLE aliases (alias_docket_norm TEXT, canonical_decision_id TEXT);
        CREATE TABLE paragraphs (decision_id TEXT, e_number TEXT, depth INTEGER, parent TEXT, text_z BLOB,
                                 PRIMARY KEY (decision_id, e_number)) WITHOUT ROWID;""")
    con.execute("INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("bger_4A_747_2012", "bger", "CH", "de", "2013-04-05", "4A_747/2012", None, "Urteil 4A_747/2012 vom 5. April 2013",
                 None, None, "https://bger/2", "https://bger/2", "h2", "bger_4A_747_2012", 1))
    con.executemany("INSERT INTO decisions (decision_id, court, content_hash) VALUES (?, 'zz', ?)",
                    [(f"zz_{i}", os.urandom(24).hex()) for i in range(filler_rows)])
    if with_meta:
        con.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        con.executemany("INSERT INTO meta VALUES (?, ?)", [("schema_version", schema_version), ("built_at", "2026-09-06T03:30:00+00:00"),
                                                            ("db_generation", "g1"), ("decisions", str(1 + filler_rows)), ("paragraphs", "0")])
    con.commit()
    con.close()
    return path


def _gz_pack(tmp_path: Path, **kw) -> tuple[bytes, str]:
    pack = _tiny_pack(tmp_path / f"src-{time.monotonic_ns()}.sqlite", **kw)
    data = gzip.compress(pack.read_bytes())
    return data, hashlib.sha256(data).hexdigest()


class _Handler(http.server.BaseHTTPRequestHandler):
    """Serves server.cfg['files'] with Range support; cfg toggles: honour_range, stall_after (bytes, then sleep)."""

    def log_message(self, *args):
        pass

    def do_GET(self):
        cfg = self.server.cfg
        name = self.path.rsplit("/", 1)[-1].split("?")[0]
        self.server.requests.append((name, self.headers.get("Range")))
        data = cfg["files"].get(name)
        if data is None:
            self.send_error(404)
            return
        start = 0
        rng = self.headers.get("Range")
        if rng and cfg.get("honour_range", True):
            start = int(re.match(r"bytes=(\d+)-", rng).group(1))
            if start >= len(data):
                self.send_error(416)
                return
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{len(data) - 1}/{len(data)}")
        else:
            self.send_response(200)
        body = data[start:]
        self.send_header("Content-Length", str(len(body)))
        self.send_header("ETag", '"pack-1"')
        self.end_headers()
        stall = cfg.get("stall_after")
        if stall is not None:
            self.wfile.write(body[:stall])
            self.wfile.flush()
            time.sleep(cfg.get("stall_seconds", 1.5))   # the client's read timeout is shorter; the connection then closes
            return
        self.wfile.write(body)


class _Server(http.server.ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def handle_error(self, request, client_address):   # a client that gave up mid-body is expected here
        pass


@pytest.fixture
def server():
    srv = _Server(("127.0.0.1", 0), _Handler)
    srv.cfg = {"files": {}}
    srv.requests = []
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    srv.url = f"http://127.0.0.1:{srv.server_address[1]}"
    yield srv
    srv.shutdown()
    srv.server_close()


def _publish(server, tmp_path, **kw) -> tuple[bytes, str]:
    data, digest = _gz_pack(tmp_path, **kw)
    server.cfg["files"]["pack.sqlite.gz"] = data
    server.cfg["files"]["pack.sqlite.gz.sha256"] = f"{digest}  pack.sqlite.gz\n".encode()
    return data, digest


@pytest.fixture(autouse=True)
def _small_chunks(monkeypatch):
    monkeypatch.setattr(local, "_CHUNK", 4096)
    monkeypatch.setattr(local, "PROGRESS_EVERY", 8192)
    monkeypatch.setenv("OCL_CONFIG", "/nonexistent/ocl-config")


def _part(dest: Path) -> Path:
    return dest.with_name(dest.name + ".gz.part")


# ── the builder's sidecar ────────────────────────────────────────────────
def _fixture_dbs(tmp_path):
    dec = tmp_path / "decisions.db"
    con = sqlite3.connect(dec)
    con.execute("CREATE TABLE decisions (decision_id TEXT PRIMARY KEY, court TEXT, canton TEXT, language TEXT, decision_date TEXT, docket_number TEXT, "
                "docket_number_2 TEXT, source_url TEXT, content_hash TEXT, collection TEXT, bge_reference TEXT, full_text TEXT)")
    con.executemany("INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [
        ("bge_BGE_136_III_513", "bge", "CH", "fr", "2010-10-07", "136 III 513", None, "https://bger/1", "h1", "BGE", "136 III 513", "text"),
        ("bger_4A_747_2012", "bger", "CH", "de", "2013-04-05", "4A_747/2012", None, "https://bger/2", "h2", None, None, "text")])
    con.commit()
    con.close()
    st = tmp_path / "structure.db"
    con = sqlite3.connect(st)
    con.execute("CREATE TABLE erwaegungen_paragraph (decision_id TEXT, e_number TEXT, depth INTEGER, parent TEXT, text TEXT)")
    con.execute("INSERT INTO erwaegungen_paragraph VALUES ('bge_BGE_136_III_513', '2.3', 2, '2', 'Selon l''art. 335 al. 1 CO ...')")
    con.commit()
    con.close()
    return dec, st


def test_builder_gzip_writes_a_sha256sum_sidecar(tmp_path, monkeypatch, capsys):
    spec = importlib.util.spec_from_file_location("build_verification_pack", ROOT / "scripts" / "build_verification_pack.py")
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    dec, st = _fixture_dbs(tmp_path)
    out = tmp_path / "2026-09-06.sqlite"
    monkeypatch.setattr(sys, "argv", ["build_verification_pack.py", "--decisions-db", str(dec), "--structure-db", str(st), "--output", str(out),
                                      "--repo-dir", str(tmp_path / "no-repo"), "--gzip"])
    assert builder.main() == 0
    captured = capsys.readouterr()
    meta = json.loads(captured.out)
    gz = out.with_name(out.name + ".gz")
    sidecar = gz.with_name(gz.name + ".sha256")
    digest = hashlib.sha256(gz.read_bytes()).hexdigest()
    assert sidecar.read_text(encoding="utf-8") == f"{digest}  {gz.name}\n"          # sha256sum -c format
    assert meta["gzip_sha256"] == digest and meta["gzip_bytes"] == gz.stat().st_size == len(gz.read_bytes())
    assert meta["sha256_file"] == str(sidecar) and meta["decisions"] == "2" and meta["paragraphs"] == "1"
    assert re.search(r"gzip .*\(\d[\d,]* bytes\) sha256 " + digest + r" -> " + re.escape(sidecar.name), captured.err)
    assert gzip.decompress(gz.read_bytes()) == out.read_bytes()
    assert not gz.with_name(gz.name + ".tmp").exists() and not sidecar.with_name(sidecar.name + ".tmp").exists()
    # the pull accepts the pair straight from disk (the way a court's share would hold it)
    report = pull(tmp_path / "dl" / "verification_pack.sqlite", url=str(gz))
    assert report["verified"] is True and report["gzip_sha256"] == digest and report["decisions"] == "2"


# ── pull over http ───────────────────────────────────────────────────────
def test_pull_verifies_installs_and_records(server, tmp_path, capsys):
    data, digest = _publish(server, tmp_path)
    url = server.url + "/pack.sqlite.gz"
    dest = tmp_path / "dl" / "verification_pack.sqlite"
    lines = []
    report = pull(dest, url=url, log=lines.append)
    assert report["verified"] is True and report["gzip_sha256"] == digest and report["downloaded_bytes"] == len(data)
    assert report["decisions"] == "401" and report["schema_version"] == "1" and report["source_url"] == url
    assert dest.is_file() and not _part(dest).exists() and not dest.with_name(dest.name + ".tmp").exists()
    assert not _part(dest).with_name(_part(dest).name + ".json").exists()
    record = json.loads(dest.with_name(dest.name + ".json").read_text(encoding="utf-8"))
    assert record["gzip_sha256"] == digest and record["source_url"] == url and record["checksum_url"] == url + ".sha256"
    assert record["verified"] is True and record["bytes"] == dest.stat().st_size and record["client_version"] == __version__
    assert record["pack_sha256"] == hashlib.sha256(dest.read_bytes()).hexdigest() and record["built_at"] == "2026-09-06T03:30:00+00:00"
    assert [n for n, _ in server.requests] == ["pack.sqlite.gz.sha256", "pack.sqlite.gz"]   # the sidecar first: fail before the big download
    progress = [l for l in lines if l.startswith("downloaded ") and "%" in l]
    assert progress and progress[-1].endswith("(100%)") and any(l.startswith("downloading ") for l in lines) and "checksum verified; unpacking" in lines
    assert LocalClient(dest).get("/api/cite", {"reference": "4A_747/2012"})["exists"] is True
    # verify and info print the same report; verify exits 0 because the pull was verified
    assert cli.main(["pack", "verify", "--path", str(dest), "--format", "json"]) == 0
    verify = json.loads(capsys.readouterr().out)
    assert cli.main(["pack", "info", "--path", str(dest), "--format", "json"]) == 0
    info = json.loads(capsys.readouterr().out)
    assert verify == info and verify["verified"] is True and verify["gzip_sha256"] == digest and verify["schema_version"] == "1"
    assert verify["built_at"] and verify["decisions"] == "401" and verify["paragraphs"] == "0" and verify["source_url"] == url
    assert "matched" in verify["verification"] and verify["bytes"] == dest.stat().st_size


def test_pull_resumes_after_an_interrupted_download(server, tmp_path):
    data, digest = _publish(server, tmp_path)
    url = server.url + "/pack.sqlite.gz"
    dest = tmp_path / "dl" / "pack.sqlite"
    server.cfg.update(stall_after=len(data) // 2, stall_seconds=1.5)
    with pytest.raises(APIError) as excinfo:
        pull(dest, url=url, read_timeout=0.3)             # per-read timeout, not an overall one
    assert excinfo.value.status is None and "run `ocl pack pull` again to resume" in str(excinfo.value)
    part = _part(dest)
    kept = part.stat().st_size
    assert 0 < kept < len(data) and not dest.exists()
    state = json.loads(part.with_name(part.name + ".json").read_text(encoding="utf-8"))
    assert state["url"] == url and state["expected"] == digest and state["validator"] == '"pack-1"' and state["total"] == len(data)
    server.cfg["stall_after"] = None
    lines = []
    report = pull(dest, url=url, log=lines.append)
    assert report["downloaded_bytes"] == len(data) - kept and report["verified"] is True
    assert server.requests[-1] == ("pack.sqlite.gz", f"bytes={kept}-") and any(l.startswith("resuming from ") for l in lines)
    assert dest.is_file() and not part.exists() and LocalClient(dest).meta["decisions"] == "401"
    # a .part that is already complete is only hashed and unpacked (HTTP 416 from the source)
    dest2 = tmp_path / "dl2" / "pack.sqlite"
    dest2.parent.mkdir()
    _part(dest2).write_bytes(data)
    _part(dest2).with_name(_part(dest2).name + ".json").write_text(json.dumps({"url": url, "expected": digest, "restart": False}), encoding="utf-8")
    report = pull(dest2, url=url)
    assert report["downloaded_bytes"] == 0 and report["verified"] is True and server.requests[-1] == ("pack.sqlite.gz", f"bytes={len(data)}-")


def test_pull_starts_over_when_it_cannot_resume(server, tmp_path):
    data, digest = _publish(server, tmp_path)
    url = server.url + "/pack.sqlite.gz"
    # the source ignores Range: a full answer replaces the part
    server.cfg["honour_range"] = False
    dest = tmp_path / "a" / "pack.sqlite"
    dest.parent.mkdir()
    _part(dest).write_bytes(data[:1000])
    _part(dest).with_name(_part(dest).name + ".json").write_text(json.dumps({"url": url, "expected": digest, "validator": '"pack-1"', "restart": False}),
                                                                 encoding="utf-8")
    lines = []
    report = pull(dest, url=url, log=lines.append)
    assert report["downloaded_bytes"] == len(data) and report["verified"] is True and dest.is_file()
    assert server.requests[-1] == ("pack.sqlite.gz", "bytes=1000-") and any("starting over" in l for l in lines)
    # the pack was republished since the part was saved (the published digest changed): no Range, fresh download
    server.cfg["honour_range"] = True
    dest = tmp_path / "b" / "pack.sqlite"
    dest.parent.mkdir()
    _part(dest).write_bytes(b"stale bytes of last week's pack")
    _part(dest).with_name(_part(dest).name + ".json").write_text(json.dumps({"url": url, "expected": "ab" * 32, "restart": False}), encoding="utf-8")
    lines = []
    report = pull(dest, url=url, log=lines.append)
    assert report["downloaded_bytes"] == len(data) and server.requests[-1] == ("pack.sqlite.gz", None)
    assert any("republished" in l for l in lines)
    # a part from another source is discarded too
    dest = tmp_path / "c" / "pack.sqlite"
    dest.parent.mkdir()
    _part(dest).write_bytes(data[:500])
    _part(dest).with_name(_part(dest).name + ".json").write_text(json.dumps({"url": "https://elsewhere.invalid/x.gz", "expected": digest}), encoding="utf-8")
    report = pull(dest, url=url)
    assert report["downloaded_bytes"] == len(data) and server.requests[-1] == ("pack.sqlite.gz", None)


def test_wrong_checksum_fails_keeps_the_part_and_restarts_next_time(server, tmp_path, capsys):
    data, digest = _publish(server, tmp_path)
    url = server.url + "/pack.sqlite.gz"
    server.cfg["files"]["pack.sqlite.gz.sha256"] = ("0" * 64 + "  pack.sqlite.gz\n").encode()
    dest = tmp_path / "dl" / "pack.sqlite"
    with pytest.raises(PackIntegrityError) as excinfo:
        pull(dest, url=url)
    message = str(excinfo.value)
    assert "checksum mismatch" in message and "0" * 64 in message and digest in message and str(_part(dest)) in message
    assert _part(dest).read_bytes() == data and not dest.exists() and not dest.with_name(dest.name + ".tmp").exists()
    state = json.loads(_part(dest).with_name(_part(dest).name + ".json").read_text(encoding="utf-8"))
    assert state["restart"] is True and state["reason"] == "checksum mismatch"
    # through the CLI: exit 2 (invalid input), message on stderr, nothing installed
    assert cli.main(["pack", "pull", "--url", url, "--to", str(dest)]) == 2
    assert "checksum mismatch" in capsys.readouterr().err and not dest.exists()
    # the published checksum is corrected: the next pull starts from zero (no Range) and installs
    server.cfg["files"]["pack.sqlite.gz.sha256"] = f"{digest}  pack.sqlite.gz\n".encode()
    server.requests.clear()
    lines = []
    report = pull(dest, url=url, log=lines.append)
    assert report["verified"] is True and dest.is_file() and not _part(dest).exists()
    assert server.requests == [("pack.sqlite.gz.sha256", None), ("pack.sqlite.gz", None)] and any("its checksum failed" in l for l in lines)
    # a malformed sidecar is refused before anything is downloaded
    server.cfg["files"]["pack.sqlite.gz.sha256"] = b"not a digest\n"
    server.requests.clear()
    with pytest.raises(PackIntegrityError, match="malformed checksum file"):
        pull(tmp_path / "x" / "pack.sqlite", url=url)
    assert server.requests == [("pack.sqlite.gz.sha256", None)]


def test_missing_checksum_needs_insecure(server, tmp_path, capsys):
    data, digest = _publish(server, tmp_path)
    del server.cfg["files"]["pack.sqlite.gz.sha256"]
    url = server.url + "/pack.sqlite.gz"
    dest = tmp_path / "dl" / "pack.sqlite"
    assert cli.main(["pack", "pull", "--url", url, "--to", str(dest)]) == 2
    err = capsys.readouterr().err
    assert "no checksum published" in err and "--insecure" in err and url + ".sha256" in err
    assert server.requests == [("pack.sqlite.gz.sha256", None)] and not dest.exists() and not _part(dest).exists()
    assert cli.main(["pack", "pull", "--url", url, "--to", str(dest), "--insecure", "--format", "json"]) == 0
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert report["verified"] is False and report["gzip_sha256"] == digest and "continuing unverified" in captured.err
    assert dest.is_file() and json.loads(dest.with_name(dest.name + ".json").read_text(encoding="utf-8"))["checksum_url"] is None
    assert cli.main(["pack", "verify", "--path", str(dest), "--format", "json"]) == 4
    verify = json.loads(capsys.readouterr().out)
    assert verify["verified"] is False and "--insecure" in verify["verification"] and verify["decisions"] == "401"
    assert cli.main(["pack", "info", "--path", str(dest), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["verified"] is False


def test_a_pack_too_new_for_the_client_is_refused_before_replacing_the_installed_one(server, tmp_path):
    data, digest = _publish(server, tmp_path)
    url = server.url + "/pack.sqlite.gz"
    dest = tmp_path / "dl" / "pack.sqlite"
    pull(dest, url=url)
    installed = dest.read_bytes()
    _publish(server, tmp_path, schema_version="3")
    with pytest.raises(APIError) as excinfo:
        pull(dest, url=url)
    assert "schema 3" in str(excinfo.value) and __version__ in str(excinfo.value)
    assert dest.read_bytes() == installed and not dest.with_name(dest.name + ".tmp").exists()
    assert _part(dest).exists()          # kept: an upgraded client unpacks it without downloading again


# ── local sources: file://, shares, plain paths ──────────────────────────
def test_pull_from_a_file_url_and_a_plain_path(tmp_path):
    share = tmp_path / "share dir"
    share.mkdir()
    data, digest = _gz_pack(tmp_path)
    (share / "latest.sqlite.gz").write_bytes(data)
    (share / "latest.sqlite.gz.sha256").write_text(f"{digest}  latest.sqlite.gz\n", encoding="utf-8")
    url = (share / "latest.sqlite.gz").as_uri()
    assert "%20" in url
    dest = tmp_path / "clerk" / "verification_pack.sqlite"
    report = pull(dest, url=url)
    assert report["verified"] is True and report["source_url"] == url and dest.is_file() and report["downloaded_bytes"] == len(data)
    assert json.loads(dest.with_name(dest.name + ".json").read_text(encoding="utf-8"))["checksum_url"] == url + ".sha256"
    # a plain path works too, and a resume from a share continues from the saved bytes
    dest2 = tmp_path / "clerk2" / "pack.sqlite"
    dest2.parent.mkdir()
    _part(dest2).write_bytes(data[:5000])
    _part(dest2).with_name(_part(dest2).name + ".json").write_text(json.dumps({"url": str(share / "latest.sqlite.gz"), "expected": digest}), encoding="utf-8")
    report = pull(dest2, url=str(share / "latest.sqlite.gz"))
    assert report["downloaded_bytes"] == len(data) - 5000 and report["verified"] is True and LocalClient(dest2).meta["decisions"] == "401"
    # no sidecar on the share: refused without --insecure; a wrong sidecar fails; a missing file is invalid input
    (share / "latest.sqlite.gz.sha256").unlink()
    with pytest.raises(PackIntegrityError, match="no checksum published"):
        pull(tmp_path / "x" / "pack.sqlite", url=url)
    assert pull(tmp_path / "x" / "pack.sqlite", url=url, insecure=True)["verified"] is False
    (share / "latest.sqlite.gz.sha256").write_text("f" * 64 + "  latest.sqlite.gz\n", encoding="utf-8")
    with pytest.raises(PackIntegrityError, match="checksum mismatch"):
        pull(tmp_path / "y" / "pack.sqlite", url=url)
    with pytest.raises(ValueError, match="no pack at"):
        pull(tmp_path / "z" / "pack.sqlite", url=str(share / "nothing.sqlite.gz"), insecure=True)


def test_share_and_drive_spellings_resolve_to_windows_paths():
    win = lambda u: str(local.local_source_path(u, "win32"))
    assert win("file://server/share/packs/latest.sqlite.gz") == r"\\server\share\packs\latest.sqlite.gz"
    assert win("file:////server/share/latest.sqlite.gz") == r"\\server\share\latest.sqlite.gz"
    assert win("file://///server/share/latest.sqlite.gz") == r"\\server\share\latest.sqlite.gz"
    assert win("file:///C:/Packs/a%20b/latest.sqlite.gz") == r"C:\Packs\a b\latest.sqlite.gz"
    assert win("file://localhost/C:/Packs/latest.sqlite.gz") == r"C:\Packs\latest.sqlite.gz"
    assert win(r"D:\packs\latest.sqlite.gz") == r"D:\packs\latest.sqlite.gz"
    assert win(r"\\server\share\latest.sqlite.gz") == r"\\server\share\latest.sqlite.gz"
    assert local.local_source_path("https://example.invalid/x.gz", "win32") is None
    assert str(local.local_source_path("file:///home/u/x.gz", "linux")) == "/home/u/x.gz"
    assert str(local.local_source_path("/mnt/share/x.gz", "darwin")) == "/mnt/share/x.gz"
    with pytest.raises(ValueError, match="Windows share"):
        local.local_source_path("file://server/share/x.gz", "linux")
    with pytest.raises(ValueError, match="unsupported"):
        local.local_source_path("ftp://h/x.gz")
    assert local.checksum_url("https://h/p/latest.sqlite.gz?download=true") == "https://h/p/latest.sqlite.gz.sha256?download=true"
    assert local.checksum_url("file://server/share/latest.sqlite.gz") == "file://server/share/latest.sqlite.gz.sha256"
    assert local.checksum_url(r"\\server\share\latest.sqlite.gz") == r"\\server\share\latest.sqlite.gz.sha256"


def test_default_pack_dir_per_platform(tmp_path):
    assert local.default_pack_dir("win32", {"LOCALAPPDATA": str(tmp_path / "Local")}).parts[-2:] == ("Local", "ocl")
    assert local.default_pack_dir("win32", {}).parts[-3:] == ("AppData", "Local", "ocl")
    assert local.default_pack_dir("linux", {"XDG_DATA_HOME": str(tmp_path / "xdg")}) == tmp_path / "xdg" / "ocl"
    assert local.default_pack_dir("darwin", {}).parts[-3:] == (".local", "share", "ocl")
    assert local.DEFAULT_PACK_DIR == local.default_pack_dir()


# ── opening a pack ───────────────────────────────────────────────────────
def test_schema_gate_on_open(tmp_path, capsys):
    assert LocalClient(_tiny_pack(tmp_path / "v2.sqlite", schema_version="2", filler_rows=0)).meta["schema_version"] == "2"
    assert LocalClient(_tiny_pack(tmp_path / "v2_1.sqlite", schema_version="2.1", filler_rows=0)).get("/health")["decisions"] == 1
    too_new = _tiny_pack(tmp_path / "v3.sqlite", schema_version="3", filler_rows=0)
    with pytest.raises(APIError) as excinfo:
        LocalClient(too_new)
    message = str(excinfo.value)
    assert "schema 3" in message and f"opencaselaw-cli {__version__}" in message and "schema 1 to 2" in message and "Upgrade" in message
    with pytest.raises(APIError, match="schema 3"):
        local.pack_report(too_new)
    with pytest.raises(APIError, match="not a verification pack"):
        LocalClient(_tiny_pack(tmp_path / "np.sqlite", with_meta=False, filler_rows=0))
    with pytest.raises(APIError, match="not a verification pack"):
        LocalClient(Path(__file__))
    # through the CLI, a too-new pack is a clear failure, not a traceback
    assert cli.main(["--local", str(too_new), "cite", "4A_747/2012", "--format", "json"]) == 4
    assert "schema 3" in capsys.readouterr().err
    # a pack copied by hand (no pull record) opens; verify says so and exits 4, info exits 0
    copied = _tiny_pack(tmp_path / "copied.sqlite", filler_rows=0)
    assert cli.main(["pack", "verify", "--path", str(copied), "--format", "json"]) == 4
    report = json.loads(capsys.readouterr().out)
    assert report["verified"] is None and "no pull record" in report["verification"] and report["schema_version"] == "1"
    assert cli.main(["pack", "info", "--path", str(copied), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["verified"] is None
    assert cli.main(["pack", "verify", "--path", str(tmp_path / "absent.sqlite"), "--format", "json"]) == 4
    assert json.loads(capsys.readouterr().out)["installed"] is False


def test_pack_opens_read_only_from_a_path_with_spaces_and_percent(tmp_path):
    folder = tmp_path / "Max Muster" / "100% ocl"
    folder.mkdir(parents=True)
    pack = _tiny_pack(folder / "verification pack.sqlite", filler_rows=0)
    uri = local._sqlite_uri(pack)
    assert uri.startswith("file:///") and uri.endswith("?mode=ro&immutable=1") and "%20" in uri and "%25" in uri and " " not in uri
    client = LocalClient(pack)
    assert client.get("/api/cite", {"reference": "4A_747/2012"})["citation_string"] == "Urteil 4A_747/2012 vom 5. April 2013"
    with pytest.raises(sqlite3.OperationalError):
        client._con.execute("INSERT INTO meta VALUES ('x', 'y')")
