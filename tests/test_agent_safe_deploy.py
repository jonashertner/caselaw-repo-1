"""agent_safe_deploy must FAIL CLOSED: untracked files (control-plane, guardrails,
new scripts) must be scanned by default, not only with an opt-in flag — otherwise
the guard reports allowed:true while unreviewed untracked paths exist."""
import importlib.util
import subprocess
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "agent_safe_deploy",
    Path(__file__).resolve().parent.parent / "scripts" / "agent_safe_deploy.py",
)
asd = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(asd)


def _git(repo, *args):
    subprocess.run(["git", *args], cwd=str(repo), capture_output=True, text=True, check=False)


def _init(repo):
    _git(repo, "init")
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")


def test_changed_files_scans_untracked_by_default(tmp_path):
    _init(tmp_path)
    (tmp_path / "committed.py").write_text("x\n")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-m", "init")
    # an untracked control-plane file in a subdir must be caught with NO flag
    (tmp_path / "ops").mkdir()
    (tmp_path / "ops" / "secret_control.py").write_text("y\n")
    files = asd.changed_files(tmp_path)
    assert "ops/secret_control.py" in files, f"untracked file not scanned (fail-open): {files}"


def test_changed_files_scans_modified_tracked(tmp_path):
    _init(tmp_path)
    (tmp_path / "a.py").write_text("1\n")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-m", "init")
    (tmp_path / "a.py").write_text("2\n")
    files = asd.changed_files(tmp_path)
    assert "a.py" in files


def test_changed_files_excludes_gitignored(tmp_path):
    _init(tmp_path)
    (tmp_path / ".gitignore").write_text("secrets.env\n")
    (tmp_path / "committed.py").write_text("x\n")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-m", "init")
    (tmp_path / "secrets.env").write_text("KEY=1\n")  # gitignored → not a change to gate
    files = asd.changed_files(tmp_path)
    assert "secrets.env" not in files


def test_load_policy_fails_closed_on_missing_file(tmp_path):
    # A missing policy must NOT crash the guard — it must fail closed: under the
    # returned minimal policy, a real source path is "unknown" -> disallowed.
    policy = asd.load_policy(tmp_path / "does_not_exist.json")
    result = asd.evaluate(["mcp_server.py"], policy)
    assert result["allowed"] is False


def test_load_policy_fails_closed_on_corrupt_file(tmp_path):
    bad = tmp_path / "policy.json"
    bad.write_text("{not valid json")
    policy = asd.load_policy(bad)
    result = asd.evaluate(["tests/test_agent_foo.py"], policy)
    assert result["allowed"] is False
