"""Build the offline smoke-test fixture the installer workflow runs `ocl check` against:
a small verification pack (from the test-suite fixtures) and a Word draft that cites
decisions in it. Reuses the helpers of the client test suite so the fixture stays in
step with what the tests cover; pytest must be importable (the tests import it).

    python installer/smoke/make_fixture.py OUT_DIR   ->  OUT_DIR/pack.sqlite, OUT_DIR/draft.docx
"""
from __future__ import annotations

import sys
from pathlib import Path

CLIENT = Path(__file__).resolve().parents[2]  # clients/python


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    out = Path(argv[1]).resolve()
    out.mkdir(parents=True, exist_ok=True)
    sys.path[:0] = [str(CLIENT / "src"), str(CLIENT / "tests")]
    from test_check_document import MEMO, make_docx  # noqa: E402
    from test_local_pack import _build_pack  # noqa: E402

    pack, meta, _builder = _build_pack(out)
    draft = make_docx(out / "draft.docx", MEMO)
    print(f"pack={pack} decisions={meta.get('decisions')} paragraphs={meta.get('paragraphs')} draft={draft}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
