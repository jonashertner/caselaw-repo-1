"""Revoke the two pre-launch test Pro licenses (found 2026-07-29).

`cus_test` and `cus_test2` (both test@opencaselaw.ch, created 2026-03-28/29)
still validate as paid Pro. The maintainer key (`internal_test`) is
deliberately untouched. Revoke, not delete — reversible with
UPDATE ... SET status='active'.

Idempotent: a second run finds 0 active rows and changes nothing.
Root-only (writes the production billing DB); staged in the repo rather
than /tmp because /tmp got cleaned between staging and execution once.

    python3 /opt/caselaw/repo/scripts/revoke_test_licenses.py
"""
from __future__ import annotations

import datetime
import sqlite3
import sys

DB = "/opt/caselaw/repo/output/licenses.db"
TARGETS = ("cus_test", "cus_test2")


def main() -> int:
    try:
        c = sqlite3.connect(DB)
    except sqlite3.Error as e:
        print(f"cannot open {DB}: {e}", file=sys.stderr)
        return 1
    try:
        c.execute("PRAGMA busy_timeout=5000")
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        cur = c.execute(
            "UPDATE licenses SET status='revoked', cancelled_at=? "
            "WHERE status='active' AND stripe_customer IN (?, ?)",
            (now, *TARGETS),
        )
        c.commit()
        print(f"revoked {cur.rowcount} test license(s)")
        for r in c.execute(
            "SELECT stripe_customer, status FROM licenses "
            "WHERE stripe_customer LIKE '%test%' ORDER BY rowid"
        ):
            print(f"  {r[0]:16s} {r[1]}")
        return 0
    finally:
        c.close()


if __name__ == "__main__":
    raise SystemExit(main())
