# Proposal: gate ECtHR content out of the CC0 HuggingFace mirror

- **Date:** 2026-07-27
- **Status:** PREPARED — not applied. Touches `publish.py` (pipeline gate, CLAUDE.md invariant #5).
- **Trigger:** review of user feedback asking for full HUDOC ingest surfaced the redistribution question.

## Finding

`scrapers/hudoc.py:277-282` states:

> A registry permission letter has been sent to secure bulk + commercial
> redistribution; until it returns, ingest runs but HF publication of
> ECtHR content is gated to a separate repository (not CC0).

Three parts of that comment are false as of today.

**1. The gate does not exist.** There is no court-based exclusion anywhere in the Python
tree. `export_parquet.py:333-335` enumerates courts with a bare
`SELECT DISTINCT court FROM decisions ORDER BY court`, and `publish.py:744-752` uploads
the whole directory with `allow_patterns="*.parquet"`, excluding only `graph/` and
`structure/`.

**2. The content is live on the CC0 repo.** Verified against the HF API on 2026-07-27:

```
data/ecthr_chamber.parquet        data/hudoc_ch.parquet
data/ecthr_committee.parquet      data/bge_egmr.parquet
data/ecthr_grand_chamber.parquet
```

`voilaj/swiss-caselaw` carries `license: cc0-1.0`; `lastModified 2026-07-26T15:45 UTC`.
`dataset_card.md:2` declares `license: cc0-1.0` and lines 270-273 list all four ECtHR
tables by name. Local staging in `output/dataset/` holds the same five files (~64 MB,
timestamped Jul 26 14:40).

**3. The permission letter was never sent.** `docs/letters/echr_registry_permission_2026-04-24.md`
carries `status: draft (awaiting send)`, committed 2026-04-25 in `af1c050`, untouched
since. A full-mailbox search returns no correspondence with any `coe.int` address in
either direction.

## Terms

ECHR copyright-and-disclaimer permits reproduction on three **cumulative** conditions:
source acknowledged as © ECHR-CEDH; purpose limited to private use or
information/education in connection with the Court's activities; and the reproduction
**free of charge**. Commercial use is expressly carved out and requires prior written
permission. The project's CC0 rationale (art. 5 URG — Swiss official texts are not
copyrightable) is jurisdiction-specific and does not reach a body asserting its own
copyright.

`bge_egmr` is a deliberate judgment call, not an obvious inclusion: those are Strasbourg
judgments as reproduced/translated in the official BGE collection. The Federal Court's
own translation apparatus is plausibly art. 5 URG material; the underlying judgment text
is not. The deny-list below includes it — narrow it if you disagree.

Separately worth noting: 96 rows in these tables are third-party translation summaries
(`[German Translation] summary by the Austrian Institute for Human Rights (ÖIM)` and
similar). `scrapers/hudoc.py:292-295` says third-party translations are excluded because
their copyright sits with the translators, not the Court. They are in the corpus anyway.

## Patch 1 — `export_parquet.py`

Add near the top, beside `DECISION_SCHEMA`:

```python
# Strasbourg judgment text is © ECHR-CEDH, not CC0. The ECHR reuse terms are
# conditioned on free-of-charge information/education use and carve out
# commercial use; art. 5 URG (Swiss official texts) does not reach it. Until
# written Registry permission is on file, these courts are excluded from the
# CC0 mirror at the source of the export rather than filtered downstream.
# See docs/agent-loop/proposals/2026-07-27-ecthr-cc0-redistribution-gate.md
EXCLUDED_COURTS = frozenset({
    "ecthr_chamber",
    "ecthr_committee",
    "ecthr_grand_chamber",
    "hudoc_ch",
    "bge_egmr",
})
```

Then in `export_from_db` (currently lines 333-336):

```diff
         total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
         courts = [r[0] for r in conn.execute(
             "SELECT DISTINCT court FROM decisions ORDER BY court"
         ).fetchall()]
-        logger.info(f"Exporting {total} decisions from {len(courts)} courts")
+        held_back = sorted(c for c in courts if c in EXCLUDED_COURTS)
+        courts = [c for c in courts if c not in EXCLUDED_COURTS]
+        if held_back:
+            logger.warning(
+                f"Excluding {len(held_back)} court(s) from the CC0 export "
+                f"(non-CC0 licence): {', '.join(held_back)}"
+            )
+        logger.info(f"Exporting {total} decisions from {len(courts)} courts")
```

## Patch 2 — `publish.py` step 4 (defence in depth)

Excluding at export is not sufficient on its own: `DATASET_DIR` is never cleaned, so a
stale `ecthr_chamber.parquet` from a previous run would still be picked up by
`DATASET_DIR.glob("*.parquet")` and uploaded. Refuse explicitly.

```diff
     parquet_files = list(DATASET_DIR.glob("*.parquet"))
     if not parquet_files:
         logger.error("  No Parquet files to upload")
         return False
+
+    # A stale parquet from a previous run survives in DATASET_DIR even after
+    # the court is dropped from the export, so the deny-list is enforced here
+    # too. Fail the step rather than upload — silently skipping would make a
+    # licence breach look like a successful publish.
+    from export_parquet import EXCLUDED_COURTS
+    blocked = [p for p in parquet_files if p.stem in EXCLUDED_COURTS]
+    if blocked:
+        logger.error(
+            "  Refusing to upload non-CC0 court(s) to %s: %s. "
+            "Delete the stale file(s) from %s and re-run.",
+            HF_REPO_ID, ", ".join(sorted(p.stem for p in blocked)), DATASET_DIR,
+        )
+        return False
```

## Patch 3 — `dataset_card.md`

Remove the four ECtHR rows from the court table at lines 270-273, or replace them with a
line recording that Strasbourg material is held out of the CC0 mirror pending Registry
permission. Leaving them listed while the files are absent is its own defect.

## Remote purge

`publish.py:751` already passes `delete_patterns="*.parquet"`, which prunes remote parquet
not present in the local folder. **So once Patch 1 lands and the stale local files are
removed, the next publish deletes the five remote files by itself** — no manual HF call
needed.

For immediate removal without waiting for a publish cycle:

```bash
# on the prod host, or anywhere with HF_TOKEN for voilaj
python3 - <<'PY'
from huggingface_hub import HfApi          # 1.4.1 installed
api = HfApi()
api.delete_files(
    repo_id="voilaj/swiss-caselaw",
    repo_type="dataset",
    delete_patterns=[
        "data/ecthr_chamber.parquet",
        "data/ecthr_committee.parquet",
        "data/ecthr_grand_chamber.parquet",
        "data/hudoc_ch.parquet",
        "data/bge_egmr.parquet",
    ],
    commit_message="Hold Strasbourg material out of the CC0 mirror pending ECtHR Registry permission",
)
PY

# and clear the stale local staging copies so the next publish does not re-upload
rm -f /opt/caselaw/repo/output/dataset/{ecthr_chamber,ecthr_committee,ecthr_grand_chamber,hudoc_ch,bge_egmr}.parquet
```

Note: two legacy copies also sit at the repo root on HF (`bge_egmr.parquet`,
`hudoc_ch.parquet`, outside `data/`). `delete_patterns="*.parquet"` in the publish step
only prunes under `data/`, so those need the explicit delete above regardless.

## Timing

The publish running now (started 03:30:35 UTC) was in step 2c at 12:09 UTC with ~830k of
~1.05M decisions processed. Steps 3 and 4 are still ahead of it, so this run will
re-export and re-upload the ECtHR parquets on current code. Applying Patch 1 mid-run is
not safe — `step_3` invokes `export_parquet.py` as a subprocess and would pick up a
half-edited module. Either apply after tonight's run completes, or stop the service
first.

## What this does not resolve

Excluding the courts from the CC0 mirror addresses redistribution. It does not address
the free-of-charge condition as against the CHF 5/month Word add-in, which serves ECtHR
content through the API. That needs the Registry permission, i.e. sending the letter that
has been in draft since April — and to the contact form named on the ECHR site rather
than the `publications@` address the draft currently targets.
