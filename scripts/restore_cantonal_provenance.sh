#!/usr/bin/env bash
# Restore cantonal_laws.db to its documented provenance (2026-08-19).
#
# WHAT WENT WRONG
# The 18:30 UTC rebuild ran `python3 -m search_stack.build_cantonal_laws_db`
# with no arguments, so it took the module defaults. DIRECT_DIR defaults to
# the repo-local output/cantonal_laws_direct, which holds a single stale
# Apr-14 ZH shard (3 laws) instead of the 23 current shards on the corpus
# volume. build() replaces a canton wholesale when a direct shard exists and
# is non-empty *in bytes* (that ZH shard is 126 KB of full text), so:
#
#   * 22 cantons silently switched from official portal text to the
#     LexFind PDF fallback,
#   * ZH fell from 150 laws to 3,
#   * the total went 15,608 -> 28,957, which reads like growth and is not.
#
# THE CORRECT INPUTS ARE SPLIT ACROSS TWO FILESYSTEMS
#   direct  -> the volume   (23 shards, 12,567 laws, Aug 2)
#   lexfind -> repo-local   (26 shards, 30,328 laws, Apr 11)
# The volume's own lexfind_cantonal/ is empty, so overnight_cantonal_laws.sh
# cannot be used verbatim: it points --input-lexfind at the volume and would
# drop JU, SZ and VD entirely. Direct 12,567 + LexFind JU/SZ/VD 3,056 =
# 15,623, which is the 15,608-law corpus that was being served before.
#
# Run after the pipeline has exited and before 03:30 UTC (invariant #9).
set -euo pipefail
V=/mnt/HC_Volume_104655575/output
R=/opt/caselaw/repo/output
LOG=/opt/caselaw/repo/logs/cantonal_restore.log
exec >> "$LOG" 2>&1
echo "=== $(date -u +%FT%TZ) restore start ==="

# _build_law_names(conn, direct_dir.parent) looks for the abbreviation
# harvest beside the direct dir, so with --input-direct on the volume it has
# to be reachable there or the law_names table comes out with titles only
# and no abbreviations at all -- a silent loss of the whole naming feature.
# A symlink rather than a copy: the harvester writes repo-local, and a copy
# would go stale invisibly.
ln -sfn "$R/cantonal_abbreviations.jsonl" "$V/cantonal_abbreviations.jsonl"
echo "harvest: $(readlink -f "$V/cantonal_abbreviations.jsonl")" \
     "($(wc -l < "$V/cantonal_abbreviations.jsonl") rows)"

cd /opt/caselaw/repo
ionice -c 3 nice -n 10 python3 -m search_stack.build_cantonal_laws_db \
    --input-direct "$V/cantonal_laws_direct" \
    --input-lexfind "$R/lexfind_cantonal" \
    --output "$V/cantonal_laws.db"

echo "=== $(date -u +%FT%TZ) build done, verifying ==="
python3 - <<'PY'
import sqlite3
c = sqlite3.connect(
    "file:/opt/caselaw/repo/output/cantonal_laws.db?mode=ro&immutable=1",
    uri=True)
q = lambda s: c.execute(s).fetchone()[0]
laws = q("SELECT count(*) FROM laws")
print("laws      :", laws, "(expect ~15,600; 28,957 means the wrong dir again)")
print("law_names :", q("SELECT count(*) FROM law_names"))
for r in c.execute("SELECT name_type, count(*) FROM law_names GROUP BY 1"):
    print("   ", r[0], r[1])
print("cantons   :", q("SELECT count(DISTINCT canton) FROM laws"))
for ct in ("ZH", "JU", "SZ", "VD"):
    print(f"   {ct}: {q(f'SELECT count(*) FROM laws WHERE canton=\"{ct}\"')}")
# The LexWork probes have to be re-run: the earlier passes were against the
# wrong-provenance DB and prove nothing about this one.
for canton, name, want in (("ZG", "eg schkg", "231.1"),
                           ("AG", "gesg", "301.100"),
                           ("BE", "gemeindegesetz", "170.11")):
    r = c.execute(
        "SELECT sr_number FROM law_names WHERE canton=? AND name_folded=? "
        "ORDER BY CASE name_type WHEN 'abbreviation' THEN 0 "
        "WHEN 'short_title' THEN 1 ELSE 2 END LIMIT 1", (canton, name)).fetchone()
    got = r[0] if r else "MISS"
    print(f"   {canton}/{name} -> {got} {'OK' if got == want else 'CHECK'}")
PY
echo "=== $(date -u +%FT%TZ) restore complete ==="
