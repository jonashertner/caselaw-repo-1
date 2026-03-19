# March 20, 2026 Release — Exact Commands

Run these AFTER the 04:00 UTC publish completes (check `tail -5 logs/publish.log`).

## Step 1: Cut the release on VPS

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40

cd /opt/caselaw/repo
git pull --rebase origin main

python3 scripts/cut_paper_release.py \
  --release-id opencaselaw-paper-2026-03-20 \
  --stats-file docs/stats.json \
  --db /mnt/HC_Volume_104655575/output/decisions.db \
  --output-dir artifacts/paper_release_2026-03-20 \
  --graph-db /mnt/HC_Volume_104655575/output/reference_graph.db
```

## Step 2: Verify

```bash
# Check release match
cat artifacts/paper_release_2026-03-20/release_match_check.json | python3 -m json.tool | grep matched

# Check benchmark
cat artifacts/paper_release_2026-03-20/benchmark_report_release_matched.json | python3 -m json.tool | head -20

# Check paper was updated
head -15 docs/paper/opencaselaw-arxiv-final.md
```

## Step 3: Commit and push release bundle

```bash
git add artifacts/paper_release_2026-03-20/ docs/paper/opencaselaw-arxiv-final.md docs/paper/opencaselaw-arxiv-draft.md
git commit -m "release: opencaselaw-paper-2026-03-20 — frozen paper release bundle"
git push origin main
```

## Step 4: Apply remaining text fixes (from local)

```bash
git pull origin main

# Fix 1: Single-annotator statement (if not already stated)
# Fix 2: Replace March 18/19 with March 20
# Fix 3: Fix schema reference in dataset_card.md and README.md
# Fix 4: Fix federal source count (20 vs 19)
# Fix 5: Add citations for Open Legal Data and entscheidsuche.ch or remove mentions

git add -A && git commit -m "docs: final manuscript corrections for March 20 release"
git push origin main
```

## Step 5: Create GitHub release + Zenodo DOI

```bash
# Create GitHub release
gh release create paper-2026-03-20 \
  artifacts/paper_release_2026-03-20/manifest.json \
  artifacts/paper_release_2026-03-20/stats_snapshot.json \
  artifacts/paper_release_2026-03-20/benchmark_golden.json \
  artifacts/paper_release_2026-03-20/benchmark_report_release_matched.json \
  --title "OpenCaseLaw Paper Release 2026-03-20" \
  --notes "Frozen release bundle for arXiv submission. See docs/paper/opencaselaw-arxiv-final.md."

# Zenodo: create manually at zenodo.org/deposit/new, upload same files, get DOI
```

## Step 6: Final hostile review

Check four things:
1. Every number matches the March 20 bundle
2. Every claim is supported by a repo artifact or citation
3. Every limitation is stated plainly
4. No local-path leakage or draft wording remains

## Step 7: Align README + dataset card + paper

```bash
# Verify counts match
python3 -c "
import json
stats = json.load(open('artifacts/paper_release_2026-03-20/stats_snapshot.json'))
print(f'Release count: {stats[\"total\"]}')
# grep for this number in all public surfaces
import subprocess
for f in ['README.md', 'dataset_card.md', 'docs/paper/opencaselaw-arxiv-final.md']:
    count = subprocess.run(['grep', '-c', str(stats['total']), f], capture_output=True, text=True).stdout.strip()
    print(f'  {f}: {count} occurrences')
"
```

## Go/No-Go

Submit if:
- release_match_check.json says "matched": true
- benchmark numbers in paper match benchmark_report_release_matched.json
- README, dataset_card.md, and paper all use the same total count
- no March 18/19 dates remain in the paper
