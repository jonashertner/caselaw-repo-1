# Zenodo deposit bundle — OpenCaseLaw Paper 1 (Resource)

Snapshot: **2026-05-21**

This directory is a self-contained Zenodo deposit for the
*OpenCaseLaw: A Verifiable Multilingual Citation Graph for Swiss
Jurisprudence* paper. Upload all six files (excluding this README,
which is metadata) plus paste in the `zenodo_metadata.json` fields.

## Files

| File | Purpose | SHA-256 (in `checksums.sha256`) |
|---|---|---|
| `paper.pdf` | Compiled paper, 14 pages | `8594aaca…` |
| `corpus_graph_stats_2026-05-21.json` | Frozen corpus + graph statistics; single source of truth for every count in the paper | `87566b50…44fc1` |
| `precision_proxies_2026-05-21.json` | Re-run of `benchmarks/citation_precision_proxies.py` against the 2026-05-21 graph; matches the released E_link denominator (8,102,236 link-edge rows) exactly | `190ec47c…dc661` |
| `integrity_manifest_2026-05-21.json` | RFC-6962 build manifest: root hex, leaf encoding, decision count, build timing | `6e74438c…0968bc` |
| `integrity_root_2026-05-21.bin` | The 32-byte RFC-6962 Merkle root (hex prefix `1b597b92`) | `1fbe2681…ae2ab` |
| `integrity_root_2026-05-21.bin.ots` | OpenTimestamps Bitcoin anchor for the root | `de78f368…42e73` |
| `checksums.sha256` | SHA-256 of every uploaded file | — |
| `zenodo_metadata.json` | Title/description/keywords/related-identifiers fields for the Zenodo deposit form | — |

## How to mint the DOI

### Option A — Zenodo web UI (recommended for first deposit)

1. Log in at https://zenodo.org
2. New Upload → Files: drag in the six data files + `checksums.sha256`
3. Paste the fields from `zenodo_metadata.json` into the matching form fields:
   - Upload type: `Publication → Preprint`
   - Title, description, creators (Jonas Hertner), keywords, language
   - License: `CC-BY-4.0`
   - Related identifiers: paste the four URL entries
4. Save draft → Publish
5. The minted DOI will be of the form `10.5281/zenodo.<N>`. Capture both:
   - The version-specific DOI
   - The concept DOI (resolves to "latest version")

### Option B — Zenodo REST API

```bash
ZENODO_TOKEN=...      # personal access token, deposit:write scope
DEPOSIT_DIR=docs/paper/p1-resource/zenodo_deposit

# 1. Create deposition
curl -s -X POST "https://zenodo.org/api/deposit/depositions" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $ZENODO_TOKEN" \
    -d "{\"metadata\": $(jq '.' $DEPOSIT_DIR/zenodo_metadata.json)}" \
    > /tmp/deposit.json
BUCKET_URL=$(jq -r '.links.bucket' /tmp/deposit.json)
DEPOSIT_ID=$(jq -r '.id' /tmp/deposit.json)

# 2. Upload each file via the bucket
for f in paper.pdf corpus_graph_stats_2026-05-21.json \
         precision_proxies_2026-05-21.json \
         integrity_manifest_2026-05-21.json \
         integrity_root_2026-05-21.bin \
         integrity_root_2026-05-21.bin.ots \
         checksums.sha256; do
  curl -s --upload-file "$DEPOSIT_DIR/$f" \
       -H "Authorization: Bearer $ZENODO_TOKEN" \
       "$BUCKET_URL/$f" > /dev/null
done

# 3. Publish
curl -s -X POST \
    "https://zenodo.org/api/deposit/depositions/$DEPOSIT_ID/actions/publish" \
    -H "Authorization: Bearer $ZENODO_TOKEN"
```

## After the DOI is minted

Update three files in this repo with the version-specific DOI:

1. `docs/paper/p1-resource/ARXIV_SUBMISSION.md` — append the DOI to the
   pre-submission checklist and the Comments line
2. `docs/paper/p1-resource/paper.tex` — optional: add a `\thanks{}` or
   footnote citing the Zenodo DOI alongside the GitHub/HF links
3. `README.md` (repo root) — add the DOI badge

Then re-tag and re-bbl if `paper.tex` is touched.

## Verification

The integrity root can be independently verified:

```bash
ots verify integrity_root_2026-05-21.bin.ots
# expected: success after the OTS calendar pool upgrades the proof
# to a confirmed Bitcoin block
```

Per-decision inclusion proofs are served at:
```
https://mcp.opencaselaw.ch/api/integrity/<decision_id>
```
