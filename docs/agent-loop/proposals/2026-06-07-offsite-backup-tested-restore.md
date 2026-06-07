# Proposal: Offsite Backup With Tested Restore

Date: 2026-06-07

## What

Add an offsite backup and restore procedure for OpenCaseLaw assets that are not already protected by the HuggingFace decision parquet mirror.

## Why It Matters

The production system is single-maintainer and has non-rebuildable or expensive-to-rebuild state outside the published decision mirror: `state/coverage.db`, sidecar DBs, cantonal law/commentary/materialien DBs, deployed systemd/nginx configuration, and secrets/configuration needed to restore service. The technical overview and memory both identify this as an apex operational risk.

Read-only evidence from this iteration:
- Local repo search found no backup/restore script or existing agent-loop proposal.
- Prod `systemctl list-timers --all` / `list-units --all` filtered for backup/rclone/restic/borg/B2/storage showed only Ubuntu's `dpkg-db-backup.timer`, not an OpenCaseLaw data backup.

## Proposed Change

Implement a human-approved backup plan in two stages:

1. Minimal critical set:
   - `state/coverage.db`
   - `output/reference_graph.db`
   - `output/statutes.db`
   - `output/cantonal_laws.db`
   - `output/ok_commentaries.db`
   - `output/materialien.db`
   - deployed systemd unit files and nginx site config
   - encrypted secrets/configuration archive, created only by the maintainer after reviewing secret handling

2. Optional larger set:
   - selected raw JSONL shards and other sidecars whose rebuild time or upstream availability makes them operationally valuable
   - exclude clearly rebuildable giant artifacts such as `decision_structure.db` unless storage budget permits

Use an explicit allowlist, not a broad filesystem backup. Candidate targets: Hetzner Storage Box, Backblaze B2, or another maintainer-controlled offsite destination. A weekly timer is enough if paired with a manual restore test.

## Verification Plan

Before production deployment:
- Run backup script against a local or copied data subset.
- Verify manifest contains expected paths, sizes, hashes, and timestamp.
- Restore to `/tmp/opencaselaw-restore-test-*`.
- Open restored SQLite DBs read-only with `mode=ro`; for serving DBs also test `immutable=1` where applicable.
- Run a small restore checklist that proves at least one row can be read from each DB.

After production deployment:
- Confirm systemd timer exists and last result is success.
- Perform one restore drill to a temporary directory and record evidence in `docs/agent-loop/LOG.md`.

## Rollback

Disable and remove the backup timer/service. Leave existing production data untouched. Delete only backup artifacts created by the failed run after confirming the remote destination and path.

## Stop Conditions

Do not proceed autonomously if any of these are required:
- reading, printing, rotating, or moving secrets
- creating a paid storage resource
- destructive cleanup of local or remote backup artifacts
- backing up broad directories without an allowlist
- restoring over live production paths
