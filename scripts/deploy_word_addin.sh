#!/bin/bash
# deploy_word_addin.sh — sync the Word add-in source from the repo to
# nginx's docroot at /var/www/word-addin.
#
# The add-in lives at https://word.opencaselaw.ch and consists purely of
# static files (HTML/JS/CSS/PNG/XML). nginx serves them directly; there
# is no build step. Until this script existed the deploy was a manual
# scp-everything dance, which silently went stale (the live JS bundle on
# 2026-04-27 was last touched 2026-04-22 — 5 days behind main).
#
# Usage:
#   bash scripts/deploy_word_addin.sh                # deploy from local repo
#   ssh root@VPS bash /opt/caselaw/repo/scripts/deploy_word_addin.sh   # deploy from VPS checkout
#
# After running, refresh https://word.opencaselaw.ch in a browser; the
# index.html cache-bust query (?v=NN) ensures clients pull the new JS.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$REPO_DIR/tools/word-addin"
DST="/var/www/word-addin"

if [ ! -d "$SRC" ]; then
  echo "Source not found: $SRC" >&2
  exit 1
fi

if [ ! -d "$DST" ]; then
  echo "Destination not found: $DST" >&2
  echo "(this script must run on the VPS where nginx serves /var/www/word-addin)" >&2
  exit 1
fi

# Sync only the runtime surface — exclude tests, certification notes, and
# generators which are repo-only artefacts.
rsync -av --delete \
  --exclude='.DS_Store' \
  --exclude='tests/' \
  --exclude='generate_icons.py' \
  --exclude='appsource-listing.md' \
  --exclude='appsource-certification-notes.txt' \
  --exclude='screenshots/' \
  --exclude='.git*' \
  "$SRC/" "$DST/"

# Re-establish the OpenCaseLaw.xml symlink — Word's "Upload My Add-in"
# dialog historically defaults its filename filter to *.xml so we keep
# the OpenCaseLaw.xml alias for muscle memory.
ln -sfn manifest.xml "$DST/OpenCaseLaw.xml"

# Make sure nginx's www-data owns the deployed files.
chown -R www-data:www-data "$DST"

echo
echo "Deployed Word add-in to $DST"
echo "Live at https://word.opencaselaw.ch"
echo "Manifest version: $(grep -oE '<Version>[^<]+' "$DST/manifest.xml" | sed 's/<Version>//')"
