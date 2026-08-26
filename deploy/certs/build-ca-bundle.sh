#!/bin/sh
# Regenerate the scraper CA bundle: certifi + the intermediates in ./extra.
#
# Why this exists (2026-08-25): www.bger.ch renewed its certificate on
# 2026-08-24 and now serves a LEAF-ONLY chain — it no longer sends the
# DigiCert Global G2 TLS RSA SHA256 2020 CA1 intermediate. macOS/Windows
# clients still work because they chase the AIA extension; OpenSSL does
# not, so every requests-based scraper failed with
# CERTIFICATE_VERIFY_FAILED from 01:02 UTC on 2026-08-25.
#
# This adds NO trust anchor: each extra cert is admitted only if it
# already verifies against certifi (i.e. it is bound to a root we
# trusted anyway). It is a missing chain link, not new trust.
#
# Regenerated on every scraper start so a certifi upgrade cannot
# silently revert the fix. Writes atomically; on any failure the
# previous good bundle stays in place.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
out=${OCL_CA_BUNDLE:-/opt/caselaw/certs/ca-bundle.pem}
certifi=$(python3 -c "import certifi; print(certifi.where())")
tmp=$(mktemp "${out}.XXXXXX")
trap "rm -f \"$tmp\"" EXIT
cat "$certifi" > "$tmp"
for c in "$here"/extra/*.pem; do
    [ -e "$c" ] || continue
    if openssl verify -CAfile "$certifi" "$c" >/dev/null 2>&1; then
        cat "$c" >> "$tmp"
        echo "ca-bundle: added $(basename "$c")"
    else
        echo "ca-bundle: SKIPPED $(basename "$c") - does not chain to certifi" >&2
    fi
done
chmod 644 "$tmp"
mv -f "$tmp" "$out"
trap - EXIT
echo "ca-bundle: wrote $out ($(grep -c "BEGIN CERTIFICATE" "$out") certs, certifi=$certifi)"
