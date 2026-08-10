# Investigation (RESOLVED): `be_steuerrekurs` — portal up, protocol works, source DB empty. NOT recoverable now.

**Date:** 2026-06-21
**Origin:** `/maintain` heartbeat re-probed a `KNOWN_DEAD_SOURCES` court; this is the executed follow-up (user-approved).
**Outcome:** **No code change.** Keep `be_steuerrekurs` in `KNOWN_DEAD_SOURCES` (correctly classified). Re-probe periodically.

## What the heartbeat reported (and where it was wrong)

The heartbeat saw the portal serving a `tribunavtplus` GWT module and the old `tribunapublikation.nocache.js` as 404, and proposed "the portal migrated → recoverable." **That diagnosis was incorrect.** The 404 was a probe error: the scraper already sets `TRIBUNA_PATH="tribunavtplus"` (the `base_tribuna` default), so it already targets `.../tribunapublikation/tribunavtplus/loadTable` and auto-discovers the permutation. The real probe hit the wrong path (one segment short).

## What the executed verification found (2026-06-21)

Ran the real scraper session + live search against `https://www.strk-entscheide.apps.be.ch/tribunapublikation/tribunavtplus`:

- **Session works end-to-end:** GWT permutation auto-discovered (`03791D5E…`), `readConfigFile` returns a 128-char credential, `getBerechtigungen` ok.
- **Search returns a valid, EMPTY result set:** `//OK[0,…]` — a well-formed `tribunavtplus.client.db.PagingResultSet` envelope with **0 rows**, for **every filter tried** (`STRK`, `STR`, `SK`, `RK`, and empty/no-filter). Not `//EX` — so the `search()` signature still matches `SEARCH_FIELD_COUNT=21`; the DB simply has no data.
- **Control (method validation):** the *same code* against the working `be_verwaltungsgericht` portal (`vg-urteile.apps.be.ch`) returns `//OK[11420,…]` with real dockets/dates. So the probe is sound — `be_steuerrekurs` is genuinely empty, not a parse/protocol bug.

## Conclusion

The Bern Steuerrekurskommission portal is **up and the scraper is correct**, but its backing DB returns **0 decisions** (the "DB disconnected Feb 2026" state persists). There is **nothing to recover** — and removing it from `KNOWN_DEAD_SOURCES` would only re-enable false-positive freshness alerts for a genuinely empty source.

- **Historical data is safe:** all **343** decisions (2013-03-09 … 2025-12-16) are already in the corpus from the frozen entscheidsuche archive. Nothing was lost.
- **No new decisions exist** to fetch while the portal DB is empty. This is a be.ch-side outage, not our bug.

## Standing recommendation

- Keep in `KNOWN_DEAD_SOURCES`. Optionally refine the comment to "portal up + protocol OK but DB returns 0 (verified 2026-06-21); 343 historical preserved" — a one-line edit to `scripts/check_scraper_freshness.py` (gated; do on the next touch).
- The `[loop-safe]` KNOWN_DEAD_SOURCES re-probe (proposed in the maintenance plan) is exactly the right periodic check to catch the day the DB reconnects — at which point the existing scraper will just work, no code change.
