# Investigation: GitHub #57 — `chamber` wrong for `court=bger`. One function causes both patterns.

**Date:** 2026-07-26
**Origin:** GitHub issue #57 (`sglbot`, continuing their #42–#53 series).
**Status:** Root-caused, verified, and **F1–F3 implemented** in `scrapers/bger.py` (uncommitted; 14 tests in `tests/test_bger_chamber_issue_57.py`). The shipped resolver was re-checked against the stored `full_text` of all eight decisions the issue cites: **8/8 correct**, where production today gets 7/8 wrong.

**F4 (correcting the existing records) is NOT done and needs approval** — see why below.

## Verdict on the report

Both patterns are real and reproduce. The reporter's *diagnosis* is right for Pattern B and incomplete for Pattern A — the two are not independent, as the issue assumes. **They are the same eight lines of code.**

Verified against the live REST API:

| decision | `chamber` (API) | division named in `full_text` |
|---|---|---|
| `bger_7B_311_2023` | I. Strafrechtliche Abteilung | **II. strafrechtliche Abteilung** |
| `bger_7B_185_2023` | I. Strafrechtliche Abteilung | **IIe Cour de droit pénal** |
| `bger_7F_9_2024` | I. Strafrechtliche Abteilung | **II. strafrechtliche Abteilung** |
| `bger_6B_1330_2023` (control) | I. Strafrechtliche Abteilung | I. strafrechtliche Abteilung ✓ |

`GET /api/decisions?court=bger&chamber=Bundesstrafgericht` → **847**, `total_is_lower_bound: false`.

## Root cause

`scrapers/bger.py:1274-1289` derives the chamber by scanning **the entire page text** for any division name in `ABTEILUNG_MAP`, in any of de/fr/it, longest name first, first hit wins:

```python
sorted_abt = sorted(ABTEILUNG_MAP.items(),
                    key=lambda kv: max(len(kv[1][lang]) for lang in ["de","fr","it"]),
                    reverse=True)
for _, info in sorted_abt:
    for lang_key in ["de", "fr", "it"]:
        if info[lang_key].lower() in text_lower:   # <-- unanchored substring, whole document
            meta["chamber"] = info["de"]
```

and `scrapers/bger.py:1142` gives that scan **priority over the docket**:

```python
chamber = (meta.get("chamber") or stub.get("vkammer")
           or self._docket_to_abteilung(docket, language))
```

### Pattern A — 7B/7F labelled I. instead of II.

`ABTEILUNG_MAP` has **no entry for the II. strafrechtliche Abteilung**. The 2023 reorganisation that created it, and moved criminal-procedure appeals to the 7B/7F dockets, was never reflected in the map.

So when the scan reads a 7B decision whose rubrum says *"II. strafrechtliche Abteilung"*, the only criminal division it knows is `"I. Strafrechtliche Abteilung"` — and `"i. strafrechtliche abteilung"` **is a substring of** `"ii. strafrechtliche abteilung"`. The match succeeds on the wrong division.

The length-descending sort at 1277-1281 exists to prevent exactly this (its comment: *"match 'II. Öffentlich-' before 'I. Öffentlich-'"*). It works for the public-law divisions because both I. and II. are in the map. It cannot work here, because there is nothing longer to match first.

Same mechanism in FR and IT: `"cour de droit pénal"` is a substring of `"IIe Cour de droit pénal"`, `"corte di diritto penale"` of `"II Corte di diritto penale"`. That is why the issue's FR and IT samples fail identically.

### Pattern B — a foreign court in `chamber` (847 records)

`ABTEILUNG_MAP["CH_BGer_007"]` is `"Beschwerdekammer des Bundesstrafgerichts"` — **not a Federal Supreme Court division at all**, but the Federal Criminal Court body whose decisions BGer hears appeals against. At 40 characters it is the **longest name in the map**, so the length-descending sort checks it **first**.

Result: any BGer decision that mentions the Federal Criminal Court's Complaints Chamber anywhere in its text — as the lower instance, or merely in passing — is labelled with it, whatever division actually decided. The sort intended as a precision fix is what makes this the *most likely* outcome rather than the least.

Docket-prefix distribution of the 847 (500 sampled):

| prefix | n | prefix | n |
|---|---:|---|---:|
| 1C | 229 | 1F | 6 |
| 1B | 124 | 7F | 4 |
| 7B | 68 | 9C, 2C, 6F | 2 each |
| 6B | 59 | 5D, 12T, 5A, 4A | 1 each |

The spread across every division confirms this is the text scan, not the docket map. `bger_5A_320_2019` — the issue's telling example, where the Federal Criminal Court appears only in a remark about a misaddressed filing — is exactly this.

`CH_BGer_007`'s docket entry (`7B, 7D, 7E, 7F, 7G, 7X, 7Y`) is separately wrong post-2023 and would surface whenever the text scan finds nothing.

## Proposed fix

**F1 — `ABTEILUNG_MAP`.** Add the II. strafrechtliche Abteilung (de/fr/it) with prefixes `7B`, `7F`. Remove those prefixes from `CH_BGer_007`, and drop `"Beschwerdekammer des Bundesstrafgerichts"` from the map entirely — it is not a BGer chamber and has no business being assignable as one for `court=bger`.

**F2 — anchor the scan.** Two independent hardenings, both needed:
   - **Word-boundary match**, so `I.` cannot match inside `II.`. A plain `re.search(r"(?<![IVX])" + re.escape(name), text, re.I)` fixes the roman-numeral prefix class.
   - **Scope the search to the rubrum/closing formula** rather than the whole document. Both are present in the stored text and follow a fixed per-language format, as the reporter notes.

**F3 — cross-court guard.** Never assign a `chamber` naming a court other than the record's own `court`. Cheap, and it clears the 847 on its own even if F2 regresses.

**F4 — re-derivation. NOT DONE, needs approval.** F1–F3 only affect newly scraped records (~1,200 7B/7F a year). The existing wrong values stay wrong.

Two things make this bigger than it looks:

- **`decisions.db` is derived, not authoritative.** `output/decisions/*.jsonl` is the source of truth and `build_fts5.py --full-rebuild` regenerates the DB from it every night. An `UPDATE decisions SET chamber=...` would be silently reverted by the next publish, and it would violate invariant #1 besides. The correction has to land in the shard — and `output/decisions/bger.jsonl` is **1.55 GB**.
- So this is a shard rewrite, not a column update: stream the JSONL, correct the affected records, write to a temp file, `os.replace()`. Run against a copy, verify counts, then let a normal publish pick it up.

Precise scope, measured on the live DB rather than estimated:

| set | n | current value | correct value |
|---|---:|---|---|
| 7B/7F labelled I. | 3,420 | I. Strafrechtliche Abteilung | II. Strafrechtliche Abteilung |
| 7B/7F with the foreign court | 72 | Beschwerdekammer des BStGer | II. Strafrechtliche Abteilung |
| 7B/7F with an unrelated division | 22 | I./II. Öffentlich-rechtliche etc. | II. Strafrechtliche Abteilung |
| foreign court on other dockets | 775 | Beschwerdekammer des BStGer | re-derive (docket map) |
| **total** | **~4,289** | | |

Already correct and to be left alone: 21 of 3,535 7B/7F records carry the native-language form (`IIe Cour de droit pénal` ×13, `II. strafrechtliche Abteilung` ×7, `II Corte di diritto penale` ×1). These came from `stub["vkammer"]`, the portal's own label on the Neuheiten listing — evidence that vkammer is reliable where present.

Note this is lower than the issue's estimated ≈6,700. Their figure extrapolates a 3.5 % error rate from a sample; the table above counts the records that are actually identifiable as wrong. The difference is mostly their "33 % prefix-only" bucket, which is uninformative rather than incorrect.

## Why this is worth doing promptly

`chamber` is the field a Swiss citation needs, and for the entire 2023-onward criminal docket it is wrong in ~99 % of records. Per the issue's sample the error rate across `bger` is ~3.5 % (≈6,700 of 192,084). An LLM agent citing from API metadata misattributes systematically — and MCP contract R1 exists precisely so that agents can trust what this server returns.

Note the reporter takes their own citations from the rubrum, not from `chamber`, which is why they caught it. Clients that trust the field do not have that safety net.

## Suggested reply to #57

Confirm both patterns, credit the reproduction, correct the record on Pattern A/B being one cause rather than two, and state that F1–F3 plus the backfill are queued. Their `zh_obergericht` 14/14 counter-check is right: this is `bger`-specific.
