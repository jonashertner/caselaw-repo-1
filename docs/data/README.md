# Reference data

Small, versioned reference artifacts derived from the OpenCaseLaw corpus.
Regenerate rather than edit by hand.

## `law_codes.json` / `law_codes.csv`

Cross-language law-code table: abbreviation → SR number, for federal acts,
plus cantonal acts keyed (canton, systematic number).

**Why it exists.** The citation extractor
(`search_stack/reference_extraction.py`) emits *language-specific* law
codes by design. A consumer that does not know `OR == CO` or `ZGB == CC`
silently loses most French-language statute references and gets no error
at all — just quietly incomplete results. Our own QC module measures the
gap: `OR` alone is 125,592 statute edges, `OR` + `CO` is 359,195. This
table converts a silent wrong answer into a resolvable lookup.

Raised as [#74](https://github.com/jonashertner/opencaselaw/issues/74).

**How it is produced.** Derived, not curated:
`output/statutes.db` already carries `sr_number` with `abbr_de` /
`abbr_fr` / `abbr_it` per act, so the alias groups *are* the data.

```
python3 scripts/build_law_code_table.py \
    --statutes output/statutes.db \
    --cantonal output/cantonal_laws.db \
    --out docs/data/law_codes.json --csv docs/data/law_codes.csv
```

The derivation independently reproduces every pair
`quality/checks/statute_graph.py` maintains by hand:

| Alias | SR | Alias | SR |
|---|---|---|---|
| OR | 220 | CO | 220 |
| ZGB | 210 | CC | 210 |
| StGB | 311.0 | CP | 311.0 |
| BGG | 173.110 | LTF | 173.110 |

**Contents.** 1,172 federal acts (1,149 with multilingual abbreviations),
3,018 distinct aliases, 15,552 cantonal acts.

**Limits, stated plainly.**

- **Federal abbreviation coverage is 21 %.** Only 1,172 of 5,528 federal
  acts carry an abbreviation at all. The table is complete for what is
  abbreviated, not for the corpus.
- **No cantonal abbreviations.** `cantonal_laws.laws` has no abbreviation
  column — we never captured one. Published instead: canton, systematic
  number, title per language, source URL. Cantonal abbreviations also
  collide across cantons (`PBG`, `StG`, `VRPG` exist in many), so any
  future field must be canton-prefixed (`ZH_PBG`) to be a usable key.
- `alias_to_sr` maps an alias to one SR number, or to a list where an
  abbreviation is genuinely shared by several acts.

**Licence.** CC0-1.0, like the rest of the project-created data.
