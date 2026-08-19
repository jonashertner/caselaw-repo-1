"""Count Ia/Ib-form reporter citations in window decisions (raw text).
Read-only; sizes the class the extraction grammar excludes."""
import re
import sqlite3
from collections import Counter

DB = "/mnt/HC_Volume_104655575/output/decisions.db"
PAT = re.compile(r"\b(BGE|ATF|DTF)\s+(\d{1,3})\s+I([ab])\s+(\d{1,4})\b")
# comparison: I..V uppercase forms in the same texts (rough, same grammar shape)
PAT_UP = re.compile(r"\b(BGE|ATF|DTF)\s+\d{1,3}\s+(?:I{1,3}|IV|V)\s+\d{1,4}\b")

c = sqlite3.connect(f"file:{DB}?mode=ro&immutable=1", uri=True)
n_dec = n_dec_hit = occ = occ_up = 0
by_lang = Counter()
sample = []
cur = c.execute("SELECT decision_id, language, full_text FROM decisions "
                "WHERE decision_date >= '2024-01-01'")
while True:
    rows = cur.fetchmany(400)
    if not rows:
        break
    for did, lang, txt in rows:
        n_dec += 1
        t = txt or ""
        hits = PAT.findall(t)
        occ_up += len(PAT_UP.findall(t))
        if hits:
            n_dec_hit += 1
            occ += len(hits)
            by_lang[lang] += len(hits)
            if len(sample) < 8:
                m = PAT.search(t)
                sample.append((did, m.group(0)))
print(f"decisions scanned: {n_dec:,}")
print(f"Ia/Ib-form citations: {occ:,} occurrences in {n_dec_hit:,} decisions")
print(f"by language: {dict(by_lang)}")
print(f"uppercase I-V form occurrences (same texts, raw): {occ_up:,}")
print(f"Ia/Ib share of raw reporter-citation surface: "
      f"{100*occ/max(1,occ+occ_up):.2f}%")
for s in sample:
    print(" ", s)
