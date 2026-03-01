#!/usr/bin/env python3
"""Check source breakdown and sample text for stub courts."""
import pyarrow.parquet as pq, pyarrow as pa, pyarrow.compute as pc, glob

tables = []
for f in sorted(glob.glob('output/dataset/*.parquet')):
    tables.append(pq.read_table(f, columns=['court', 'text_length', 'source', 'full_text', 'decision_id', 'source_url']))
t = pa.concat_tables(tables)

for court_name in ['sg_publikationen', 'zh_gerichte', 'bpatger', 'ch_vb', 'ta_sst',
                   'be_bvd', 'be_steuerrekurs', 'sh_gerichte', 'ag_gerichte', 'ai_gerichte',
                   'ar_gerichte', 'ur_gerichte', 'ch_bundesrat']:
    mask = pc.equal(t.column('court'), court_name)
    sub = t.filter(mask)
    n = len(sub)
    avg = int(pc.mean(sub.column('text_length')).as_py() or 0)

    # Source breakdown
    sources = {}
    for i in range(n):
        s = sub.column('source')[i].as_py() or 'none'
        sources[s] = sources.get(s, 0) + 1

    print(f'\n{court_name} (n={n:,}, avg_len={avg:,}):')
    for s, cnt in sorted(sources.items(), key=lambda x: -x[1]):
        print(f'  source={s}: {cnt:,}')

    # Sample: shortest text
    lens = sub.column('text_length').to_pylist()
    min_idx = lens.index(min(lens))
    min_text = sub.column('full_text')[min_idx].as_py()[:200]
    min_url = sub.column('source_url')[min_idx].as_py()
    min_id = sub.column('decision_id')[min_idx].as_py()
    print(f'  SHORTEST: {min_id} ({lens[min_idx]} chars) url={min_url}')
    print(f'  Text: {min_text!r}')

    # Sample: median text
    sorted_lens = sorted(enumerate(lens), key=lambda x: x[1])
    med_idx = sorted_lens[n//2][0]
    med_text = sub.column('full_text')[med_idx].as_py()[:200]
    med_id = sub.column('decision_id')[med_idx].as_py()
    print(f'  MEDIAN: {med_id} ({lens[med_idx]} chars)')
    print(f'  Text: {med_text!r}')
