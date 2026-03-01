#!/usr/bin/env python3
"""Audit text_length distribution across all parquet files."""
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc
import glob

tables = []
for f in sorted(glob.glob('output/dataset/*.parquet')):
    tables.append(pq.read_table(f, columns=['court', 'text_length']))

t = pa.concat_tables(tables)
tlen = t.column('text_length')
court = t.column('court')

total = len(tlen)
empty = pc.sum(pc.equal(tlen, 0)).as_py()
sub500 = pc.sum(pc.and_(pc.greater(tlen, 0), pc.less(tlen, 500))).as_py()
sub1k = pc.sum(pc.and_(pc.greater_equal(tlen, 500), pc.less(tlen, 1000))).as_py()
sub3k = pc.sum(pc.and_(pc.greater_equal(tlen, 1000), pc.less(tlen, 3000))).as_py()
ok = total - empty - sub500 - sub1k - sub3k

print(f'Total: {total:,}')
print(f'Empty (0 chars): {empty:,} ({100*empty/total:.1f}%)')
print(f'Stub (1-499): {sub500:,} ({100*sub500/total:.1f}%)')
print(f'Short (500-999): {sub1k:,} ({100*sub1k/total:.1f}%)')
print(f'Thin (1000-2999): {sub3k:,} ({100*sub3k/total:.1f}%)')
print(f'Normal (3000+): {ok:,} ({100*ok/total:.1f}%)')
print()

# Per-court stats
courts_uniq = pc.unique(court).to_pylist()
rows = []
for c in courts_uniq:
    mask = pc.equal(court, c)
    lens = pc.filter(tlen, mask)
    n = len(lens)
    avg = pc.mean(lens).as_py() or 0
    sh = pc.sum(pc.less(lens, 1000)).as_py()
    emp = pc.sum(pc.equal(lens, 0)).as_py()
    rows.append((c, n, avg, sh, emp))

rows.sort(key=lambda x: x[2])
print('Courts with lowest avg text_length:')
for court_name, cnt, avg, sh, emp in rows[:30]:
    pct = 100 * sh / cnt if cnt else 0
    print(f'  {court_name:40s} n={cnt:>7,}  avg={int(avg):>8,}  <1k={sh:>6,} ({pct:5.1f}%)  empty={emp:>5,}')
