"""1280x640 GitHub social preview.

Background: the real citation subgraph among the 60 most-cited BGE
decisions (136 resolved edges), laid out with a deterministic
force-directed pass so the image is reproducible from the data.
Restrained: dark ground, thin edges, node size by citation count, text
carries the message. No court logos.
"""
import json
import math
import random

from PIL import Image, ImageDraw, ImageFont

W, H = 1280, 640
BG = (13, 15, 19)
EDGE = (48, 62, 80)
NODE = (120, 150, 180)
NODE_HI = (196, 214, 232)
TXT = (243, 245, 248)
SUB = (150, 162, 176)
ACCENT = (209, 36, 47)          # the dashboard's red

data = json.load(open("subgraph.json"))
nodes, edges = data["nodes"], data["edges"]
adj0 = {}
for a, b in edges:
    adj0.setdefault(a, set()).add(b); adj0.setdefault(b, set()).add(a)
cand = {n for n in nodes if len(adj0.get(n, ())) >= 2}
# largest connected component only: a stray chain reads as an artifact
seen, best = set(), []
for s in cand:
    if s in seen: continue
    comp, stack = [], [s]; seen.add(s)
    while stack:
        u = stack.pop(); comp.append(u)
        for v in adj0.get(u, ()):
            if v in cand and v not in seen:
                seen.add(v); stack.append(v)
    if len(comp) > len(best): best = comp
ids = best
nodes = {n: nodes[n] for n in ids}
edges = [(a, b) for a, b in edges if a in nodes and b in nodes]
print("groesste Komponente:", len(ids), "Knoten,", len(edges), "Kanten")

# deterministic layout
random.seed(7)
pos = {n: [random.uniform(0.1, 0.9), random.uniform(0.1, 0.9)] for n in ids}
adj = {n: set() for n in ids}
for a, b in edges:
    if a in adj and b in adj:
        adj[a].add(b); adj[b].add(a)

for step in range(400):
    k = 0.045
    disp = {n: [0.0, 0.0] for n in ids}
    for i, a in enumerate(ids):
        for b in ids[i + 1:]:
            dx = pos[a][0] - pos[b][0]; dy = pos[a][1] - pos[b][1]
            d2 = dx * dx + dy * dy + 1e-6
            f = k * k / d2 * 0.0034
            disp[a][0] += dx * f; disp[a][1] += dy * f
            disp[b][0] -= dx * f; disp[b][1] -= dy * f
    for a in ids:
        for b in adj[a]:
            dx = pos[a][0] - pos[b][0]; dy = pos[a][1] - pos[b][1]
            d = math.sqrt(dx * dx + dy * dy) + 1e-6
            f = (d - k) * 0.045
            disp[a][0] -= dx / d * f; disp[a][1] -= dy / d * f
    for n in ids:
        # gentle pull to centre instead of a hard clamp: clamping piles
        # nodes onto the frame and reads as a drawing artifact
        cx, cy = pos[n][0] - 0.5, pos[n][1] - 0.5
        pos[n][0] += disp[n][0] - cx * 0.004
        pos[n][1] += disp[n][1] - cy * 0.004

xs=[p[0] for p in pos.values()]; ys=[p[1] for p in pos.values()]
x0,x1,y0,y1 = min(xs),max(xs),min(ys),max(ys)
for n in ids:
    pos[n][0] = 0.03 + 0.94*(pos[n][0]-x0)/max(1e-6,x1-x0)
    pos[n][1] = 0.04 + 0.92*(pos[n][1]-y0)/max(1e-6,y1-y0)

img = Image.new("RGB", (W, H), BG)
d = ImageDraw.Draw(img, "RGBA")

# graph occupies the right two-thirds, text sits left
GX0, GY0, GW, GH = 585, 52, 662, 540
def P(n):
    return (GX0 + pos[n][0] * GW, GY0 + pos[n][1] * GH)

for a, b in edges:
    if a in pos and b in pos:
        d.line([P(a), P(b)], fill=EDGE + (140,), width=1)

mx = max(nodes.values())
for n in ids:
    x, y = P(n)
    r = 2.2 + 8.5 * (nodes[n] / mx) ** 0.6
    col = NODE_HI if nodes[n] > mx * 0.45 else NODE
    d.ellipse([x - r, y - r, x + r, y + r], fill=col + (225,))

# left-edge vignette so text stays legible over the graph
for i in range(560):
    a = int(255 * min(1.0, (560 - i) / 340.0))
    d.line([(i, 0), (i, H)], fill=BG + (a,))

def font(sz, bold=False):
    try:
        return ImageFont.truetype(
            "/System/Library/Fonts/Helvetica.ttc", sz, index=1 if bold else 0)
    except Exception:
        return ImageFont.load_default()

d.text((72, 150), "OpenCaseLaw", font=font(66, True), fill=TXT)
d.line([(74, 232), (74 + 92, 232)], fill=ACCENT, width=3)
d.text((72, 260), "1M+ Swiss court decisions", font=font(29), fill=TXT)
d.text((72, 300), "Federal and cantonal legislation", font=font(29), fill=TXT)
d.text((72, 356), "Citation graph  ·  MCP  ·  REST  ·  CC0",
       font=font(23), fill=SUB)
d.text((72, 404), "1875 – today  ·  DE / FR / IT", font=font(23), fill=SUB)
d.text((72, 470), "opencaselaw.ch", font=font(22, True), fill=SUB)

img.save("social-preview.png", "PNG", optimize=True)
print("geschrieben: social-preview.png", img.size)
