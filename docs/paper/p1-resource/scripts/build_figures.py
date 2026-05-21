"""Build figures for the Paper 1 resource manuscript."""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


ROOT = Path(__file__).resolve().parents[1]
STATS = ROOT / "tables" / "corpus_graph_stats.json"
FIG_DIR = ROOT / "figures"


def main() -> int:
    stats = json.loads(STATS.read_text())
    matrix = {(r["src"], r["tgt"]): r["n"] for r in stats["cross_lang_matrix"]}
    langs = ["de", "fr", "it"]
    labels = ["DE", "FR", "IT"]

    counts = np.array(
        [[matrix.get((src, tgt), 0) for tgt in langs] for src in langs],
        dtype=float,
    )
    row_totals = counts.sum(axis=1, keepdims=True)
    shares = np.divide(counts, row_totals, out=np.zeros_like(counts), where=row_totals > 0)

    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(5.6, 3.9), constrained_layout=True)
    im = ax.imshow(shares, cmap="YlGnBu", vmin=0.0, vmax=1.0)

    ax.set_xticks(range(len(labels)), labels=labels)
    ax.set_yticks(range(len(labels)), labels=labels)
    ax.set_xlabel("Cited decision language")
    ax.set_ylabel("Source decision language")
    ax.set_title("Resolved citation-language shares")

    for i in range(len(langs)):
        for j in range(len(langs)):
            value = shares[i, j]
            color = "white" if value >= 0.55 else "black"
            ax.text(
                j,
                i,
                f"{value * 100:.1f}%\n{int(counts[i, j]):,}",
                ha="center",
                va="center",
                color=color,
                fontsize=9,
            )

    cbar = fig.colorbar(im, ax=ax, shrink=0.84)
    cbar.set_label("Row share")

    for suffix in ("pdf", "png"):
        fig.savefig(FIG_DIR / f"citation_language_flow.{suffix}", dpi=220)
    plt.close(fig)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
