"""upload_legislation_hf — push the legislation export to a PRIVATE HF dataset.

Uploads federal/fedlex.parquet + cantonal/<canton>.parquet (produced by
export_legislation.py) to ``voilaj/swiss-legislation`` (private=True). Creates
the repo if missing, writes the dataset card, and uploads the parquet tree as a
single commit via upload_folder.

This dataset is PRIVATE on purpose: Fedlex is public-domain and freely
redistributable, but the cantonal side carries LexFind-fallback rows whose
bulk republication we keep private. Federal-only public release stays a
separate, later decision.

Usage:
  HF_TOKEN from .env.publish is read automatically.
  python upload_legislation_hf.py --export /mnt/.../legislation_export \
      [--card dataset_card_legislation.md] [--dry-run]
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from huggingface_hub import HfApi

REPO_ID = "voilaj/swiss-legislation"


def _load_token() -> str:
    tok = os.environ.get("HF_TOKEN")
    if tok:
        return tok
    env = Path(".env.publish")
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line.startswith("HF_TOKEN=") and not line.startswith("#"):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("HF_TOKEN not found in environment or .env.publish")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--export", type=Path, required=True,
                    help="legislation_export dir (contains federal/ + cantonal/)")
    ap.add_argument("--card", type=Path, default=Path("dataset_card_legislation.md"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    fed = args.export / "federal" / "fedlex.parquet"
    can = args.export / "cantonal"
    if not fed.exists():
        raise SystemExit(f"missing {fed}")
    shards = sorted(can.glob("*.parquet"))
    if not shards:
        raise SystemExit(f"no cantonal shards in {can}")
    total_mb = sum(p.stat().st_size for p in [fed, *shards]) / 1e6
    print(f"federal: {fed.stat().st_size/1e6:.1f} MB", flush=True)
    print(f"cantonal: {len(shards)} shards, "
          f"{sum(p.stat().st_size for p in shards)/1e6:.1f} MB", flush=True)
    print(f"total upload: {total_mb:.1f} MB", flush=True)

    if args.dry_run:
        print("dry-run: not uploading", flush=True)
        return

    api = HfApi(token=_load_token())
    api.create_repo(REPO_ID, repo_type="dataset", private=True, exist_ok=True)
    print(f"repo ready (private): {REPO_ID}", flush=True)

    if args.card.exists():
        api.upload_file(path_or_fileobj=str(args.card), path_in_repo="README.md",
                        repo_id=REPO_ID, repo_type="dataset",
                        commit_message="dataset card")
        print("uploaded README.md", flush=True)

    api.upload_folder(
        folder_path=str(args.export), repo_id=REPO_ID, repo_type="dataset",
        allow_patterns=["federal/*.parquet", "cantonal/*.parquet"],
        commit_message="legislation export: fedlex + 26 cantonal shards",
    )
    print(f"DONE: uploaded federal + {len(shards)} cantonal shards to {REPO_ID}",
          flush=True)


if __name__ == "__main__":
    main()
