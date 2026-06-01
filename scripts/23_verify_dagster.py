#!/usr/bin/env python3
"""Verify Dagster definitions load and list registered assets."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main() -> int:
    from dagster_pipeline import defs

    repo = defs.get_repository_def()
    asset_keys = sorted(key.to_user_string() for key in repo.asset_graph.get_all_asset_keys())
    print(f"Loaded {len(asset_keys)} assets:")
    for key in asset_keys:
        print(f"  - {key}")
    print(f"Jobs: {list(repo.job_names)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
