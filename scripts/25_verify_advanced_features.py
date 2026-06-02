#!/usr/bin/env python3
"""Run advanced features setup then benchmark."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    for module in ("src.setup_advanced_features", "src.benchmark_advanced"):
        result = subprocess.run(
            [sys.executable, "-m", module],
            cwd=ROOT,
            check=False,
        )
        if result.returncode != 0:
            return result.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
