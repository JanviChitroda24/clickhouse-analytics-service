#!/usr/bin/env python3
"""Run recovery/fault tolerance tests (requires Docker + API on :8000 for Test 2)."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    print("Prerequisites:")
    print("  1. Docker Desktop running: docker compose up -d")
    print("  2. API server (Test 2): uvicorn src.api.main:app --port 8000")
    print("")
    result = subprocess.run(
        [sys.executable, "-m", "src.recovery_test"],
        cwd=ROOT,
        check=False,
    )
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
