#!/usr/bin/env python3
"""Read-only static/physical baseline; no Spark session and no Gold writes."""
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from gold.preflight import inspect

if __name__ == "__main__":
    report = inspect()
    print(json.dumps(report, indent=2, ensure_ascii=False))
    raise SystemExit(0 if report["status"] == "READY" else 2)
