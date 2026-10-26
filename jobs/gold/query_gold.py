#!/usr/bin/env python3
"""Read a verified published Gold batch through Trino, refusing partial batches."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from gold.views import VIEW_NAMES
from gold.consumer import query

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--view", choices=VIEW_NAMES, required=True)
    parser.add_argument("--year", type=int, help="Optional validated reporting year; default sample starts at latest year")
    args = parser.parse_args()
    print(json.dumps(query(args.view, args.year), ensure_ascii=False, indent=2))
