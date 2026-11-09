#!/usr/bin/env python3
"""Static Gold schema and canonical field-mapping compatibility (no runtime writes)."""
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from gold.spec import validate_mapping

if __name__ == "__main__":
    print(json.dumps(validate_mapping(), sort_keys=True))
