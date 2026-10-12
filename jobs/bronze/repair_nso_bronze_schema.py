"""Plan/repair NSO's all-NULL DOUBLE columns; default execution is read-only."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.storage.bronze_writer import BronzeIcebergWriter
from ingestion.storage.metadata_repository import MetadataRepository
from ingestion.storage.nso_schema_repair import repair_nso_schema


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Commit verified all-NULL schema changes")
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()
    inventory = json.loads((ROOT / "config/silver/nso_source_tables.json").read_text(encoding="utf-8"))
    columns = {name for source in inventory.values() for name in source["source_columns"]}
    with MetadataRepository().source_lock("nso_vietnam"):
        report = repair_nso_schema(BronzeIcebergWriter(), columns, execute=args.execute)
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    print(rendered)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
