"""Verify all 13 NSO files against a fresh, isolated native Iceberg table."""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.adapters.nso_adapter import NsoVietnamAdapter
from ingestion.config.registry import SourceRegistry
from ingestion.storage.bronze_writer import BronzeIcebergWriter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()
    registry = SourceRegistry()
    registry.load()
    config = registry.get_source("nso_vietnam")
    inventory = json.loads((ROOT / "config/silver/nso_source_tables.json").read_text(encoding="utf-8"))
    expected_columns = {name for source in inventory.values() for name in source["source_columns"]}
    namespace = f"bronze_nso_validation_{uuid.uuid4().hex[:12]}"
    run_id = f"nso_cold_start_{uuid.uuid4().hex[:12]}"
    writer = BronzeIcebergWriter(schema=namespace)
    expected_counts = {}
    with tempfile.TemporaryDirectory(prefix="nso_cold_start_") as staging:
        for chunk in NsoVietnamAdapter().extract_full(config, staging):
            files = chunk.data["_source_file"].dropna().unique().tolist()
            if len(files) != 1:
                raise AssertionError(f"Expected one source file per chunk, got {files}")
            filename = files[0]
            expected_counts[filename] = chunk.record_count
            writer.write_chunk(
                "nso_vietnam", chunk.data, run_id, run_id,
                source_checksum=chunk.checksum or "validation",
                chunk_id=chunk.chunk_id, source_snapshot_id=0, source_file=filename,
            )
    table_name = f'iceberg."{namespace}"."nso_vietnam"'
    _, schema_rows = writer.execute_query(f"DESCRIBE {table_name}")
    types = {row[0]: row[1] for row in schema_rows}
    if any(types.get(name) != "varchar" for name in expected_columns):
        raise AssertionError("NSO source fields must all be VARCHAR, including first-chunk NULL padding")
    _, counts = writer.execute_query(f"SELECT _source_file, count(*) FROM {table_name} GROUP BY 1")
    actual_counts = {name: count for name, count in counts}
    if actual_counts != expected_counts or len(actual_counts) != len(inventory):
        raise AssertionError(f"Missing or duplicated NSO records: {actual_counts}")
    _, names = writer.execute_query(
        f"SELECT DISTINCT t_nh_th_nh_ph_ FROM {table_name} WHERE t_nh_th_nh_ph_ IS NOT NULL"
    )
    if not names:
        raise AssertionError("Province source labels were lost during Bronze casting")
    report = {
        "status": "SUCCESS", "table": f"iceberg.{namespace}.nso_vietnam",
        "run_id": run_id, "source_files": actual_counts,
        "records": sum(actual_counts.values()), "source_fields": len(expected_columns),
        "all_source_fields_varchar": True, "distinct_raw_geography_labels": len(names),
        "verification_table_retained": True,
    }
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    print(rendered)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
