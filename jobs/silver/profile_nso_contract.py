"""Read-only, pinned NSO E1 profile and local Parquet export; never writes catalog."""
import argparse
from collections import Counter
import hashlib
import io
import json
from pathlib import Path
import re
import zipfile

import pandas as pd
from ingestion.recovery.nso import parse_csv, digest_rows
from ingestion.storage.bronze_writer import BronzeIcebergWriter
from ingestion.storage.minio_storage import MinioStorage


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--snapshot", type=int, default=3499764399995403202)
    p.add_argument("--raw-plan", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--parquet", required=True)
    p.add_argument("--local-dir", default="/opt/airflow/data/raw/nso")
    args = p.parse_args()
    plan = json.loads(Path(args.raw_plan).read_text(encoding="utf-8"))
    writer = BronzeIcebergWriter()
    table = writer.get_iceberg_catalog().load_table("bronze.nso_vietnam")
    if table.metadata.current_snapshot_id != args.snapshot:
        raise ValueError("STOP: current NSO snapshot drifted from B4")
    arrow = table.scan(snapshot_id=args.snapshot).to_arrow()
    frame = arrow.to_pandas()
    storage = MinioStorage()
    bucket, key = plan["archive_uri"].removeprefix("s3://").split("/", 1)
    content = storage.s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert hashlib.sha256(content).hexdigest() == plan["archive_sha256"]
    files, inventory = {}, {}
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        for name in sorted(archive.namelist()):
            data = archive.read(name)
            raw, encoding, headers = parse_csv(data)
            expected = next(f for f in plan["files"] if f["file"] == name)
            assert hashlib.sha256(data).hexdigest() == expected["sha256"]
            stored = frame[frame["_source_file"] == name]
            assert len(raw) == len(stored)
            assert digest_rows(raw) == digest_rows(stored[list(raw)])
            matrix = [c for c in raw if re.fullmatch(r"col_\d{4}|so_b_\d{4}", c)]
            files[name] = {"source_columns": list(raw), "headers": headers,
                "sha256": expected["sha256"], "rows": len(raw), "measure_columns": matrix,
                "all_source_null_rows": int(raw.isna().all(axis=1).sum()),
                "matrix_nonnull": int(raw[matrix].notna().sum().sum()),
                "matrix_null": int(raw[matrix].isna().sum().sum()), "encoding": encoding}
            if matrix:
                for label, group in raw.groupby("t_nh_th_nh_ph_", dropna=False):
                    if pd.isna(label):
                        continue
                    item = inventory.setdefault(label, {"raw_label": label, "source_tables": [], "physical_rows": 0})
                    item["source_tables"].append(name)
                    item["physical_rows"] += len(group)
    import pyarrow.parquet as pq
    pq.write_table(arrow, args.parquet)
    report = {"status": "PROFILED", "writes_shared_data": False, "snapshot": args.snapshot,
        "rows": len(frame), "schema": table.schema().model_dump(mode="json"), "files": files,
        "geography_inventory": sorted(inventory.values(), key=lambda item: item["raw_label"]),
        "metadata_txt": (Path(args.local_dir) / "metadata.txt").read_text(encoding="utf-8"),
        "local_parquet_sha256": hashlib.sha256(Path(args.parquet).read_bytes()).hexdigest(),
        "matrix_source_columns_verified_against_raw": True}
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "rows": len(frame), "snapshot": args.snapshot,
        "geography_labels": len(inventory), "all_source_null_rows_by_file": {k:v["all_source_null_rows"] for k,v in files.items()}}))


if __name__ == "__main__":
    main()
