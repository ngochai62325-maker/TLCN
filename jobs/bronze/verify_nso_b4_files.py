"""Read-only physical-file, schema and original-projection verification after B4."""
import argparse
from datetime import datetime
import json
from pathlib import Path

from recover_nso import load_inputs
from ingestion.recovery.nso import digest_rows
from ingestion.storage.bronze_writer import BronzeIcebergWriter
from ingestion.storage.minio_storage import MinioStorage


def verify(args):
    writer = BronzeIcebergWriter(trino_port=8080)
    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    commit = json.loads(Path(args.result).read_text(encoding="utf-8"))
    before = json.loads(Path(args.before).read_text(encoding="utf-8"))
    table, frame, _, _ = load_inputs(writer, args.local_dir)
    columns, rows = writer.execute_query("DESCRIBE iceberg.bronze.nso_vietnam")
    sql_schema = [dict(zip(columns, row)) for row in rows]
    sql_types = {row["Column"]: row["Type"] for row in sql_schema}
    expected = {"string": "varchar", "long": "bigint", "timestamptz": "timestamp(6) with time zone"}
    types_match = all(sql_types.get(f.name) == expected[str(f.field_type)] for f in table.schema().fields)
    old = table.scan(snapshot_id=plan["current_snapshot_id"]).to_arrow().to_pandas()
    preserved = digest_rows(old) == digest_rows(frame[list(old)])
    storage = MinioStorage()
    bucket, key = table.metadata_location.removeprefix("s3://").split("/", 1)
    prefix = key.split("/metadata/", 1)[0] + "/"
    cutoff = datetime.fromisoformat(commit["commit_intent_at"]).replace(microsecond=0)
    written = [{"uri": "s3://" + bucket + "/" + obj["Key"], "bytes": obj["Size"],
                "etag": obj["ETag"], "last_modified": str(obj["LastModified"])}
               for obj in storage.list_objects(bucket, prefix) if obj["LastModified"] >= cutoff]
    retained = []
    for item in before["data_files"]:
        bucket, key = item["path"].removeprefix("s3://").split("/", 1)
        stat = storage.s3_client.head_object(Bucket=bucket, Key=key)
        if stat["ContentLength"] != item["bytes"]:
            raise ValueError("STOP: retained baseline data file size changed")
        retained.append({"uri": item["path"], "bytes": stat["ContentLength"], "head_verified": True})
    bucket, key = before["metadata_location"].removeprefix("s3://").split("/", 1)
    storage.s3_client.head_object(Bucket=bucket, Key=key)
    national = frame[frame["_source_file"] == "V06.12.csv"]
    matrices = frame[frame["_source_file"] != "V06.12.csv"]
    measures = [c for c in plan["schema_additions"] if c != "t_nh_th_nh_ph_"]
    national_measures = [f["name"] for f in plan["schema_before"]["fields"]
                         if f["name"] not in ("gi_tr_v_ch_s_ph_t_tri_n", "nam") and not f["name"].startswith("_")]
    checks = {"trino_schema_49": len(sql_schema) == 49, "native_trino_types_match": types_match,
              "retained_18_field_projection_exact": preserved, "all_13_baseline_data_files_still_exist": len(retained) == 13,
              "baseline_metadata_file_exists": True,
              "recorded_snapshot_still_current": table.metadata.current_snapshot_id == commit["shared_recovery"]["snapshot_after"]}
    labels = national["gi_tr_v_ch_s_ph_t_tri_n"].value_counts(dropna=False)
    report = {"status": "PASSED" if all(checks.values()) else "STOP", "writes_shared_data": False,
        "checks": checks, "trino_schema": sql_schema, "written_objects_since_commit_intent": written,
        "written_objects_selection": "NSO table prefix; LastModified >= floor(commit_intent_at, second)",
        "retained_baseline_data_files": retained, "original_18_field_projection_preserved": preserved,
        "phase_e_profile": {"national_rows": len(national),
            "national_statistic_labels": [{"label": label, "rows": int(count)} for label, count in labels.items()],
            "national_periods": sorted(national["nam"].dropna().unique().tolist()),
            "national_null_period_rows": int(national["nam"].isna().sum()),
            "national_null_period_examples": national[national["nam"].isna()][["nam", "gi_tr_v_ch_s_ph_t_tri_n", *national_measures]].to_dict("records"),
            "national_nonnull_measure_cells": int(national[national_measures].notna().sum().sum()),
            "matrix_rows": len(matrices), "matrix_null_measure_cells": int(matrices[measures].isna().sum().sum()),
            "matrix_unique_raw_labels": sorted(matrices["t_nh_th_nh_ph_"].dropna().unique().tolist()),
            "provisional_2024_nonnull_cells": int(matrices["so_b_2024"].notna().sum())}}
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "checks": checks, "written_objects": len(written),
        "phase_e_profile": {k: v for k, v in report["phase_e_profile"].items() if k != "matrix_unique_raw_labels"}}, ensure_ascii=True))
    if not all(checks.values()):
        raise ValueError("STOP: schema/files/projection verification failed; retain evidence")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--before", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--local-dir", default="/opt/airflow/data/raw/nso")
    verify(parser.parse_args())
