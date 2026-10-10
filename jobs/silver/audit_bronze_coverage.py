"""Read-only inventory of the live catalog, ingestion metadata and raw NSO.

Never creates namespaces, tables, buckets or ingestion runs. Output is a local
JSON evidence file; service errors are recorded rather than counted as success.
Run from the repository root with PYTHONPATH=src.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import yaml

from ingestion.storage.bronze_writer import BronzeIcebergWriter, sanitize_column_name


def read_csv_bytes(content):
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            return pd.read_csv(io.BytesIO(content), encoding=encoding, dtype=str)
        except UnicodeDecodeError:
            continue
    raise ValueError("No supported CSV encoding")


def audit(trino_port=8080, rest_uri="http://localhost:8181", metadata_file=None):
    writer = BronzeIcebergWriter(trino_port=trino_port)
    result = {"observed_at": datetime.now(timezone.utc).isoformat(), "writes_shared_data": False,
              "errors": [], "tables": {}, "contracts": {}, "nso_files": []}
    registry = yaml.safe_load(Path("config/ingestion/source_registry.yaml").read_text(encoding="utf-8"))["sources"]
    result["registry_sources"] = sorted(registry)

    def query(sql):
        columns, rows = writer.execute_query(sql)
        return [dict(zip(columns, row)) for row in rows]

    response = requests.get(f"{rest_uri}/v1/namespaces", timeout=20)
    response.raise_for_status()
    result["namespaces"] = response.json()["namespaces"]
    for namespace in result["namespaces"]:
        ns = ".".join(namespace)
        response = requests.get(f"{rest_uri}/v1/namespaces/{ns}/tables", timeout=20)
        response.raise_for_status()
        for identifier in response.json()["identifiers"]:
            table = identifier["name"]
            qualified = f"iceberg.{ns}.{table}"
            metadata = requests.get(f"{rest_uri}/v1/namespaces/{ns}/tables/{table}", timeout=20)
            metadata.raise_for_status()
            meta = metadata.json()["metadata"]
            evidence = {"qualified_table": qualified, "schema": meta["schemas"],
                        "current_snapshot_id": meta.get("current-snapshot-id"),
                        "snapshot_count": len(meta.get("snapshots", [])), "snapshots": meta.get("snapshots", [])}
            result["tables"][table] = evidence
            if ns != "bronze":
                continue
            try:
                evidence["totals"] = query(f"SELECT count(*) AS rows, count(DISTINCT _source_file) AS files, "
                    f"count(DISTINCT _ingestion_run_id) AS runs, count(DISTINCT _source_checksum) AS checksums, "
                    f"min(_ingestion_timestamp) AS first_ingestion, max(_ingestion_timestamp) AS last_ingestion, "
                    f"count_if(_source_file IS NULL OR _source_checksum IS NULL OR _ingestion_run_id IS NULL "
                    f"OR _source_snapshot_id IS NULL OR _ingestion_batch_id IS NULL OR _ingestion_chunk_id IS NULL "
                    f"OR _ingestion_timestamp IS NULL OR _source_id IS NULL) AS missing_lineage, "
                    f"count_if(_source_snapshot_id = 0) AS unlinked_snapshot_rows FROM {qualified}")[0]
                evidence["files"] = query(f"SELECT _source_file, _source_checksum, _source_snapshot_id, "
                    f"_ingestion_run_id, count(*) AS rows FROM {qualified} GROUP BY 1,2,3,4 ORDER BY 1,4")
                evidence["sample"] = query(f"SELECT * FROM {qualified} LIMIT 3")
                fields = [field["name"] for field in meta["schemas"][-1]["fields"]]
                temporal = [c for c in fields if c in ("year", "nam", "period", "market_year") or c.startswith("col_20")]
                evidence["temporal"] = {c: query(f'SELECT min(cast("{c}" AS varchar)) AS minimum, '
                    f'max(cast("{c}" AS varchar)) AS maximum, count("{c}") AS nonnull FROM {qualified}')[0]
                    for c in temporal if not c.startswith("col_")}
            except Exception as exc:
                evidence["error"] = str(exc)
                result["errors"].append(f"{qualified}: {exc}")
    for path in sorted(Path("contracts/silver").glob("*.yaml")):
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        result["contracts"][path.stem] = {"dataset": contract.get("dataset"), "status": contract.get("contract_status"),
            "keys": contract.get("grain", {}).get("business_key"), "columns": contract.get("schema", {}).get("columns"),
            "rules": contract.get("data_quality")}
    try:
        if metadata_file:
            result.update(json.loads(Path(metadata_file).read_text(encoding="utf-8")))
            raise StopIteration
        from ingestion.storage.metadata_repository import MetadataRepository
        import psycopg2
        connection = (MetadataRepository()._get_connection() if os.getenv("INGESTION_DB_URL") else
            psycopg2.connect(host=os.getenv("POSTGRES_HOST", "localhost"),
                port=os.getenv("POSTGRES_PORT", "5432"), user=os.getenv("POSTGRES_USER", "airflow"),
                password=os.getenv("POSTGRES_PASSWORD", "airflow"), dbname=os.getenv("POSTGRES_DB", "airflow")))
        with connection as conn:
            conn.set_session(readonly=True)
            with conn.cursor() as cursor:
                for name, sql in {
                    "runs": "SELECT source_id,run_id,status,records_extracted,records_quarantined,chunks_processed,chunks_total,manifest_uri,artifact_uri,checksum,source_metadata FROM ingestion.ingestion_runs ORDER BY source_id,started_at",
                    "source_snapshots": "SELECT source_id,id,run_id,checksum,size_bytes,status,source_uri FROM ingestion.source_snapshots ORDER BY source_id,id",
                    "checkpoint_totals": "SELECT source_id,status,count(*) FROM ingestion.ingestion_checkpoints GROUP BY 1,2 ORDER BY 1,2",
                }.items():
                    cursor.execute(sql)
                    columns = [desc[0] for desc in cursor.description]
                    result[name] = [dict(zip(columns, row)) for row in cursor.fetchall()]
    except StopIteration:
        pass
    except Exception as exc:
        result["errors"].append(f"PostgreSQL: {exc}")
    try:
        from ingestion.storage.minio_storage import MinioStorage
        storage = MinioStorage()
        result["raw_inventory"] = {}
        raw_objects = []
        for bucket in storage.s3_client.list_buckets()["Buckets"]:
            objects = storage.list_objects(bucket["Name"], "raw/")
            result["raw_inventory"][bucket["Name"]] = [{"key": o["Key"], "size": o["Size"]} for o in objects]
            raw_objects.extend((bucket["Name"], o["Key"]) for o in objects)
        result["manifests"] = []
        nso_archives = []
        for bucket, key in raw_objects:
            if key.endswith("manifest.json"):
                body = storage.s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
                result["manifests"].append({"uri": f"s3://{bucket}/{key}", "content": json.loads(body)})
            if "nso_vietnam" in key and key.endswith(".zip"):
                body = storage.s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
                with zipfile.ZipFile(io.BytesIO(body)) as archive:
                    nso_archives.append((f"s3://{bucket}/{key}", hashlib.sha256(body).hexdigest(),
                                         {name: archive.read(name) for name in archive.namelist() if name.endswith(".csv")}))
        bronze_cols = {f["name"] for f in result["tables"]["nso_vietnam"]["schema"][-1]["fields"]}
        for path in sorted(Path("data/raw/nso").glob("V06.*.csv")):
            content = path.read_bytes()
            frame = read_csv_bytes(content)
            matches = []
            for uri, archive_checksum, members in nso_archives:
                for name, body in members.items():
                    if Path(name).name == path.name:
                        matches.append({"uri": uri + "#" + name, "archive_checksum": archive_checksum,
                                        "checksum": hashlib.sha256(body).hexdigest(), "identical_to_local": body == content})
            for bucket, key in raw_objects:
                if "nso_vietnam" in key and key.endswith("/" + path.name):
                    body = storage.s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
                    matches.append({"uri": f"s3://{bucket}/{key}", "checksum": hashlib.sha256(body).hexdigest(),
                                    "identical_to_local": body == content})
            missing = [c for c in frame.columns if sanitize_column_name(c) not in bronze_cols]
            cells = int(frame[missing].notna().sum().sum()) if missing else 0
            result["nso_files"].append({"file": path.name, "rows": len(frame), "columns": list(frame.columns),
                "checksum": hashlib.sha256(content).hexdigest(), "missing_bronze_columns": missing,
                "lost_nonnull_cells": cells,
                "lost_measurement_cells": int(frame[missing[1:]].notna().sum().sum()) if missing else 0,
                "lost_numeric_cells": int(frame[missing[1:]].apply(lambda col: pd.to_numeric(col, errors="coerce")).notna().sum().sum()) if missing else 0,
                "raw_matches": matches})
    except Exception as exc:
        result["errors"].append(f"Raw/NSO: {exc}")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="docs/silver/bronze_inventory_evidence.json")
    parser.add_argument("--trino-port", type=int, default=8080)
    parser.add_argument("--rest-uri", default="http://localhost:8181")
    parser.add_argument("--env-file", help="Optional local service settings; values are never printed")
    parser.add_argument("--metadata-file", help="Read-only PostgreSQL JSON export for hosts with a different local PostgreSQL")
    args = parser.parse_args()
    if args.env_file:
        from dotenv import load_dotenv
        load_dotenv(args.env_file, override=False)
    evidence = audit(args.trino_port, args.rest_uri, args.metadata_file)
    Path(args.output).write_text(json.dumps(evidence, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"tables": sorted(evidence["tables"]), "errors": evidence["errors"], "output": args.output}))
    return bool(evidence["errors"])


if __name__ == "__main__":
    raise SystemExit(main())
