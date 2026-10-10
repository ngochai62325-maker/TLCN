"""Lossless NSO ZIP recovery with a single schema/data Iceberg transaction."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import zipfile

import pandas as pd

from ingestion.storage.bronze_writer import BronzeIcebergWriter, TECHNICAL_METADATA_COLUMNS, sanitize_column_name
from ingestion.utils.error_classifier import SchemaError


def digest_rows(frame):
    """Compare row multisets, preserving duplicates, NULL and UTC timestamps."""
    def value(item):
        if pd.isna(item):
            return None
        if isinstance(item, (datetime, pd.Timestamp)):
            return pd.Timestamp(item).isoformat()
        return str(item)
    return Counter(tuple(value(v) for v in row) for row in frame.itertuples(index=False, name=None))


def parse_csv(content, legacy_replacement=False):
    if legacy_replacement:
        frame = pd.read_csv(io.StringIO(content.decode("utf-8", errors="replace")), dtype=str)
        encoding = "utf-8-replacement"
    else:
        for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                frame = pd.read_csv(io.BytesIO(content), encoding=encoding, dtype=str)
                break
            except UnicodeDecodeError:
                continue
    headers = list(frame.columns)
    frame.columns = [sanitize_column_name(c) for c in headers]
    if len(frame.columns) != len(set(frame.columns)) or any(not c for c in frame.columns):
        raise SchemaError("NSO header sanitization collision")
    return frame.where(pd.notna(frame), None), encoding, dict(zip(headers, frame.columns))


def prepare_recovery(archive_bytes, baseline, schema, local_dir=None, manifest_checksum=None):
    """No I/O mutations: verify archive, retain existing values/lineage, restore extra columns."""
    expected_files = {f"V06.{n}.csv" for n in range(12, 25)}
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)) or set(names) != expected_files:
            raise ValueError("ZIP must contain exactly the thirteen original NSO CSV members")
        members = {name: archive.read(name) for name in sorted(names)}
    directory_hash = hashlib.sha256()
    for name, content in members.items():
        directory_hash.update(name.encode("utf-8"))
        directory_hash.update(content)
    if manifest_checksum and directory_hash.hexdigest() != manifest_checksum:
        raise ValueError("Directory content checksum differs from ingestion manifest")
    if set(baseline["_source_file"]) != expected_files:
        raise ValueError("Existing Bronze file inventory differs from the verified archive")
    original_columns = [f.name for f in schema.fields]
    technical = list(TECHNICAL_METADATA_COLUMNS)
    if set(technical) - set(baseline) or baseline[technical].isna().any().any():
        raise ValueError("Missing/null Bronze lineage prevents safe recovery")
    frames, files, identities, raw_columns = [], [], [], set()
    national_columns = list(parse_csv(members["V06.12.csv"])[0].columns)
    retained_columns = [*national_columns, *technical]
    for name, content in members.items():
        checksum = hashlib.sha256(content).hexdigest()
        local_match = None
        if local_dir:
            local_match = (Path(local_dir) / name).read_bytes() == content
            if not local_match:
                raise ValueError(f"Local/raw checksum mismatch: {name}")
        raw, encoding, headers = parse_csv(content)
        raw_columns.update(raw.columns)
        existing = baseline[baseline["_source_file"] == name].copy()
        if len(existing) != len(raw) or set(existing["_source_checksum"]) != {checksum}:
            raise ValueError(f"Existing file count/checksum mismatch: {name}")
        lineage = existing[technical].drop_duplicates()
        if len(lineage) != 1:
            raise ValueError(f"Multiple run/chunk/lineage identities require a separate plan: {name}")
        if str(lineage.iloc[0]["_source_id"]) != "nso_vietnam":
            raise ValueError("Unexpected source identity")
        if name == "V06.12.csv":
            direct = digest_rows(raw[national_columns]) == digest_rows(existing[national_columns])
            legacy, _, _ = parse_csv(content, legacy_replacement=True)
            legacy_match = digest_rows(legacy[national_columns]) == digest_rows(existing[national_columns])
            if not direct and not legacy_match:
                raise ValueError("National stored values differ beyond historical UTF-8 replacement decoding")
            recovered = existing[retained_columns].reset_index(drop=True)
        else:
            direct, legacy_match = None, None
            if existing[national_columns].notna().any().any():
                raise ValueError(f"Unexpected existing national values in matrix file: {name}")
            recovered = raw.copy()
            for col in national_columns:
                recovered[col] = None
            for col in technical:
                recovered[col] = lineage.iloc[0][col]
        # Physical ordinal is a replay identity, not a business/geography key.
        for ordinal in range(len(raw)):
            identity = [str(lineage.iloc[0][c]) for c in ("_ingestion_run_id", "_ingestion_chunk_id", "_source_file", "_source_checksum")]
            identities.append(hashlib.sha256(json.dumps([*identity, ordinal]).encode()).hexdigest())
        matrix_columns = [c for c in raw if c.startswith("col_") or c.startswith("so_b_")]
        numeric = sum(pd.to_numeric(raw[c], errors="coerce").notna().sum() for c in matrix_columns)
        nonnull = sum(raw[c].notna().sum() for c in matrix_columns)
        marker_values = Counter(str(v) for c in matrix_columns for v in raw[c].dropna()
                                if pd.isna(pd.to_numeric(v, errors="coerce")))
        if set(marker_values) - {".."}:
            raise ValueError(f"Unknown nonnumeric matrix tokens require review: {name}")
        files.append({"file": name, "sha256": checksum, "rows": len(raw), "local_matches_raw": local_match,
                      "encoding": encoding, "headers": headers, "lineage": {c: str(lineage.iloc[0][c]) for c in technical},
                      "matrix_nonnull_cells": int(nonnull), "numeric_cells": int(numeric),
                      "missing_marker_cells": int(nonnull - numeric), "missing_markers": dict(marker_values),
                      "direct_national_values_match": direct, "legacy_national_values_match": legacy_match})
        frames.append(recovered)
    combined = pd.concat(frames, ignore_index=True).where(lambda df: df.notna(), None)
    additions = sorted(raw_columns - set(original_columns))
    combined = combined.reindex(columns=[*original_columns, *additions])
    if digest_rows(combined[retained_columns]) != digest_rows(baseline[retained_columns]):
        raise ValueError("Recovery changed retained national values or technical lineage")
    if len(identities) != len(set(identities)):
        raise ValueError("Replay identities are not unique")
    report = {"observed_at": datetime.now(timezone.utc).isoformat(), "writes_shared_data": False,
              "archive_sha256": hashlib.sha256(archive_bytes).hexdigest(),
              "archive_size_bytes": len(archive_bytes),
              "directory_content_size_bytes": sum(len(content) for content in members.values()),
              "directory_content_sha256": directory_hash.hexdigest(), "manifest_checksum": manifest_checksum,
              "archive_members": list(members), "files": files, "physical_rows": len(combined),
              "schema_before": schema.model_dump(mode="json"), "schema_additions": {c: "string nullable" for c in additions},
              "schema_fields_after": len(combined.columns), "unique_replay_identities": len(set(identities)),
              "replay_identity_sha256": hashlib.sha256("".join(sorted(identities)).encode()).hexdigest(),
              "matrix_nonnull_cells": sum(f["matrix_nonnull_cells"] for f in files),
              "numeric_cells": sum(f["numeric_cells"] for f in files),
              "missing_marker_cells": sum(f["missing_marker_cells"] for f in files),
              "retained_projection_matches": True, "mismatches": []}
    return combined, report


def atomic_recover(table, recovered, approved_additions, expected_snapshot, before_commit=None, repair_id="nso-recovery"):
    """Single commit for optional-string schema additions and whole-table replacement.

    Caller must hold the existing source lock and have separate shared-write approval.
    Never calls legacy DELETE+append or any Trino write fallback.
    """
    from pyiceberg.types import StringType
    table.refresh()
    actual = table.current_snapshot().snapshot_id
    writer = BronzeIcebergWriter(schema_policy="approved_evolution",
                                 approved_schema_additions={"nso_vietnam": approved_additions})
    additions = writer.plan_schema_additions("nso_vietnam", list(recovered), table.schema())
    existing = table.scan().to_arrow().to_pandas()
    if not additions and digest_rows(existing[list(recovered)]) == digest_rows(recovered):
        return {"status": "NOOP", "snapshot_before": actual, "snapshot_after": actual}
    if actual != expected_snapshot:
        raise ValueError("Snapshot changed; re-audit and obtain a new recovery plan")
    if digest_rows(existing[list(TECHNICAL_METADATA_COLUMNS)]) != digest_rows(recovered[list(TECHNICAL_METADATA_COLUMNS)]):
        raise ValueError("Recovery cannot change existing file/run/chunk lineage")
    with table.transaction() as transaction:
        with transaction.update_schema() as update:
            for name in additions:
                update.add_column(name, StringType(), required=False)
        arrow = writer._dataframe_to_arrow(recovered, transaction.table_metadata.schema())
        transaction.overwrite(arrow, snapshot_properties={"repair-id": repair_id, "repair-kind": "nso-raw-matrix-recovery"})
        if before_commit:
            before_commit()  # Used by isolated failure/optimistic-concurrency tests.
    table.refresh()
    result = table.scan().to_arrow().to_pandas()
    if digest_rows(result[list(recovered)]) != digest_rows(recovered):
        raise RuntimeError("Post-commit cell reconciliation failed; retain snapshots and use recorded rollback plan")
    return {"status": "RECOVERED", "snapshot_before": actual,
            "snapshot_after": table.current_snapshot().snapshot_id, "rows": len(result),
            "schema_fields": len(table.schema().fields)}
