"""Bronze storage hierarchical layout manager for MinIO Object Storage.

Enforces standard functional directory separation per source:
bronze/
├── {source_group}/
│   ├── raw/         - Original unparsed / raw input artifacts
│   ├── metadata/    - Batch metadata (batch_{batch_id}.json)
│   ├── manifest/    - Execution manifests and pipeline state
│   ├── audit/       - Audit execution logs and operational history
│   └── quarantine/  - Invalid, corrupted, or schema-drifted files + companion .error.json
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from ingestion.storage.metadata_schemas import (
    AuditLogEntry,
    BatchMetadata,
    BatchStatus,
    QuarantineDiagnostic,
)
from ingestion.storage.minio_storage import MinioStorage
from ingestion.utils.logging_config import create_ingestion_logger


SOURCE_GROUP_MAPPING: Dict[str, str] = {
    # FAOSTAT group
    "faostat": "faostat",
    "faostat_trade": "faostat",
    "faostat_production": "faostat",
    "faostat_monthly_price": "faostat",
    "faostat_supply_utilization": "faostat",
    "faostat_trade_validation": "faostat",
    # NSO group
    "nso": "nso",
    "nso_vietnam": "nso",
    "nso_gso": "nso",
    # USDA group
    "usda": "usda",
    "usda_psd": "usda",
    "usda_rice_yearbook": "usda",
    # World Bank group
    "worldbank": "worldbank",
    "worldbank_pinksheet": "worldbank",
    # Local Vietnamese market group
    "thitruongnongsan": "thitruongnongsan",
}


def resolve_source_group(source_id: str) -> str:
    """Resolve the source group from a given source_id."""
    clean_id = source_id.lower().strip()
    if clean_id in SOURCE_GROUP_MAPPING:
        return SOURCE_GROUP_MAPPING[clean_id]
    for key, group in SOURCE_GROUP_MAPPING.items():
        if clean_id.startswith(key):
            return group
    return "general"


class BronzeStorageLayout:
    """Manages the 5-folder storage hierarchy in MinIO Bronze bucket."""

    DEFAULT_BUCKET = "bronze"

    def __init__(
        self,
        minio_storage: Optional[MinioStorage] = None,
        bucket: str = DEFAULT_BUCKET,
    ) -> None:
        self.storage = minio_storage or MinioStorage()
        self.bucket = bucket
        self.logger = create_ingestion_logger("bronze_storage_layout")
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        try:
            self.storage.ensure_bucket(self.bucket)
        except Exception as e:
            self.logger.warning(f"Could not auto-create bucket {self.bucket}: {e}")

    # ── Path Construction Methods ──────────────────────────────────────

    def get_raw_key(self, source_id: str, batch_id: str, filename: str) -> str:
        """Construct raw object key: {source_group}/raw/{batch_id}/{filename}"""
        group = resolve_source_group(source_id)
        basename = os.path.basename(filename)
        return f"{group}/raw/{batch_id}/{basename}"

    def get_metadata_key(self, source_id: str, batch_id: str) -> str:
        """Construct metadata object key: {source_group}/metadata/batch_{batch_id}.json"""
        group = resolve_source_group(source_id)
        return f"{group}/metadata/batch_{batch_id}.json"

    def get_manifest_key(self, source_id: str, run_id: str) -> str:
        """Construct manifest object key: {source_group}/manifest/run_{run_id}.json"""
        group = resolve_source_group(source_id)
        return f"{group}/manifest/run_{run_id}.json"

    def get_audit_key(self, source_id: str, run_id: str) -> str:
        """Construct audit object key: {source_group}/audit/audit_{run_id}.json"""
        group = resolve_source_group(source_id)
        return f"{group}/audit/audit_{run_id}.json"

    def get_quarantine_key(self, source_id: str, filename: str, timestamp_str: Optional[str] = None) -> str:
        """Construct quarantine object key: {source_group}/quarantine/{ts}_{filename}"""
        group = resolve_source_group(source_id)
        ts = timestamp_str or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        basename = os.path.basename(filename)
        return f"{group}/quarantine/{ts}_{basename}"

    def get_quarantine_diagnostic_key(self, quarantine_key: str) -> str:
        """Construct companion diagnostic key: {quarantine_key}.error.json"""
        return f"{quarantine_key}.error.json"

    # ── Operational Storage Methods ────────────────────────────────────

    def archive_raw_artifact(self, source_id: str, batch_id: str, local_path: str) -> str:
        """Upload and archive raw input artifact to {source_group}/raw/."""
        key = self.get_raw_key(source_id, batch_id, local_path)
        uri = self.storage.upload_file(local_path, self.bucket, key)
        self.logger.info("Raw artifact archived", source_id=source_id, uri=uri)
        return uri

    def save_batch_metadata(self, metadata: BatchMetadata) -> str:
        """Save batch metadata to {source_group}/metadata/."""
        key = self.get_metadata_key(metadata.source_name, metadata.batch_id)
        uri = self.storage.upload_json(metadata.to_dict(), self.bucket, key)
        self.logger.info("Batch metadata persisted", batch_id=metadata.batch_id, uri=uri)
        return uri

    def load_batch_metadata(self, source_id: str, batch_id: str) -> Optional[BatchMetadata]:
        """Load batch metadata from {source_group}/metadata/ if it exists."""
        key = self.get_metadata_key(source_id, batch_id)
        if not self.storage.file_exists(self.bucket, key):
            return None
        try:
            resp = self.storage.s3_client.get_object(Bucket=self.bucket, Key=key)
            raw = json.loads(resp["Body"].read().decode("utf-8"))
            return BatchMetadata.from_dict(raw)
        except Exception as e:
            self.logger.warning(f"Failed to load batch metadata for {batch_id}: {e}")
            return None

    def save_audit_entry(self, entry: AuditLogEntry) -> str:
        """Save execution audit entry to {source_group}/audit/."""
        key = self.get_audit_key(entry.source_id, entry.run_id)
        uri = self.storage.upload_json(entry.to_dict(), self.bucket, key)
        self.logger.info("Audit entry saved", run_id=entry.run_id, status=entry.status.value, uri=uri)
        return uri

    def save_manifest(self, source_id: str, run_id: str, manifest_data: Dict[str, Any]) -> str:
        """Save run manifest to {source_group}/manifest/."""
        key = self.get_manifest_key(source_id, run_id)
        uri = self.storage.upload_json(manifest_data, self.bucket, key)
        return uri

    def quarantine_file(
        self,
        source_id: str,
        local_path: str,
        diagnostic: QuarantineDiagnostic,
        timestamp_str: Optional[str] = None,
    ) -> Dict[str, str]:
        """Quarantine invalid file and upload companion .error.json diagnostic.

        Returns dict with keys 'quarantined_uri' and 'diagnostic_uri'.
        """
        ts = timestamp_str or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        q_key = self.get_quarantine_key(source_id, local_path, ts)
        diag_key = self.get_quarantine_diagnostic_key(q_key)

        # Upload the quarantined file (if exists locally and non-empty, otherwise upload placeholder)
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            q_uri = self.storage.upload_file(local_path, self.bucket, q_key)
        else:
            q_uri = self.storage.upload_bytes(b"", self.bucket, q_key)

        # Upload companion .error.json
        diag_uri = self.storage.upload_json(diagnostic.to_dict(), self.bucket, diag_key)

        self.logger.warning(
            "File quarantined with diagnostics",
            source_id=source_id,
            error_type=diagnostic.error_type.value,
            quarantined_uri=q_uri,
            diagnostic_uri=diag_uri,
        )
        return {
            "quarantined_uri": q_uri,
            "diagnostic_uri": diag_uri,
        }

    def list_functional_folders(self, source_id: str) -> Dict[str, List[str]]:
        """List contents across all 5 functional folders for a given source."""
        group = resolve_source_group(source_id)
        folders = ["raw", "metadata", "manifest", "audit", "quarantine"]
        result: Dict[str, List[str]] = {}
        for folder in folders:
            prefix = f"{group}/{folder}/"
            objects = self.storage.list_objects(self.bucket, prefix)
            result[folder] = [obj["Key"] for obj in objects]
        return result
