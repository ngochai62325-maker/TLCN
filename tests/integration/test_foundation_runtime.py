"""Owned disposable REST/PostgreSQL fixtures. Never addresses real source tables.

Explicit opt-in: FOUNDATION_OWNED_FIXTURES=1, --with-runtime. Uses only a newly
created foundation_h05_<random> namespace and identically scoped metadata rows.
"""
import os
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ingestion.core.checkpoint import PostgresCheckpointStore
from ingestion.core.config import SourceConfig
from ingestion.core.enums import ArtifactFormat, IngestionStatus, LoadStrategy, SourceType
from ingestion.core.ingestion_engine import IngestionEngine
from ingestion.core.result import DataChunk
from ingestion.reliability.bronze_quality_validator import BronzeQualityValidator
from ingestion.reliability.quarantine_manager import QuarantineManager
from ingestion.storage.bronze_writer import BronzeIcebergWriter
from ingestion.storage.metadata_repository import MetadataRepository

pytestmark = pytest.mark.integration


@pytest.fixture
def owned(tmp_path):
    if os.getenv("FOUNDATION_OWNED_FIXTURES") != "1":
        pytest.fail("Explicit FOUNDATION_OWNED_FIXTURES=1 required for owned runtime fixtures")
    source = "foundation_h05_" + uuid.uuid4().hex
    repo = MetadataRepository()
    writer = BronzeIcebergWriter(trino_host="trino", trino_port=8080, schema=source,
        minio_access_key=os.environ["AWS_ACCESS_KEY_ID"], minio_secret_key=os.environ["AWS_SECRET_ACCESS_KEY"])
    catalog = writer.get_iceberg_catalog()
    assert catalog is not None
    # Creation must fail on any collision; never claim an existing namespace.
    catalog.create_namespace(source, {"foundation.owner": source})
    try:
        yield source, repo, writer, catalog, tmp_path
    finally:
        # Exact owned namespace and source IDs; never enumerate/drop other data.
        assert source.startswith("foundation_h05_") and len(source) == 47
        assert catalog.load_namespace_properties(source)["foundation.owner"] == source
        for table in catalog.list_tables(source):
            assert table == (source, source)
            catalog.purge_table(table)
        catalog.drop_namespace(source)
        with repo._get_connection() as conn:
            with conn.cursor() as cur:
                for table in ("dead_letter_records", "ingestion_checkpoints", "source_snapshots", "ingestion_runs"):
                    cur.execute(f"DELETE FROM ingestion.{table} WHERE source_id=%s", (source,))


def test_durable_diagnostic_retry_and_reference(owned):
    source, repo, _, _, directory = owned
    raw = directory / "invalid.csv"
    raw.write_bytes(b"")
    run = source + "_invalid"
    repo.create_run(run, source, "FULL", datetime.now(timezone.utc).isoformat())
    validation = BronzeQualityValidator().validate_file(str(raw))
    manager = QuarantineManager(repo)
    first = manager.quarantine_artifact(source, str(raw), validation, run_id=run, batch_id="b")
    retry = manager.quarantine_artifact(source, str(raw), validation, run_id=run, batch_id="b")
    assert first.diagnostic_uri == retry.diagnostic_uri
    records = repo.get_dead_letters(source_id=source, run_id=run)
    assert len(records) == 1 and first.diagnostic_uri.endswith("/" + str(records[0]["id"]))
    assert records[0]["record_data"]["original_file"] == str(raw)
    assert records[0]["record_data"]["checksum"]
    assert raw.read_bytes() == b""


def engine(owned):
    source, repo, writer, catalog, directory = owned
    raw = directory / "source.csv"
    raw.write_text("id,value\n1,0\n2,-3\n", encoding="utf-8")
    config = SourceConfig(source_id=source, provider="fixture", dataset="owned", source_type=SourceType.LOCAL_FILE,
        load_strategy=LoadStrategy.FULL, format=ArtifactFormat.CSV, local_path=str(raw), expected_columns=["id", "value"])
    registry, adapter = MagicMock(), MagicMock()
    registry.get_source.return_value = config
    adapter.extract_full.side_effect = lambda *args, **kwargs: iter([
        DataChunk(0, pd.read_csv(raw), 0, 1, 2)])
    obj = IngestionEngine(registry=registry, metadata_repo=repo, checkpoint_store=PostgresCheckpointStore(repo),
                          minio_storage=MagicMock(), bronze_writer=writer)
    obj._resolve_adapter = lambda config: adapter
    return obj, source, repo, writer, catalog


def identity(catalog, source):
    table = catalog.load_table((source, source))
    return (str(table.metadata.table_uuid), table.current_snapshot().snapshot_id, table.metadata_location,
            sorted(str(task.file.file_path) for task in table.scan().plan_files()), table.scan().to_arrow().to_pylist())


def test_physical_append_lost_response_and_noop(owned, monkeypatch):
    obj, source, repo, writer, catalog = engine(owned)
    original = writer._commit_chunk
    def lost_response(cat, name, proposed, run, chunk):
        class Proxy:
            def load_table(self, name):
                table = cat.load_table(name)
                append = table.append
                def commit_then_lose(data):
                    append(data)
                    raise TimeoutError("injected lost response AFTER actual commit")
                table.append = commit_then_lose
                return table
        return original(Proxy(), name, proposed, run, chunk)
    monkeypatch.setattr(writer, "_commit_chunk", lost_response)
    result = obj.run(source)
    assert result.status == IngestionStatus.SUCCESS, result.error_message
    before = identity(catalog, source)
    assert len(before[-1]) == 2 and all(row["_source_snapshot_id"] is None for row in before[-1])
    assert repo.get_run(result.run_id)["source_metadata"]["batch_commit"]["extra_properties"]["bronze_proof"]
    replay = obj.run(source)
    assert replay.status == IngestionStatus.SKIPPED, replay.error_message
    assert before == identity(catalog, source)


def test_metadata_failure_recovers_with_original_identity_and_preserves_failed_history(owned, monkeypatch):
    obj, source, repo, _, catalog = engine(owned)
    save_snapshot = repo.save_snapshot
    monkeypatch.setattr(repo, "save_snapshot", MagicMock(side_effect=OSError("injected metadata failure")))
    failed = obj.run(source)
    assert failed.status == IngestionStatus.FAILED
    assert repo.get_run(failed.run_id)["status"] == "FAILED"
    before = identity(catalog, source)
    assert len(before[-1]) == 2
    monkeypatch.setattr(repo, "save_snapshot", save_snapshot)
    retry = obj.run(source)
    assert retry.status == IngestionStatus.SUCCESS, retry.error_message
    assert retry.source_metadata["physical_run_id"] == failed.run_id
    assert repo.get_run(failed.run_id)["status"] == "FAILED"
    assert identity(catalog, source) == before
    assert obj.run(source).status == IngestionStatus.SKIPPED
    assert identity(catalog, source) == before


def test_input_context_is_durable_and_immutable(owned):
    source, repo, _, _, _ = owned
    run = source + "_context"
    repo.create_run(run, source, "FULL", datetime.now(timezone.utc).isoformat())
    context = dict(checksum="sha", layout="layout", physical_run_id=run, physical_batch_id="b")
    repo.save_input_context(run, context)
    repo.save_input_context(run, context)
    with pytest.raises(RuntimeError, match="conflicting immutable"):
        repo.save_input_context(run, {**context, "checksum": "changed"})
    assert repo.get_run(run)["source_metadata"]["input_context"] == context


def test_quarantine_then_corrected_raw_can_ingest_without_phantom_resume(owned):
    obj, source, repo, _, catalog = engine(owned)
    config = obj.registry.get_source(source)
    from pathlib import Path
    raw = Path(config.local_path)
    raw.write_bytes(b"")
    failed = obj.run(source)
    assert failed.status == IngestionStatus.FAILED
    assert not catalog.table_exists((source, source))
    assert repo.get_run(failed.run_id)["source_metadata"]["physical_write_started"] is False
    raw.write_text("id,value\n1,0\n2,-3\n", encoding="utf-8")
    assert obj.run(source).status == IngestionStatus.SUCCESS


def test_real_source_lock_conflict_and_release(owned):
    source, repo, _, _, _ = owned
    with repo.source_lock(source):
        with pytest.raises(RuntimeError, match="LockAcquisitionError"):
            with MetadataRepository().source_lock(source):
                pytest.fail("Second writer admitted")
    with repo.source_lock(source):
        pass
