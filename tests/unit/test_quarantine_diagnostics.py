from unittest.mock import MagicMock

import pytest

from ingestion.reliability.bronze_quality_validator import ValidationResult
from ingestion.reliability.quarantine_manager import QuarantineManager
from ingestion.reliability.quarantine_manager import QuarantinePersistenceError
from ingestion.storage.metadata_schemas import QuarantineErrorType


def test_quarantine_claim_requires_persisted_diagnostic(tmp_path):
    raw = tmp_path / "bad.csv"
    raw.write_text("bad", encoding="utf-8")
    repo = MagicMock()
    repo.persist_quarantine_diagnostic.return_value = 23
    result = QuarantineManager(metadata_repo=repo).quarantine_artifact(
        "nso", str(raw), ValidationResult(False, QuarantineErrorType.SCHEMA_DRIFT, "missing year"),
        run_id="run", batch_id="batch")
    repo.persist_quarantine_diagnostic.assert_called_once()
    assert result.is_quarantined
    assert result.diagnostic_uri == "postgresql://ingestion.dead_letter_records/23"
    assert raw.read_text(encoding="utf-8") == "bad"


@pytest.mark.parametrize("source", [None, "", "   ", 3])
def test_invalid_source_never_persists(source):
    repo = MagicMock()
    with pytest.raises(ValueError, match="source_id"):
        QuarantineManager(repo).quarantine_artifact(source, "bad.csv", run_id="r")
    repo.persist_quarantine_diagnostic.assert_not_called()


def test_missing_or_successful_validation_context_is_rejected():
    manager = QuarantineManager(MagicMock())
    with pytest.raises(ValueError, match="context"):
        manager.quarantine_artifact("nso", "missing.csv", run_id="r")
    with pytest.raises(ValueError, match="passed"):
        manager.quarantine_artifact("nso", "missing.csv", ValidationResult(True), run_id="r")


def test_persistence_failure_preserves_validation_cause(tmp_path):
    repo = MagicMock()
    repo.persist_quarantine_diagnostic.side_effect = OSError("database unavailable")
    with pytest.raises(QuarantinePersistenceError, match="bad schema.*database unavailable") as failure:
        QuarantineManager(repo).quarantine_artifact("nso", str(tmp_path / "missing"),
            error_type=QuarantineErrorType.SCHEMA_DRIFT, error_message="bad schema", run_id="r")
    assert isinstance(failure.value.__cause__, OSError)


def test_retry_identity_and_context(tmp_path):
    raw = tmp_path / "bad.csv"
    raw.write_bytes(b"\xff\xfe")
    repo = MagicMock()
    repo.persist_quarantine_diagnostic.return_value = 5
    manager = QuarantineManager(repo)
    for _ in range(2):
        result = manager.quarantine_artifact("nso", str(raw), run_id="r", batch_id="b",
            error_type=QuarantineErrorType.CORRUPT_FILE, error_message="invalid encoding", extra_details={"provider": "nso"})
    first, second = [c.args[0] for c in repo.persist_quarantine_diagnostic.call_args_list]
    assert first["diagnostic_id"] == second["diagnostic_id"]
    assert first["checksum"] and first["run_id"] == "r" and first["batch_id"] == "b"
    assert result.quarantined_uri == raw.as_uri()
    assert raw.read_bytes() == b"\xff\xfe"


@pytest.mark.parametrize("persist_fails", [False, True])
def test_full_loader_rejected_input_never_writes_bronze(tmp_path, sample_source_config, persist_fails):
    from ingestion.core.full_loader import FullLoader
    from ingestion.core.enums import IngestionStatus
    raw = tmp_path / "empty.csv"
    raw.write_bytes(b"")
    sample_source_config.local_path = str(raw)
    repo, writer, adapter, checkpoints = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    repo.find_snapshot_by_checksum.return_value = None
    repo.persist_quarantine_diagnostic.return_value = 17
    if persist_fails:
        repo.persist_quarantine_diagnostic.side_effect = OSError("sink offline")
    checkpoints.get_last_successful.return_value = None
    result = FullLoader(adapter, checkpoints, MagicMock(), repo, bronze_writer=writer).execute(
        sample_source_config, "run", "batch")
    assert result.status == IngestionStatus.FAILED
    writer.write_chunk.assert_not_called()
    adapter.extract_full.assert_not_called()
    if persist_fails:
        assert "EMPTY_FILE" in result.error_message and "sink offline" in result.error_message
    else:
        assert result.source_metadata["diagnostic_uri"] == "postgresql://ingestion.dead_letter_records/17"
    assert raw.read_bytes() == b""


def test_invalid_schema_validator_and_rejected_diagnostic(tmp_path):
    from ingestion.reliability.bronze_quality_validator import BronzeQualityValidator
    raw = tmp_path / "schema.csv"
    raw.write_text("wrong\n1\n", encoding="utf-8")
    validation = BronzeQualityValidator().validate_file(str(raw), expected_columns=["year"])
    assert not validation.is_valid and validation.error_type == QuarantineErrorType.SCHEMA_DRIFT
    repo = MagicMock()
    repo.persist_quarantine_diagnostic.return_value = 9
    QuarantineManager(repo).quarantine_artifact("nso", str(raw), validation, run_id="r")
    assert repo.persist_quarantine_diagnostic.call_args.args[0]["schema_diff"]
