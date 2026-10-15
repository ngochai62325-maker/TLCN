import pytest
from unittest.mock import patch, MagicMock
from ingestion.orchestration.pipeline_tasks import run_ingestion_task, AirflowException, AirflowSkipException
from ingestion.core.enums import IngestionStatus
from ingestion.core.result import IngestionResult

def test_run_ingestion_task_success():
    """Test 1: Verify Airflow task invokes IngestionEngine (Unit Test)."""
    with patch("ingestion.orchestration.pipeline_tasks.IngestionEngine") as MockEngine:
        mock_engine = MockEngine.create_default.return_value
        
        # Setup mock result
        mock_result = MagicMock(spec=IngestionResult)
        mock_result.status = IngestionStatus.SUCCESS
        mock_result.to_dict.return_value = {"status": "SUCCESS"}
        mock_engine.run_with_readiness_check.return_value = mock_result
        
        result = run_ingestion_task("test_source")
        
        # Assert engine was instantiated and called
        MockEngine.create_default.assert_called_once()
        mock_engine.run_with_readiness_check.assert_called_once_with("test_source")
        assert result == {"status": "SUCCESS"}

def test_run_ingestion_task_failure():
    """Test 11: Airflow failure propagation. Engine exception must fail task."""
    with patch("ingestion.orchestration.pipeline_tasks.IngestionEngine") as MockEngine:
        mock_engine = MockEngine.create_default.return_value
        
        mock_result = MagicMock(spec=IngestionResult)
        mock_result.status = IngestionStatus.FAILED
        mock_result.error_message = "Data corrupt"
        mock_engine.run_with_readiness_check.return_value = mock_result
        
        with pytest.raises(AirflowException, match="Ingestion failed for source test_source: Data corrupt"):
            run_ingestion_task("test_source")

def test_run_ingestion_task_not_ready():
    """Test: Source not ready skips Airflow task cleanly."""
    with patch("ingestion.orchestration.pipeline_tasks.IngestionEngine") as MockEngine:
        mock_engine = MockEngine.create_default.return_value
        
        mock_result = MagicMock(spec=IngestionResult)
        mock_result.status = IngestionStatus.NOT_READY
        mock_result.error_message = "API unavailable"
        mock_engine.run_with_readiness_check.return_value = mock_result
        
        with pytest.raises(AirflowSkipException, match="not ready"):
            run_ingestion_task("test_source")

def test_run_ingestion_task_skipped_idempotency():
    """Test: Source skipped due to idempotency skips Airflow task cleanly."""
    with patch("ingestion.orchestration.pipeline_tasks.IngestionEngine") as MockEngine:
        mock_engine = MockEngine.create_default.return_value
        
        mock_result = MagicMock(spec=IngestionResult)
        mock_result.status = IngestionStatus.SKIPPED
        mock_result.source_metadata = {"idempotency_reason": "No changes"}
        mock_engine.run_with_readiness_check.return_value = mock_result
        
        with pytest.raises(AirflowSkipException, match="safely skipped"):
            run_ingestion_task("test_source")


@pytest.fixture
def release_records(monkeypatch):
    import ingestion.orchestration.pipeline_tasks as tasks
    records = [{"source_id": source, "run_id": source + "_current", "status": "SUCCESS",
                "checksum": "a" * 64, "records_extracted": 2, "source_metadata": {},
                "registry_sha256": tasks.REGISTRY_SHA256} for source in tasks.RELEASE_SOURCES]
    repository = MagicMock()
    def get_run(run_id):
        if run_id.endswith("_previous"):
            return {"source_id": run_id.removesuffix("_previous"), "status": "SUCCESS", "checksum": "a" * 64, "records_extracted": 2}
        return next((dict(r) for r in records if r["run_id"] == run_id), None)
    repository.get_run.side_effect = get_run
    repository.find_snapshot_by_checksum.side_effect = lambda source, checksum: {"run_id": source + "_previous"}
    monkeypatch.setattr(tasks, "MetadataRepository", lambda: repository)
    return tasks, records, repository


@pytest.mark.parametrize("skips", [0, 8, 3])
def test_release_batch_success_checksum_skip_and_mixed(release_records, skips):
    tasks, records, repository = release_records
    for record in records[:skips]:
        record.update(status="SKIPPED", records_extracted=0, source_metadata={"skipped_reason": "unchanged_snapshot"})
    result = tasks.verify_bronze_batch(records)
    assert result["status"] == "PASSED" and len(result["source_runs"]) == 8


@pytest.mark.parametrize("status", ["NOT_READY", "FAILED", "upstream_failed", "NO_NEW_DATA"])
def test_release_batch_rejects_non_success_outcomes(release_records, status):
    tasks, records, _ = release_records
    records[0]["status"] = status
    with pytest.raises(AirflowException, match="did not complete"):
        tasks.verify_bronze_batch(records)


def test_release_skip_requires_prior_successful_checksum(release_records):
    tasks, records, repository = release_records
    records[0].update(status="SKIPPED", records_extracted=0, source_metadata={"skipped_reason": "unchanged_snapshot"})
    repository.find_snapshot_by_checksum.return_value = None
    repository.find_snapshot_by_checksum.side_effect = None
    with pytest.raises(AirflowException, match="successful ingestion evidence"):
        tasks.verify_bronze_batch(records)


@pytest.mark.parametrize("reason", ["not_ready", "unknown", None])
def test_release_rejects_unverified_skip_reasons(release_records, reason):
    tasks, records, _ = release_records
    records[0].update(status="SKIPPED", source_metadata={"skipped_reason": reason})
    with pytest.raises(AirflowException, match="SKIPPED reason"):
        tasks.verify_bronze_batch(records)


def test_release_not_ready_fails_instead_of_skipping(monkeypatch):
    import ingestion.orchestration.pipeline_tasks as tasks
    from ingestion.core.enums import LoadStrategy
    result = IngestionResult("faostat_production", "not-ready", "batch", LoadStrategy.FULL,
                             IngestionStatus.NOT_READY, error_message="Raw unavailable")
    engine = MagicMock()
    engine.run_with_readiness_check.return_value = result
    monkeypatch.setattr(tasks.IngestionEngine, "create_default", lambda: engine)
    with pytest.raises(AirflowException, match="NOT_READY; batch blocked"):
        tasks.run_ingestion_task("faostat_production", approved_release=tasks.SILVER_RELEASE)
    engine.run_with_readiness_check.assert_called_once_with("faostat_production")


def test_release_checksum_skip_is_a_verified_task_success(release_records, monkeypatch):
    tasks, records, repository = release_records
    record = records[0]
    record.update(status="SKIPPED", records_extracted=0, source_metadata={"skipped_reason": "unchanged_snapshot"})
    result = MagicMock(status=IngestionStatus.SKIPPED)
    result.to_dict.return_value = record
    engine = MagicMock(metadata_repo=repository)
    engine.run_with_readiness_check.return_value = result
    monkeypatch.setattr(tasks.IngestionEngine, "create_default", lambda: engine)
    assert tasks.run_ingestion_task(record["source_id"], approved_release=tasks.SILVER_RELEASE)["status"] == "SKIPPED"


@pytest.mark.parametrize("change", ["missing", "disabled", "source_id", "checksum"])
def test_release_registry_rejects_drift(tmp_path, change):
    import copy
    from types import SimpleNamespace
    import ingestion.orchestration.pipeline_tasks as tasks
    from ingestion.config.registry import SourceRegistry
    real = SourceRegistry(); real.load()
    sources = copy.deepcopy(real.get_all_sources())
    path = real.registry_path
    if change == "missing":
        sources.pop("faostat_trade")
    elif change == "disabled":
        sources["faostat_trade"].enabled = False
    elif change == "source_id":
        sources["faostat_trade"].source_id = "nso_vietnam"
    else:
        altered = tmp_path / "registry.yaml"
        altered.write_bytes(__import__("pathlib").Path(path).read_bytes() + b"\n# drift\n")
        path = str(altered)
    registry = SimpleNamespace(registry_path=path, load=lambda: None, get_all_sources=lambda: sources)
    with pytest.raises(AirflowException, match="registry"):
        tasks.validate_release_registry(registry)


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "unexpected", "registry", "persisted"])
def test_release_batch_rejects_incomplete_or_unverified_results(release_records, mutation):
    tasks, records, repository = release_records
    if mutation == "missing": records.pop()
    elif mutation == "duplicate": records[-1] = records[0]
    elif mutation == "unexpected": records[-1]["source_id"] = "nso_vietnam_v2"
    elif mutation == "registry": records[0]["registry_sha256"] = "drift"
    else: repository.get_run.side_effect = lambda run_id: None
    with pytest.raises(AirflowException):
        tasks.verify_bronze_batch(records)


def test_trigger_identity_is_deterministic_and_rejects_historical_canary():
    from ingestion.orchestration.pipeline_tasks import silver_trigger_run_id
    assert silver_trigger_run_id("batch-A") == silver_trigger_run_id("batch-A")
    assert silver_trigger_run_id("batch-A") != silver_trigger_run_id("batch-B")
    with pytest.raises(AirflowException, match="Historical canary"):
        silver_trigger_run_id("silver_v2_canary_20261015T091831Z_bronze")
