"""Local overlay and batch admission; no external writer or shared metadata."""
import copy
from unittest.mock import MagicMock

import pytest

from silver import local_profile as local
from silver.contract.loader import SilverContractLoader
from ingestion.orchestration import pipeline_tasks as tasks


@pytest.fixture
def local_env(monkeypatch):
    for name in ("SILVER_ACCEPTED_RELEASE", "SILVER_BRONZE_SCHEDULE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SILVER_ACCEPTED_RELEASE", "")  # Explicit unset value in isolated Compose.
    for name in ("SILVER_PRODUCTION_ENABLED", "SILVER_CANARY_ONLY"):
        monkeypatch.setenv(name, "0")
    monkeypatch.setenv("SILVER_LOCAL_PROFILE", local.PROFILE)
    monkeypatch.setenv("SILVER_LOCAL_ENABLED", "1")


def test_exact_scope_and_approved_overlay(local_env):
    loader = SilverContractLoader()
    assert len(local.BRONZE_SOURCES) == len(set(local.BRONZE_SOURCES)) == 9
    assert local.mapping("nso_vietnam_v2") == ("nso_vietnam", "nso_rice_statistics_v2")
    assert "nso_vietnam_v2" not in local.BRONZE_SOURCES
    assert {c.source_id for c in local.validate_registry()} == set(local.BRONZE_SOURCES)
    for source, dataset, target in local.MAPPINGS:
        original = loader.load_contract(dataset)
        approved = local.local_contract(loader, dataset)
        assert approved.bronze_input == f"iceberg.bronze.{source}"
        assert approved.silver_output == f"iceberg.silver_local.{target}"
        expected = copy.deepcopy(original._data)
        expected["dataset"]["silver_output"] = approved.silver_output
        if dataset == "nso_vietnam_v2":
            expected["contract_status"] = "READY"
            assert loader.load_contract(dataset).status == "DRAFT"
        assert approved._data == expected
    with pytest.raises(ValueError, match="outside"):
        local.mapping("nso_vietnam")


@pytest.mark.parametrize("name,value", [("SILVER_ACCEPTED_RELEASE", tasks.SILVER_RELEASE),
    ("SILVER_PRODUCTION_ENABLED", "1"), ("SILVER_CANARY_ONLY", "1"),
    ("SILVER_BRONZE_SCHEDULE", "0 3 * * *"), ("SILVER_LOCAL_ENABLED", "0"),
    ("SILVER_LOCAL_PROFILE", "local_other")])
def test_ambiguous_activation_fails_closed(local_env, monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        local.active_profile()


def test_nso_overlay_refuses_contract_drift(local_env, tmp_path):
    from pathlib import Path
    original = Path(SilverContractLoader().contract_dir) / "nso_vietnam_v2.yaml"
    (tmp_path / original.name).write_bytes(original.read_bytes() + b"\n# unreviewed\n")
    with pytest.raises(ValueError, match="exact Phase 1"):
        local.local_contract(SilverContractLoader(str(tmp_path)), "nso_vietnam_v2")


@pytest.mark.parametrize("status", ["FAILED", "NOT_READY", "NO_NEW_DATA", "UNKNOWN"])
def test_local_ingestion_failure_cannot_skip_batch(local_env, monkeypatch, status):
    record = dict(source_id="nso_vietnam", run_id="run", status=status, checksum="a"*64)
    engine = MagicMock()
    engine.run_with_readiness_check.return_value.to_dict.return_value = record
    monkeypatch.setattr(tasks.IngestionEngine, "create_default", lambda: engine)
    with pytest.raises(tasks.AirflowException, match="did not complete"):
        tasks.run_ingestion_task("nso_vietnam", profile=local.PROFILE)


def test_skip_needs_prior_success_and_physical_proof(local_env, monkeypatch):
    record = dict(source_id="nso_vietnam", run_id="skip", status="SKIPPED", checksum="a"*64,
                  source_metadata={"skipped_reason":"unchanged_snapshot"})
    repo = MagicMock()
    repo.get_run.side_effect = lambda run: record if run == "skip" else None
    repo.find_snapshot_by_checksum.return_value = {"run_id":"original"}
    with pytest.raises(tasks.AirflowException, match="successful ingestion evidence"):
        tasks.verify_release_result(record, repo, profile=local.PROFILE)
    success = dict(source_id="nso_vietnam", status="SUCCESS", checksum="a"*64, records_extracted=833)
    repo.get_run.side_effect = lambda run: record if run == "skip" else success
    assert tasks.verify_release_result(record, repo, profile=local.PROFILE) == record
    proof = dict(snapshot_id="123", proved_run_id="original")
    engine = MagicMock(metadata_repo=repo)
    engine.run_with_readiness_check.return_value.to_dict.return_value = record
    monkeypatch.setattr(tasks.IngestionEngine, "create_default", lambda: engine)
    monkeypatch.setattr(tasks, "local_bronze_proof", lambda *a: proof)
    result = tasks.run_ingestion_task("nso_vietnam", profile=local.PROFILE)
    assert result["status"] == "SKIPPED" and result["bronze_identity"] == proof


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "wrong_source", "identity_drift"])
def test_local_batch_is_exact_and_pinned(local_env, monkeypatch, mutation):
    records = [dict(source_id=s, registry_sha256=local.REGISTRY_SHA256, bronze_identity={"snapshot_id":"123"})
               for s in local.BRONZE_SOURCES]
    monkeypatch.setattr(tasks, "local_bronze_proof", lambda *a: {"snapshot_id":"123"})
    if mutation == "missing": records.pop()
    elif mutation == "duplicate": records[-1] = records[0]
    elif mutation == "wrong_source": records[-1]["source_id"] = "nso_vietnam_v2"
    else: records[-1]["bronze_identity"] = {"snapshot_id":"456"}
    with pytest.raises(tasks.AirflowException):
        tasks.verify_local_bronze_batch(records)
