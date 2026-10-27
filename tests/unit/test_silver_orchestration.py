"""Validate API boundaries and local HTTP behavior without starting Spark jobs."""
import io
import json
from unittest.mock import MagicMock, patch

import pytest

from silver.orchestration import trigger_spark_job
from jobs.silver.spark_api_server import SparkApiHandler, ThreadingHTTPServer
from jobs.silver.run_all_silver import datasets, selected_sources, source_lock, ROOT
from silver.framework import SilverTransformationFramework, RELEASE_NAMESPACE, RELEASE_SOURCES


def test_remote_job_requires_successful_structured_result():
    response = MagicMock()
    response.__enter__.return_value.read.return_value = json.dumps({"status": "SUCCESS", "result": {"usda_psd": {"status": "SUCCESS"}}}).encode()
    with patch("silver.orchestration.urllib.request.urlopen", return_value=response) as request:
        assert trigger_spark_job(dataset="usda_psd", mode="incremental", run_id="bronze-1")["usda_psd"]["status"] == "SUCCESS"
        payload = json.loads(request.call_args.args[0].data)
        assert payload["run_id"] == "bronze-1" and payload["dataset"] == "usda_psd"
    response.__enter__.return_value.read.return_value = b'{"status":"SUCCESS"}'
    with patch("silver.orchestration.urllib.request.urlopen", return_value=response), pytest.raises(RuntimeError, match="no successful result"):
        trigger_spark_job()


def handler(payload):
    body = json.dumps(payload).encode()
    instance = SparkApiHandler.__new__(SparkApiHandler)
    instance.path = "/run"
    instance.headers = {"Content-Length": str(len(body))}
    instance.rfile = io.BytesIO(body)
    instance._send_json = MagicMock()
    return instance


def test_api_rejects_unknown_script_without_subprocess():
    instance = handler({"job": "../../unapproved.py"})
    with patch("jobs.silver.spark_api_server.subprocess.run") as run:
        instance.do_POST()
    run.assert_not_called()
    assert instance._send_json.call_args.args[0] == 400


def test_api_passes_arguments_and_extracts_json():
    instance = handler({"job": "run_all_silver.py", "dataset": "usda_psd", "run_id": "bronze-1", "mode": "incremental"})
    with patch("jobs.silver.spark_api_server.subprocess.run", return_value=MagicMock(returncode=0, stdout='log\nSILVER_RESULT={"usda_psd":{"status":"SUCCESS"}}\n')) as run:
        instance.do_POST()
    command = run.call_args.args[0]
    assert command[-2:] == ["--dataset", "usda_psd"]
    assert "--run-id" in command and "bronze-1" in command
    assert instance._send_json.call_args.args[0] == 200
    assert instance._send_json.call_args.args[1]["result"]["usda_psd"]["status"] == "SUCCESS"


def test_release_scope_and_routing_are_opt_in():
    contracts = datasets(ROOT / "contracts/silver")
    legacy = SilverTransformationFramework(MagicMock())
    release = SilverTransformationFramework(MagicMock(), release=RELEASE_NAMESPACE)
    assert selected_sources(",".join(RELEASE_SOURCES), contracts, RELEASE_NAMESPACE) == list(RELEASE_SOURCES)
    for source in RELEASE_SOURCES:
        original = legacy.contract(source)
        routed = release.contract(source)
        assert routed.silver_output == original.silver_output.replace(".silver.", f".{RELEASE_NAMESPACE}.")
        assert legacy.contract_loader.load_contract(source)._data == original._data
        expected = dict(routed._data["dataset"], silver_output=original.silver_output)
        assert expected == original._data["dataset"]
        for key in original._data.keys() - {"dataset"}:
            assert original._data[key] == routed._data[key]
    for selection in ("all", "nso", "nso_vietnam", "nso_vietnam_v2", "faostat_trade_validation", "usda_psd,usda_psd"):
        with pytest.raises(ValueError):
            selected_sources(selection, contracts, RELEASE_NAMESPACE)
    with pytest.raises(ValueError):
        release.contract("nso")
    with pytest.raises(ValueError):
        release.contract("nso_vietnam")


def test_release_lock_fails_closed_and_covers_outcome(monkeypatch):
    monkeypatch.delenv("INGESTION_DB_URL", raising=False)
    with pytest.raises(ValueError, match="explicit INGESTION_DB_URL"):
        source_lock("usda_psd", RELEASE_NAMESPACE)
    monkeypatch.setenv("INGESTION_DB_URL", "postgresql://test/isolated")
    events = []
    from contextlib import contextmanager
    @contextmanager
    def locked(source):
        events.append("locked")
        yield
        events.append("unlocked")
    with patch("ingestion.storage.metadata_repository.MetadataRepository") as repository:
        repository.return_value.source_lock.side_effect = locked
        from jobs.silver.run_all_silver import run_pipeline
        def identity(spark, name):
            assert events[0] == "locked" and "unlocked" not in events
            events.append("catalog_readback")
            return {"snapshot_id": "123", "uuid": "test-uuid"}
        def write(*args, **kwargs):
            assert "unlocked" not in events
            assert kwargs["bronze_snapshot_id"] == "123"
            events.append("write")
            return {"status": "SUCCESS"}
        with patch("jobs.silver.run_all_silver.SilverTransformationFramework") as factory, patch("jobs.silver.validate_silver_e2e.table_identity", side_effect=identity):
            factory.return_value.contract.return_value.silver_output = f"iceberg.{RELEASE_NAMESPACE}.usda_rice_psd"
            factory.return_value.run.side_effect = write
            run_pipeline("usda_psd", spark=MagicMock(), release=RELEASE_NAMESPACE)
    assert events == ["locked", "catalog_readback", "catalog_readback", "write", "catalog_readback", "unlocked"]


def test_api_forwards_release_without_changing_default():
    instance = handler({"dataset": "usda_psd", "release": RELEASE_NAMESPACE})
    with patch("jobs.silver.spark_api_server.subprocess.run", return_value=MagicMock(returncode=0, stdout='SILVER_RESULT={}\n')) as run:
        instance.do_POST()
    assert run.call_args.args[0][-2:] == ["--release", RELEASE_NAMESPACE]


def test_old_running_api_cannot_silently_write_legacy_for_release():
    response = MagicMock()
    response.__enter__.return_value.read.return_value = b'{"status":"ok"}'
    with patch("silver.orchestration.urllib.request.urlopen", return_value=response) as request:
        with pytest.raises(RuntimeError, match="lacks approved release routing"):
            trigger_spark_job(dataset="usda_psd", release=RELEASE_NAMESPACE)
    assert request.call_count == 1
    assert request.call_args.args[0].endswith("/health")


def test_release_preflight_failure_prevents_every_write(monkeypatch, tmp_path):
    import jobs.silver.validate_silver_e2e as validator
    from contextlib import nullcontext
    framework = SilverTransformationFramework(MagicMock(), release=RELEASE_NAMESPACE)
    framework.run = MagicMock()
    monkeypatch.setattr(validator, "get_spark_session", lambda _: MagicMock())
    monkeypatch.setattr(validator, "SilverTransformationFramework", lambda *a, **k: framework)
    monkeypatch.setattr(validator, "source_lock", lambda *a: nullcontext())
    monkeypatch.setattr(validator, "check_bronze", lambda *a: {"status": "PASSED"})
    monkeypatch.setattr(validator, "code_fingerprint", lambda *a: "fingerprint")
    monkeypatch.setattr(validator, "table_identity", lambda *a: {"exists": False})
    def identity(spark, contract, *a):
        if contract.source_id == "faostat_trade":
            raise ValueError("Trade input missing")
        return {"snapshot_id": "123"}
    monkeypatch.setattr(validator, "input_state", identity)
    output = tmp_path / "preflight.json"
    monkeypatch.setattr("sys.argv", ["validate", "--release", RELEASE_NAMESPACE, "--dataset",
                                    "faostat_production,faostat_trade", "--preflight", "--output-json", str(output)])
    with pytest.raises(ValueError, match="Trade input missing"):
        validator.main()
    framework.run.assert_not_called()
    assert json.loads(output.read_text())["status"] == "FAILED"


@pytest.mark.parametrize("partition_rows,unique_keys", [(2, 2), (1, 1), (2, 1)])
def test_trino_partitioned_key_check_is_exact(monkeypatch, partition_rows, unique_keys):
    import jobs.silver.verify_silver_trino as validator
    fields = [{"name": "year", "type": "integer"}, {"name": "id", "type": "string"},
              {"name": "_dq_warnings", "type": {}}]
    record = {"replay_verified": True, "incremental_replay_verified": True,
              "metrics": {"target_table": f"iceberg.{RELEASE_NAMESPACE}.test", "warning_count": 0},
              "silver": {"rows": 2, "schema": {"fields": fields}}, "business_keys": ["year", "id"],
              "bronze_table": "iceberg.bronze.test", "bronze_snapshot_id": "123",
              "key_partitions": [{"year": 2020, "rows": 2}], "unit_distribution": [], "flag_distributions": {}}
    statements = []
    def query(url, sql):
        statements.append(sql)
        if sql.startswith("DESCRIBE"):
            return [{"Column": n, "Type": t} for n, t in {"year": "integer", "id": "varchar", "_dq_warnings": "array(row(rule_id varchar, error_message varchar, failed_column varchar))"}.items()]
        if "WHERE year=" in sql:
            return [{"rows": partition_rows, "unique_keys": unique_keys}]
        if "AS warnings" in sql:
            assert "DISTINCT ROW" not in sql
            return [{"rows": 2, "warnings": 0, "invalid_missing": 0}]
        if "EXCEPT" in sql:
            return [{"invalid": 0}]
        return []
    monkeypatch.setattr(validator, "query", query)
    if partition_rows == unique_keys == 2:
        assert validator.verify_record("http://test", "faostat_trade", record)["counts"]["unique_keys"] == 2
        assert any("WHERE year=2020" in sql for sql in statements)
    else:
        with pytest.raises(ValueError, match="mismatch"):
            validator.verify_record("http://test", "faostat_trade", record)


def test_api_forwards_incremental_watermark():
    instance = handler({"dataset": "nso", "mode": "incremental", "watermark": "2026-10-01T00:00:00+00:00"})
    with patch("jobs.silver.spark_api_server.subprocess.run", return_value=MagicMock(returncode=0, stdout='SILVER_RESULT={}\n')) as run:
        instance.do_POST()
    assert "--watermark" in run.call_args.args[0]
    assert "2026-10-01T00:00:00+00:00" in run.call_args.args[0]


def test_api_writer_timeout_is_not_retried_or_reported_as_success():
    import subprocess
    instance = handler({"dataset": "faostat_production", "release": RELEASE_NAMESPACE})
    with patch("jobs.silver.spark_api_server.subprocess.run", side_effect=subprocess.TimeoutExpired("spark-submit", 3300)) as execute:
        instance.do_POST()
    assert execute.call_count == 1
    assert instance._send_json.call_args.args[0] == 504
    assert "result" not in instance._send_json.call_args.args[1]


@pytest.mark.parametrize("error", [TimeoutError("writer timed out"), RuntimeError("unknown commit")])
def test_release_unknown_commit_reads_catalog_under_lock_without_retry(monkeypatch, error):
    from contextlib import contextmanager
    from jobs.silver.run_all_silver import run_pipeline
    from unittest.mock import MagicMock

    monkeypatch.setenv("INGESTION_DB_URL", "postgresql://test/isolated")
    events = []
    @contextmanager
    def locked(source):
        events.append("locked")
        try:
            yield
        finally:
            events.append("unlocked")
    def identity(*args):
        assert "unlocked" not in events
        events.append("catalog")
        return {"snapshot_id": "123", "uuid": "test-uuid"}
    with patch("ingestion.storage.metadata_repository.MetadataRepository") as repository, \
         patch("jobs.silver.run_all_silver.SilverTransformationFramework") as factory, \
         patch("jobs.silver.validate_silver_e2e.table_identity", side_effect=identity):
        repository.return_value.source_lock.side_effect = locked
        factory.return_value.contract.return_value.silver_output = f"iceberg.{RELEASE_NAMESPACE}.usda_rice_psd"
        factory.return_value.run.side_effect = error
        with pytest.raises(type(error)) as failure:
            run_pipeline("usda_psd", spark=MagicMock(), release=RELEASE_NAMESPACE)
        assert factory.return_value.run.call_count == 1
        assert failure.value.silver_failure["commit_outcome"] == "UNKNOWN"
        assert failure.value.silver_failure["retry_requires_catalog_readback"]
        assert failure.value.silver_failure["target_after"]["uuid"] == "test-uuid"
    assert events == ["locked", "catalog", "catalog", "catalog", "unlocked"]


def test_health_responds_during_job_and_jobs_remain_serial():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event, Thread
    from urllib.request import Request, urlopen

    started, release = Event(), Event()
    executions = []

    def fake_job(*args, **kwargs):
        executions.append(args[0])
        if len(executions) == 1:
            started.set()
            assert release.wait(5), "Test did not release the first job"
        return MagicMock(returncode=0, stdout='SILVER_RESULT={}\n')

    server = ThreadingHTTPServer(('127.0.0.1', 0), SparkApiHandler)
    server_thread = Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    url = f'http://127.0.0.1:{server.server_port}'

    def post():
        request = Request(url + '/run', data=b'{"dataset":"nso"}',
                          headers={'Content-Type': 'application/json'})
        with urlopen(request, timeout=10) as response:
            return json.loads(response.read())

    try:
        with patch('jobs.silver.spark_api_server.subprocess.run', side_effect=fake_job):
            with ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(post)
                assert started.wait(5)
                second = executor.submit(post)
                try:
                    with urlopen(url + '/health', timeout=2) as response:
                           health = json.loads(response.read())
                           assert health['status'] == 'ok' and health['silver_release'] == RELEASE_NAMESPACE
                           assert health['gold_model'] == 'gold-20261025.1'
                    assert len(executions) == 1
                finally:
                    release.set()
                assert first.result(timeout=10)['status'] == 'SUCCESS'
                assert second.result(timeout=10)['status'] == 'SUCCESS'
                assert len(executions) == 2
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)


@pytest.fixture
def local_profile_env(monkeypatch):
    for name in ("SILVER_ACCEPTED_RELEASE", "SILVER_BRONZE_SCHEDULE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SILVER_PRODUCTION_ENABLED", "0")
    monkeypatch.setenv("SILVER_CANARY_ONLY", "0")
    monkeypatch.setenv("SILVER_LOCAL_PROFILE", "local_nine")
    monkeypatch.setenv("SILVER_LOCAL_ENABLED", "1")
    monkeypatch.setenv("INGESTION_DB_URL", "postgresql://test/isolated")


@pytest.mark.parametrize("payload", [{"dataset":"nso"},
    {"profile":"local_nine", "dataset":"nso_vietnam"},
    {"profile":"local_nine", "dataset":"nso"},
    {"profile":"local_nine", "dataset":"nso", "bronze_snapshot_id":True},
    {"profile":"local_nine", "dataset":"nso", "bronze_snapshot_id":2**63}])
def test_local_api_refuses_fallback_unknown_dataset_and_unpinned_write(local_profile_env, payload):
    instance = handler(payload)
    with patch("jobs.silver.spark_api_server.subprocess.run") as run:
        instance.do_POST()
    run.assert_not_called()
    assert instance._send_json.call_args.args[0] == 400


def test_local_api_preserves_integer_snapshot_and_profile(local_profile_env):
    snapshot = 8786414080148355674
    instance = handler({"dataset":"nso", "profile":"local_nine", "bronze_snapshot_id":str(snapshot)})
    with patch("jobs.silver.spark_api_server.subprocess.run", return_value=MagicMock(returncode=0, stdout='SILVER_RESULT={}\n')) as run:
        instance.do_POST()
    command = run.call_args.args[0]
    assert command[command.index("--bronze-snapshot-id")+1] == str(snapshot)
    assert command[command.index("--profile")+1] == "local_nine"


def test_local_runner_overlay_and_no_implicit_selection(local_profile_env):
    from silver.local_profile import DATASETS
    contracts = datasets(ROOT / "contracts/silver")
    assert selected_sources(",".join(DATASETS), contracts, profile="local_nine") == list(DATASETS)
    with pytest.raises(ValueError, match="explicit approved"):
        selected_sources("all", contracts, profile="local_nine")
    with pytest.raises(ValueError, match="no default fallback"):
        selected_sources("usda_psd", contracts)
    local = SilverTransformationFramework(MagicMock(), profile="local_nine")
    assert local.contract("nso").silver_output == "iceberg.silver_local.nso_rice_statistics"
    assert contracts["nso"].status == "READY"


def test_local_runner_holds_bronze_source_lock_through_commit_readback(local_profile_env):
    from contextlib import contextmanager
    from jobs.silver.run_all_silver import run_pipeline
    snapshot = 8786414080148355674
    events = []
    @contextmanager
    def lock(source):
        assert source == "nso"
        events.append("lock")
        try:
            yield
        finally:
            events.append("unlock")
    def identity(*args):
        assert events[-1] != "unlock"
        events.append("catalog")
        return {"snapshot_id":str(snapshot), "uuid":"test"}
    with patch("ingestion.storage.metadata_repository.MetadataRepository") as repo, \
         patch("jobs.silver.run_all_silver.SilverTransformationFramework") as factory, \
         patch("jobs.silver.validate_silver_e2e.table_identity", side_effect=identity):
        repo.return_value.source_lock.side_effect = lock
        factory.return_value.run.return_value = {"status":"SUCCESS"}
        run_pipeline("nso", spark=MagicMock(), profile="local_nine", bronze_snapshot_id=snapshot)
        assert factory.return_value.run.call_args.kwargs["bronze_snapshot_id"] == snapshot
    assert events == ["lock", "catalog", "catalog", "catalog", "unlock"]
