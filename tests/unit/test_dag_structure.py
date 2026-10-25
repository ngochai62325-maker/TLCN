import pytest
from airflow.models import DagBag


@pytest.fixture(autouse=True)
def clear_release_environment(monkeypatch):
    for key in ("SILVER_ACCEPTED_RELEASE", "SILVER_CANARY_ONLY", "SILVER_PRODUCTION_ENABLED", "SILVER_BRONZE_SCHEDULE"):
        monkeypatch.delenv(key, raising=False)

def test_dag_loaded_and_dependencies():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"
    
    dag_id = "rice_lakehouse_ingestion"
    dag = dagbag.dags.get(dag_id)
    
    assert dag is not None, f"DAG {dag_id} not found"
    assert dag.schedule_interval is None
    assert "trigger_silver_release" not in dag.task_ids
    
    # Check that initialize_metadata is upstream of start_lakehouse_ingestion
    init_task = dag.get_task("initialize_metadata")
    start_all = dag.get_task("start_lakehouse_ingestion")
    assert start_all.task_id in init_task.downstream_task_ids
    
    # Check faostat_production ingestion task
    ingest_faostat = dag.get_task("ingest_faostat_production")
    assert start_all.task_id in ingest_faostat.upstream_task_ids
    assert ingest_faostat.retries == 2
    
    assert not any(t.task_id.startswith('silver_') for t in dag.tasks)
    end_all = dag.get_task('end_lakehouse_ingestion')
    assert end_all.task_id in ingest_faostat.downstream_task_ids
    silver_dag = dagbag.dags.get('rice_lakehouse_silver_pipeline')
    assert silver_dag is not None
    assert silver_dag.schedule_interval is None
    expected = {'faostat_production','faostat_monthly_price','faostat_supply_utilization','faostat_trade',
                'usda_psd','usda_rice_yearbook','worldbank_pinksheet','thitruongnongsan'}
    assert {t.task_id.removeprefix('transform_') for t in silver_dag.tasks if t.task_id.startswith('transform_')} == expected
    assert len(silver_dag.get_task('silver_data_quality_audit').upstream_task_ids) == 8
    import dags.dag_silver_pipeline as module
    assert module.RELEASE is None
    assert module.publish_signal.function([{'status':'PASSED'}])['silver_status'] == 'DQ_PASSED'


@pytest.mark.parametrize("canary,production", [("1", "0"), ("0", "0"), ("0", "1")])
def test_owner_approved_release_bronze_scope_excludes_nso_and_fixture(monkeypatch, canary, production):
    monkeypatch.setenv("SILVER_ACCEPTED_RELEASE", "silver_v2_release_20261013")
    monkeypatch.setenv("SILVER_CANARY_ONLY", canary)
    monkeypatch.setenv("SILVER_PRODUCTION_ENABLED", production)
    bag = DagBag(dag_folder="dags/", include_examples=False)
    assert not bag.import_errors
    bronze = bag.dags["rice_lakehouse_ingestion"]
    expected = {"faostat_production", "faostat_monthly_price", "faostat_supply_utilization", "faostat_trade",
                "usda_psd", "usda_rice_yearbook", "worldbank_pinksheet", "thitruongnongsan"}
    assert {t.task_id.removeprefix("ingest_") for t in bronze.tasks if t.task_id.startswith("ingest_")} == expected
    assert not any(t.task_id.startswith("silver_") for t in bronze.tasks)
    assert all(t.retries == 0 for t in bronze.tasks if t.task_id.startswith("ingest_"))
    silver = bag.dags["rice_lakehouse_silver_pipeline"]
    assert all(t.retries == 0 for d in (bronze, silver) for t in d.tasks)
    assert bronze.schedule_interval is None and silver.schedule_interval is None
    assert not bronze.catchup and not silver.catchup
    assert bronze.max_active_runs == silver.max_active_runs == 1
    if production == "1":
        trigger = bronze.get_task("trigger_silver_release")
        assert trigger.wait_for_completion and not trigger.reset_dag_run and not trigger._defer
        assert trigger.retries == 0 and trigger.execution_timeout.total_seconds() == 7200
        assert trigger.conf == {"mode": "full", "release": "silver_v2_release_20261013"}
        gate = bronze.get_task("verify_bronze_batch")
        assert gate.upstream_task_ids == {"ingest_" + s for s in expected}
        assert gate.trigger_rule == trigger.trigger_rule == "all_success"
        assert trigger.upstream_task_ids == {gate.task_id}
    else:
        assert "trigger_silver_release" not in bronze.task_ids
    transformations = [t for t in silver.tasks if t.task_id.startswith("transform_")]
    assert {t.op_args[0] for t in transformations} == expected
    assert all(t.retries == 0 for t in transformations)
    assert all(t.python_callable.__globals__["RELEASE"] == "silver_v2_release_20261013" for t in transformations)
    signal = silver.get_task("publish_silver_signal").python_callable([{"status": "PASSED"}])
    assert signal["silver_status"] == "DQ_PASSED" and signal["release"] == "silver_v2_release_20261013"


@pytest.mark.parametrize("release,canary", [(None, "1"), ("silver_v2_release_20261013", "true")])
def test_canary_rejects_missing_release_or_invalid_flag(monkeypatch, release, canary):
    monkeypatch.delenv("SILVER_ACCEPTED_RELEASE", raising=False)
    if release:
        monkeypatch.setenv("SILVER_ACCEPTED_RELEASE", release)
    monkeypatch.setenv("SILVER_CANARY_ONLY", canary)
    bag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(bag.import_errors) == 2
    assert all("Manual canary requires" in error for error in bag.import_errors.values())


def test_only_explicitly_approved_bronze_schedule_is_used(monkeypatch):
    monkeypatch.setenv("SILVER_ACCEPTED_RELEASE", "silver_v2_release_20261013")
    monkeypatch.setenv("SILVER_PRODUCTION_ENABLED", "1")
    monkeypatch.setenv("SILVER_BRONZE_SCHEDULE", "0 3 * * 1")  # Test input, not a selected business schedule.
    bag = DagBag(dag_folder="dags/", include_examples=False)
    assert not bag.import_errors
    assert bag.dags["rice_lakehouse_ingestion"].schedule_interval == "0 3 * * 1"
    assert bag.dags["rice_lakehouse_silver_pipeline"].schedule_interval is None


@pytest.mark.parametrize("production,canary,release", [("true", "0", "silver_v2_release_20261013"), ("1", "1", "silver_v2_release_20261013"), ("1", "0", None)])
def test_production_rejects_ambiguous_activation(monkeypatch, production, canary, release):
    monkeypatch.setenv("SILVER_PRODUCTION_ENABLED", production)
    monkeypatch.setenv("SILVER_CANARY_ONLY", canary)
    if release:
        monkeypatch.setenv("SILVER_ACCEPTED_RELEASE", release)
    bag = DagBag(dag_folder="dags/", include_examples=False)
    assert "dag_ingestion_pipeline.py" in " ".join(bag.import_errors)
