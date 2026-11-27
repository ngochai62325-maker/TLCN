"""Parse all DAGs under the separately activated local-only profile."""
from unittest.mock import MagicMock
from airflow.models import DagBag
from tests.unit.test_local_nine_profile import local_env
from silver.local_profile import BRONZE_SOURCES, DATASETS, PROFILE


def test_local_nine_scope_dependencies_and_retries(local_env):
    bag = DagBag(dag_folder="dags/", include_examples=False)
    assert not bag.import_errors
    bronze, silver = (bag.dags[k] for k in ("rice_lakehouse_ingestion", "rice_lakehouse_silver_pipeline"))
    assert {t.task_id[7:] for t in bronze.tasks if t.task_id.startswith("ingest_")} == set(BRONZE_SOURCES)
    assert {t.task_id[10:] for t in silver.tasks if t.task_id.startswith("transform_")} == set(DATASETS)
    for dag in (bronze, silver):
        assert dag.schedule_interval is None and not dag.catchup and dag.max_active_runs == 1
        assert dag.max_active_tasks == 1
        assert all(t.retries == 0 for t in dag.tasks)
        assert not any("gold" in t.task_id for t in dag.tasks)
    assert "trigger_silver_release" not in bronze.task_ids
    gate = bronze.get_task("verify_bronze_batch")
    assert gate.upstream_task_ids == {"ingest_" + s for s in BRONZE_SOURCES}
    assert "end_lakehouse_ingestion" in gate.downstream_task_ids
    assert silver.params["local_profile"] == PROFILE and silver.params["silver_release"] is None
    assert silver.params["silver_namespace"] == "silver_local"
    assert len(silver.get_task("silver_data_quality_audit").upstream_task_ids) == 9
    for dag, ids in ((bronze, ["ingest_" + s for s in BRONZE_SOURCES]),
                     (silver, ["transform_" + s for s in DATASETS])):
        for previous, following in zip(ids, ids[1:]):
            assert previous in dag.get_task(following).upstream_task_ids

    e2e = bag.dags["rice_lakehouse_end_to_end_pipeline"]
    assert e2e.schedule_interval is None and not e2e.catchup and e2e.max_active_runs == 1
    chain = ["run_bronze_ingestion", "run_silver_pipeline", "run_gold_pipeline", "export_marts"]
    for prev, nxt in zip(chain, chain[1:]):
        assert nxt in e2e.get_task(prev).downstream_task_ids


def test_standalone_local_silver_resolves_bronze_batch(local_env, monkeypatch):
    from ingestion.orchestration import pipeline_tasks as tasks

    fake_batch = {
        "status": "PASSED",
        "profile": PROFILE,
        "results": [{"source_id": s} for s in BRONZE_SOURCES],
        "bronze_inputs": {s: {"snapshot_id": "123", "proved_run_id": f"run-{s}"} for s in BRONZE_SOURCES},
    }
    monkeypatch.setattr(tasks, "verify_local_bronze_batch", lambda results: fake_batch)
    resolved = tasks.resolve_local_bronze_batch({"bronze_batch": {"results": fake_batch["results"]}})
    assert resolved == fake_batch
