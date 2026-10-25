"""Parse both existing DAGs under the separately activated local-only profile."""
import pytest
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
    gate = bronze.get_task("verify_bronze_batch")
    assert gate.upstream_task_ids == {"ingest_"+s for s in BRONZE_SOURCES}
    trigger = bronze.get_task("trigger_silver_release")
    assert trigger.wait_for_completion and not trigger.reset_dag_run
    assert trigger.upstream_task_ids == {gate.task_id} and trigger.conf["profile"] == PROFILE
    assert silver.params["local_profile"] == PROFILE and silver.params["silver_release"] is None
    assert silver.params["silver_namespace"] == "silver_local"
    assert len(silver.get_task("silver_data_quality_audit").upstream_task_ids) == 9
    for dag, ids in ((bronze, ["ingest_"+s for s in BRONZE_SOURCES]),
                     (silver, ["transform_"+s for s in DATASETS])):
        for previous, following in zip(ids, ids[1:]):
            assert previous in dag.get_task(following).upstream_task_ids


def test_direct_local_silver_execution_is_blocked(local_env):
    from ingestion.orchestration.pipeline_tasks import verify_local_silver_origin, AirflowException
    with pytest.raises(AirflowException, match="Missing local Bronze"):
        verify_local_silver_origin({"profile":PROFILE}, "manual__independent")
