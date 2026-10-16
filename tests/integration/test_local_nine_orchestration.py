"""Real disposable Airflow metadata checks; no external data writers."""
import copy
import os
import uuid
from unittest.mock import MagicMock

import pytest
from tests.unit.test_local_nine_profile import local_env

pytestmark = pytest.mark.skipif(os.getenv("TLCN_ISOLATED_AIRFLOW") != "1", reason="Disposable Airflow DB only")


@pytest.fixture
def local_dags(local_env):
    from airflow import settings
    from airflow.models import DagBag, DagModel
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.utils.session import create_session
    assert str(settings.engine.url) == "sqlite:////tmp/silver-orchestration-tests.db"
    bag = DagBag(dag_folder="dags/", include_examples=False)
    assert not bag.import_errors
    dags = [bag.dags[n] for n in ["rice_lakehouse_ingestion", "rice_lakehouse_silver_pipeline"]]
    for dag in dags:
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag)
    with create_session() as db:
        model = db.query(DagModel).filter_by(dag_id=dags[1].dag_id).one()
        model.is_paused, model.is_active, model.has_import_errors = False, True, False
    return dags


def test_local_duplicate_operator_cannot_reset_existing_run(local_dags):
    from airflow.exceptions import DagRunAlreadyExists
    from airflow.models import DagRun
    from airflow.utils.session import create_session
    from ingestion.orchestration.pipeline_tasks import verify_silver_trigger_ready, AirflowException
    parent = "local_metadata_" + uuid.uuid4().hex
    run_id = verify_silver_trigger_ready(parent, profile="local_nine")
    trigger = copy.copy(local_dags[0].get_task("trigger_silver_release"))
    trigger.trigger_run_id, trigger.wait_for_completion = run_id, False
    trigger.conf = {"profile":"local_nine", "mode":"full"}
    trigger.execute({"task_instance":MagicMock()})
    with pytest.raises(AirflowException, match="Duplicate"):
        verify_silver_trigger_ready(parent, profile="local_nine")
    with pytest.raises(DagRunAlreadyExists):
        trigger.execute({"task_instance":MagicMock()})
    with create_session() as db:
        runs = db.query(DagRun).filter_by(dag_id=local_dags[1].dag_id, run_id=run_id).all()
        assert len(runs) == 1 and runs[0].state == "queued"
        runs[0].state = "failed"


@pytest.mark.parametrize("state,expected", [("success",True),("failed",False),("skipped",False),(None,False)])
def test_local_batch_real_dependencies_require_nine_completed_tasks(local_dags, state, expected):
    from airflow.utils import timezone
    from airflow.utils.types import DagRunType
    from airflow.utils.session import create_session
    from airflow.ti_deps.dep_context import DepContext
    from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
    bronze = local_dags[0]
    now = timezone.utcnow()
    with create_session() as db:
        run = bronze.create_dagrun(run_id="local_deps_"+uuid.uuid4().hex, execution_date=now,
                                  data_interval=(now,now), state="running", start_date=now,
                                  run_type=DagRunType.MANUAL, session=db)
        instances = {t.task_id:t for t in run.get_task_instances(session=db)}
        writers = [t for n,t in instances.items() if n.startswith("ingest_")]
        assert len(writers) == 9
        for ti in writers:
            ti.state,ti.start_date,ti.end_date = "success",now,now
        writers[-1].state = state
        db.flush()
        gate = instances["verify_bronze_batch"]
        gate.task = bronze.get_task(gate.task_id)
        statuses = list(TriggerRuleDep().get_dep_statuses(gate, db, DepContext(flag_upstream_failed=True)))
        assert all(s.passed for s in statuses) == expected
        run.state = "failed"


def test_local_silver_requires_actual_parent_gate_xcom(local_dags, monkeypatch):
    from airflow.models import XCom
    from airflow.utils import timezone
    from airflow.utils.types import DagRunType
    from airflow.utils.session import create_session
    from ingestion.orchestration import pipeline_tasks as tasks
    bronze = local_dags[0]
    parent = "local_origin_"+uuid.uuid4().hex
    batch = dict(bronze_dag_run_id=parent, bronze_inputs={}, results=[])
    silver_id = tasks.silver_trigger_run_id(parent, profile="local_nine")
    now = timezone.utcnow()
    with create_session() as db:
        run = bronze.create_dagrun(run_id=parent, execution_date=now, data_interval=(now,now),
                                  state="running",start_date=now,run_type=DagRunType.MANUAL,session=db)
        gate = next(t for t in run.get_task_instances(session=db) if t.task_id=="verify_bronze_batch")
        gate.state = "success"
        XCom.set(key="return_value",value=batch,dag_id=bronze.dag_id,task_id=gate.task_id,run_id=parent,session=db)
    monkeypatch.setattr(tasks,"verify_local_bronze_batch",lambda results:{"bronze_inputs":{}})
    assert tasks.verify_local_silver_origin({"profile":"local_nine","bronze_batch":batch},silver_id)==batch
    with pytest.raises(tasks.AirflowException,match="deterministic"):
        tasks.verify_local_silver_origin({"profile":"local_nine","bronze_batch":batch},"manual__wrong")
    with create_session() as db:
        run = db.merge(run)
        run.state = "failed"
    with pytest.raises(tasks.AirflowException,match="persisted parent"):
        tasks.verify_local_silver_origin({"profile":"local_nine","bronze_batch":batch},silver_id)
