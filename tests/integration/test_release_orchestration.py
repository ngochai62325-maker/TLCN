"""Airflow metadata integration on disposable SQLite; no external writers or E2E claims."""
import copy
import os
import uuid
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.skipif(os.getenv("TLCN_ISOLATED_AIRFLOW") != "1", reason="Disposable Airflow database only")


@pytest.fixture
def release_dags(monkeypatch):
    from airflow import settings
    from airflow.models import DagBag, DagModel
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.utils.session import create_session

    assert str(settings.engine.url) == "sqlite:////tmp/silver-orchestration-tests.db"
    monkeypatch.setenv("SILVER_ACCEPTED_RELEASE", "silver_v2_release_20261013")
    monkeypatch.setenv("SILVER_CANARY_ONLY", "0")
    monkeypatch.setenv("SILVER_PRODUCTION_ENABLED", "1")
    bag = DagBag(dag_folder="dags/", include_examples=False)
    assert not bag.import_errors
    bronze, silver = (bag.dags[k] for k in ("rice_lakehouse_ingestion", "rice_lakehouse_silver_pipeline"))
    for dag in (bronze, silver):
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag)
    with create_session() as session:
        model = session.query(DagModel).filter(DagModel.dag_id == silver.dag_id).one()
        model.is_paused, model.is_active, model.has_import_errors = False, True, False
    return bronze, silver


@pytest.mark.parametrize("condition", ["paused", "inactive", "import_error"])
def test_target_admission_rejects_unavailable_or_paused(release_dags, condition):
    from airflow.models import DagModel
    from airflow.utils.session import create_session
    from ingestion.orchestration.pipeline_tasks import verify_silver_trigger_ready, AirflowException

    with create_session() as session:
        model = session.query(DagModel).filter(DagModel.dag_id == release_dags[1].dag_id).one()
        setattr(model, {"paused": "is_paused", "inactive": "is_active", "import_error": "has_import_errors"}[condition], condition != "inactive")
        session.flush()
        with pytest.raises(AirflowException, match="unavailable/paused"):
            verify_silver_trigger_ready("admission_" + uuid.uuid4().hex, session=session)


def test_pause_between_gate_and_operator_is_rechecked(release_dags):
    from airflow.models import DagModel
    from airflow.utils.session import create_session
    from ingestion.orchestration.pipeline_tasks import verify_silver_trigger_ready, AirflowException
    bronze, silver = release_dags
    upstream = "pause_after_gate_" + uuid.uuid4().hex
    verify_silver_trigger_ready(upstream)
    with create_session() as session:
        session.query(DagModel).filter(DagModel.dag_id == silver.dag_id).one().is_paused = True
    with pytest.raises(AirflowException, match="unavailable/paused"):
        bronze.get_task("trigger_silver_release").pre_execute({"run_id": upstream})


def test_real_operator_duplicate_trigger_without_external_execution(release_dags):
    from airflow.exceptions import DagRunAlreadyExists
    from airflow.models import DagRun
    from airflow.utils.session import create_session
    from ingestion.orchestration.pipeline_tasks import verify_silver_trigger_ready, AirflowException

    bronze, silver = release_dags
    upstream = "metadata_test_" + uuid.uuid4().hex
    run_id = verify_silver_trigger_ready(upstream)
    trigger = copy.copy(bronze.get_task("trigger_silver_release"))
    trigger.trigger_run_id = run_id
    # Exercise the real DB trigger without running a scheduler or a fake-success Silver job.
    trigger.wait_for_completion = False
    context = {"task_instance": MagicMock()}
    trigger.pre_execute({"run_id": upstream})
    trigger.execute(context)
    with pytest.raises(AirflowException, match="Duplicate"):
        verify_silver_trigger_ready(upstream)
    with pytest.raises(DagRunAlreadyExists):
        trigger.execute(context)
    with create_session() as session:
        runs = session.query(DagRun).filter(DagRun.dag_id == silver.dag_id, DagRun.run_id == run_id).all()
        assert len(runs) == 1 and runs[0].state == "queued"
        assert runs[0].conf == {"mode": "full", "release": "silver_v2_release_20261013"}
        # This metadata-only test intentionally executes no Silver task.
        runs[0].state = "failed"


@pytest.mark.parametrize("field", ["silver_release", "registry_sha256"])
def test_serialized_release_identity_drift_is_blocked(release_dags, field):
    from airflow.models.serialized_dag import SerializedDagModel
    from ingestion.orchestration.pipeline_tasks import verify_silver_trigger_ready, AirflowException
    silver = release_dags[1]
    silver.params[field] = "unapproved"
    SerializedDagModel.write_dag(silver)
    with pytest.raises(AirflowException, match="routing/registry"):
        verify_silver_trigger_ready("drift_" + uuid.uuid4().hex)


@pytest.mark.parametrize("source_state,expected", [("success", True), ("failed", False), ("upstream_failed", False), ("skipped", False)])
def test_real_airflow_dependencies_block_incomplete_batch(release_dags, source_state, expected):
    from airflow.models import DagRun
    from airflow.ti_deps.dep_context import DepContext
    from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
    from airflow.utils import timezone
    from airflow.utils.session import create_session
    from airflow.utils.types import DagRunType

    bronze, silver = release_dags
    now = timezone.utcnow()
    with create_session() as session:
        before = session.query(DagRun).filter(DagRun.dag_id == silver.dag_id).count()
        run = bronze.create_dagrun(run_id="dependency_" + uuid.uuid4().hex, execution_date=now,
                                  data_interval=(now, now), state="running", start_date=now,
                                  run_type=DagRunType.MANUAL, session=session)
        instances = {ti.task_id: ti for ti in run.get_task_instances(session=session)}
        writers = [ti for name, ti in instances.items() if name.startswith("ingest_")]
        assert len(writers) == 8
        for ti in writers:
            ti.state, ti.start_date, ti.end_date = "success", now, now
        writers[0].state = source_state
        session.flush()
        gate = instances["verify_bronze_batch"]
        gate.task = bronze.get_task(gate.task_id)
        statuses = list(TriggerRuleDep().get_dep_statuses(gate, session, DepContext(flag_upstream_failed=True)))
        assert all(s.passed for s in statuses) == expected
        assert session.query(DagRun).filter(DagRun.dag_id == silver.dag_id).count() == before
        run.state = "failed"  # Test records only; no scheduler or writer ran.
