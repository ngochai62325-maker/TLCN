"""Unit tests for decoupled Bronze/Silver/Gold DAGs and the End-to-End Orchestrator DAG."""
from __future__ import annotations

import ast
import importlib.util
from pathlib import Path
import sys
import types
from unittest.mock import MagicMock

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
DAGS_DIR = ROOT / "dags"


class _FakeTask:
    def __init__(self, task_id: str, **kwargs):
        self.task_id = task_id
        self.kwargs = kwargs
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.upstream_task_ids: set[str] = set()
        self.downstream_task_ids: set[str] = set()

    def __rshift__(self, other):
        if isinstance(other, list):
            for item in other:
                self >> item
            return other
        self.downstream_task_ids.add(other.task_id)
        other.upstream_task_ids.add(self.task_id)
        return other

    def __rrshift__(self, other):
        if isinstance(other, list):
            for item in other:
                item >> self
        return self


class _FakeDAG:
    def __init__(self, dag_id: str, **kwargs):
        self.dag_id = dag_id
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.tasks: dict[str, _FakeTask] = {}

    def __enter__(self):
        _FakeDAG._current = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _FakeDAG._current = None

    def get_task(self, task_id: str) -> _FakeTask:
        return self.tasks[task_id]

    @property
    def task_ids(self) -> list[str]:
        return list(self.tasks.keys())


_FakeDAG._current = None


def _register_task(t: _FakeTask) -> _FakeTask:
    if _FakeDAG._current is not None:
        _FakeDAG._current.tasks[t.task_id] = t
    return t


class _FakeDecoratorTask(_FakeTask):
    def __init__(self, fn, task_id=None, **kwargs):
        self.fn = fn
        self.python_callable = fn
        self.function = fn
        super().__init__(task_id or fn.__name__, **kwargs)
        self._overrides: dict[str, str] = {}

    def override(self, task_id: str | None = None, **kwargs):
        base = _FakeDecoratorTask(self.fn, task_id=task_id or self.task_id, **{**self.kwargs, **kwargs})
        return base

    def __call__(self, *args, **kwargs):
        return _register_task(_FakeTask(self.task_id, python_callable=self.fn, op_args=args, **self.kwargs))


def _install_airflow_shim(monkeypatch):
    if "airflow" in sys.modules:
        try:
            from airflow import DAG  # noqa: F401
            return False
        except Exception:
            pass

    airflow_mod = types.ModuleType("airflow")
    airflow_mod.DAG = _FakeDAG

    decorators_mod = types.ModuleType("airflow.decorators")

    def fake_task(task_id=None, **kwargs):
        def wrapper(fn):
            return _FakeDecoratorTask(fn, task_id=task_id, **kwargs)
        return wrapper

    decorators_mod.task = fake_task

    operators_mod = types.ModuleType("airflow.operators")
    empty_mod = types.ModuleType("airflow.operators.empty")

    class EmptyOperator(_FakeTask):
        def __init__(self, task_id: str, **kwargs):
            super().__init__(task_id, **kwargs)
            _register_task(self)

    empty_mod.EmptyOperator = EmptyOperator

    trigger_mod = types.ModuleType("airflow.operators.trigger_dagrun")

    class TriggerDagRunOperator(_FakeTask):
        def __init__(self, task_id: str, **kwargs):
            super().__init__(task_id, **kwargs)
            _register_task(self)

    trigger_mod.TriggerDagRunOperator = TriggerDagRunOperator

    pendulum_mod = types.ModuleType("pendulum")
    pendulum_mod.datetime = lambda *a, **k: f"{a[0]}-{a[1]:02d}-{a[2]:02d}"

    for name, mod in {
        "airflow": airflow_mod,
        "airflow.decorators": decorators_mod,
        "airflow.operators": operators_mod,
        "airflow.operators.empty": empty_mod,
        "airflow.operators.trigger_dagrun": trigger_mod,
        "pendulum": pendulum_mod,
    }.items():
        monkeypatch.setitem(sys.modules, name, mod)
    return True


def _load_dag_module(filename: str, monkeypatch, env: dict[str, str] | None = None):
    _install_airflow_shim(monkeypatch)
    for k in ("SILVER_LOCAL_PROFILE", "SILVER_LOCAL_ENABLED", "SILVER_ACCEPTED_RELEASE", "SILVER_CANARY_ONLY", "SILVER_PRODUCTION_ENABLED", "SILVER_BRONZE_SCHEDULE"):
        monkeypatch.delenv(k, raising=False)
    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)
    path = DAGS_DIR / filename
    spec = importlib.util.spec_from_file_location(f"test_dag_mod_{path.stem}", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def test_ingestion_dag_decoupled_from_silver(monkeypatch):
    src = (DAGS_DIR / "dag_ingestion_pipeline.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    imported_names = {
        alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) for alias in node.names
    }
    assert "TriggerDagRunOperator" not in imported_names
    assert "trigger_silver_release" not in src

    for env in (
        {},
        {"SILVER_LOCAL_PROFILE": "local_nine", "SILVER_LOCAL_ENABLED": "1"},
        {"SILVER_ACCEPTED_RELEASE": "silver_v2_release_20261013", "SILVER_PRODUCTION_ENABLED": "1", "SILVER_CANARY_ONLY": "0"},
    ):
        mod = _load_dag_module("dag_ingestion_pipeline.py", monkeypatch, env)
        dag = mod.dag
        assert dag.dag_id == "rice_lakehouse_ingestion"
        assert "trigger_silver_release" not in dag.task_ids
        if env:
            gate = dag.get_task("verify_bronze_batch")
            assert "end_lakehouse_ingestion" in gate.downstream_task_ids


def test_end_to_end_dag_structure_and_sequential_chain(monkeypatch):
    mod = _load_dag_module("dag_end_to_end_pipeline.py", monkeypatch)
    dag = mod.dag

    assert dag.dag_id == "rice_lakehouse_end_to_end_pipeline"
    assert getattr(dag, "schedule", getattr(dag, "schedule_interval", None)) is None
    assert dag.catchup is False
    assert dag.max_active_runs == 1

    expected_tasks = ["run_bronze_ingestion", "run_silver_pipeline", "run_gold_pipeline", "export_marts"]
    assert list(dag.task_ids) == expected_tasks

    for prev, nxt in zip(expected_tasks, expected_tasks[1:]):
        assert nxt in dag.get_task(prev).downstream_task_ids
        assert prev in dag.get_task(nxt).upstream_task_ids

    expected_targets = {
        "run_bronze_ingestion": "rice_lakehouse_ingestion",
        "run_silver_pipeline": "rice_lakehouse_silver_pipeline",
        "run_gold_pipeline": "rice_lakehouse_gold_pipeline",
    }
    for task_id, target_dag_id in expected_targets.items():
        task_obj = dag.get_task(task_id)
        assert task_obj.trigger_dag_id == target_dag_id
        assert task_obj.wait_for_completion is True
        assert task_obj.allowed_states == ["success"]
        assert task_obj.failed_states == ["failed"]
        assert task_obj.reset_dag_run is False
        assert task_obj.poke_interval == 15


def test_silver_dag_standalone_execution_without_upstream_conf(monkeypatch):
    mod = _load_dag_module(
        "dag_silver_pipeline.py",
        monkeypatch,
        {"SILVER_LOCAL_PROFILE": "local_nine", "SILVER_LOCAL_ENABLED": "1"},
    )
    assert mod.dag.dag_id == "rice_lakehouse_silver_pipeline"

    from ingestion.orchestration import pipeline_tasks as tasks

    fake_batch = {
        "status": "PASSED",
        "profile": "local_nine",
        "bronze_inputs": {src: {"snapshot_id": "999", "proved_run_id": f"run-{src}"} for src in tasks.RELEASE_SOURCES + ("nso",)},
    }
    calls = []
    monkeypatch.setattr(tasks, "resolve_local_bronze_batch", lambda conf=None: fake_batch)
    monkeypatch.setattr(mod, "trigger_spark_job", lambda **kwargs: calls.append(kwargs) or {"status": "PASSED"})

    gate_fn = mod.dag.get_task("gate_check_bronze").python_callable
    gate_res = gate_fn(dag_run=MagicMock(conf={}), run_id="manual__silver_1")
    assert gate_res["bronze_inputs"] == fake_batch["bronze_inputs"]
    assert all(c["action"] == "check-bronze" for c in calls)

    calls.clear()
    ti = MagicMock()
    ti.xcom_pull.return_value = gate_res
    transform_fn = mod.dag.get_task("transform_worldbank_pinksheet").python_callable
    transform_fn("worldbank_pinksheet", dag_run=MagicMock(conf={}), run_id="manual__silver_1", ti=ti)
    assert calls[0]["bronze_snapshot_id"] == "999"
    assert calls[0]["dataset"] == "worldbank_pinksheet"


def test_run_export_marts_runs_powerbi_and_forecasting_and_respects_flags(monkeypatch, tmp_path):
    mod = _load_dag_module("dag_end_to_end_pipeline.py", monkeypatch)

    import analytics.powerbi as powerbi_mod
    from jobs.forecasting import build_forecasting_mart, extract_gold_prices

    pbi_calls = []
    def fake_export_bundle(root, client=None, fence=None, report=False):
        pbi_calls.append({"root": root, "report": report})
        return Path(root) / "bundle_1", {"published_batch": "b" * 64, "outputs": {"fact_overview.csv": {"rows": 9}}}

    fc_dir = tmp_path / "exports" / "forecast"
    fc_dir.mkdir(parents=True)
    raw_df = pd.DataFrame({"month_key": [202401], "wb_vn5_usd_t": [500.0]})
    mart_df = pd.DataFrame({"month_key": [202401], "split": ["test"], "y": [500.0]})

    def fake_extract_all(out_dir=None, trino_url=None, **kwargs):
        raw_df.to_csv(Path(out_dir) / "gold_prices_monthly.csv", index=False)
        return {"rows": 204, "trino_url": trino_url}

    monkeypatch.setattr(powerbi_mod, "export_bundle", fake_export_bundle)
    monkeypatch.setattr(extract_gold_prices, "extract_all", fake_extract_all)
    monkeypatch.setattr(build_forecasting_mart, "build_mart", lambda raw: mart_df)
    monkeypatch.setattr(build_forecasting_mart, "check_dq19", lambda raw, mart: {"dq19_status": "PASS"})

    res = mod.run_export_marts(conf={}, root=tmp_path)
    assert res["status"] == "SUCCESS"
    assert len(pbi_calls) == 1 and pbi_calls[0]["report"] is True
    assert res["powerbi"]["rows"] == {"fact_overview.csv": 9}
    assert res["forecast_mart"]["dq19"]["dq19_status"] == "PASS"
    assert (fc_dir / "forecasting_mart.csv").is_file()
    assert (fc_dir / "dq19_mart_report.json").is_file()

    pbi_calls.clear()
    res_skip = mod.run_export_marts(conf={"export_powerbi": False, "export_forecast_mart": False}, root=tmp_path)
    assert res_skip == {"status": "SUCCESS", "powerbi": None, "forecast_mart": None}
    assert len(pbi_calls) == 0
