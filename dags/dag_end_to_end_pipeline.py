"""End-to-end orchestrator DAG: Bronze (Ingestion) -> Silver -> Gold -> Export Marts."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))


def _available_memory_bytes() -> int:
    """Return available host/container RAM bytes on either Windows or Linux."""
    if os.name == "nt":
        from analytics.client import windows_available_bytes
        return windows_available_bytes()
    meminfo = Path("/proc/meminfo").read_text(encoding="utf-8").splitlines()
    for line in meminfo:
        if line.startswith("MemAvailable:"):
            kib = int(line.split(":", 1)[1].strip().split()[0])
            return kib * 1024
    raise RuntimeError("Missing MemAvailable in /proc/meminfo")


def _build_admission_ticket() -> dict:
    return {"windows_available_bytes": _available_memory_bytes(), "measured_at": time.time()}


class _InContainerEDAClient:
    """In-process Trino/MinIO client for executing Power BI exports inside Airflow."""

    def __init__(self) -> None:
        self.batch = None
        self.evidence = []

    def query(self, name: str, **parameters):
        import pandas as pd
        from analytics.client import run_query

        started = time.monotonic()
        response = run_query(name, parameters, _build_admission_ticket(), expected_batch=self.batch)
        response["client_seconds"] = time.monotonic() - started
        if self.batch is not None and response["batch"] != self.batch:
            raise RuntimeError("Mixed published batches")
        self.batch = response["batch"]
        self.evidence.append({k: v for k, v in response.items() if k not in ("rows", "columns", "snapshots")})
        return pd.DataFrame(response["rows"], columns=[c["name"] for c in response["columns"]])


@contextmanager
def _in_container_publication_fence(payload: dict):
    """Hold Gold publication read window in-process inside the Airflow container."""
    from analytics.client import admission
    from analytics.powerbi import check_manifest
    from gold.pipeline import read_window

    with admission(_build_admission_ticket()), read_window() as manifest:
        check_manifest(manifest, payload)
        yield
        with read_window() as readback:
            check_manifest(readback, payload)


def _in_container_gold_manifest_runner(*args, **kwargs):
    """Read the published Gold manifest directly from MinIO instead of docker exec."""
    from gold.pipeline import Publication

    pub = Publication()
    ptr = pub.get("published.json")
    if not ptr or not ptr.get("batch"):
        raise RuntimeError("No published Gold batch found in MinIO")
    manifest = pub.get(ptr["batch"] + ".json")
    return subprocess.CompletedProcess(args[0], 0, stdout=json.dumps(manifest), stderr="")


def run_export_marts(conf: dict | None = None, root: Path = ROOT) -> dict:
    """Export canonical Power BI marts and refresh the forecasting mart after Gold."""
    import pandas as pd

    conf = conf or {}
    export_powerbi = bool(conf.get("export_powerbi", True))
    export_forecast_mart = bool(conf.get("export_forecast_mart", True))
    summary: dict = {"status": "SUCCESS", "powerbi": None, "forecast_mart": None}

    if export_powerbi:
        from analytics.powerbi import export_bundle, publication_fence

        client = None if os.name == "nt" else _InContainerEDAClient()
        fence = publication_fence if os.name == "nt" else _in_container_publication_fence
        folder, manifest = export_bundle(Path(root) / "exports" / "powerbi", client=client, fence=fence, report=True)
        summary["powerbi"] = {
            "status": "DATA_MARTS_EXPORTED",
            "bundle": str(folder),
            "published_batch": manifest["published_batch"],
            "rows": {name: info["rows"] for name, info in manifest["outputs"].items()},
        }

    if export_forecast_mart:
        from jobs.forecasting import build_forecasting_mart, extract_gold_prices

        forecast_dir = Path(root) / "exports" / "forecast"
        trino_url = os.environ.get("TRINO_URL") or (
            f"http://{os.environ.get('TRINO_HOST', 'trino')}:{os.environ.get('TRINO_PORT', '8080')}"
        )
        runner = subprocess.run if os.name == "nt" else _in_container_gold_manifest_runner
        prov = extract_gold_prices.extract_all(out_dir=forecast_dir, trino_url=trino_url, runner=runner)
        raw = pd.read_csv(forecast_dir / "gold_prices_monthly.csv")
        mart = build_forecasting_mart.build_mart(raw)
        mart.to_csv(forecast_dir / "forecasting_mart.csv", index=False)
        dq19 = build_forecasting_mart.check_dq19(raw, mart)
        (forecast_dir / "dq19_mart_report.json").write_text(json.dumps(dq19, indent=2), encoding="utf-8")
        summary["forecast_mart"] = {"provenance": prov, "dq19": dq19}

    return summary


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="rice_lakehouse_end_to_end_pipeline",
    default_args=default_args,
    description="End-to-end orchestrator: Bronze (Ingestion) -> Silver -> Gold -> Export Marts",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 26, tz="Asia/Bangkok"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    render_template_as_native_obj=True,
    tags=["rice-lakehouse", "end-to-end", "orchestrator"],
) as dag:
    run_bronze_ingestion = TriggerDagRunOperator(
        task_id="run_bronze_ingestion",
        trigger_dag_id="rice_lakehouse_ingestion",
        conf="{{ dag_run.conf.get('bronze_conf', {}) }}",
        wait_for_completion=True,
        poke_interval=15,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=False,
        deferrable=False,
        retries=0,
        execution_timeout=timedelta(hours=2),
    )

    run_silver_pipeline = TriggerDagRunOperator(
        task_id="run_silver_pipeline",
        trigger_dag_id="rice_lakehouse_silver_pipeline",
        conf="{{ dag_run.conf.get('silver_conf', {}) }}",
        wait_for_completion=True,
        poke_interval=15,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=False,
        deferrable=False,
        retries=0,
        execution_timeout=timedelta(hours=2),
    )

    run_gold_pipeline = TriggerDagRunOperator(
        task_id="run_gold_pipeline",
        trigger_dag_id="rice_lakehouse_gold_pipeline",
        conf="{{ dag_run.conf.get('gold_conf', {}) }}",
        wait_for_completion=True,
        poke_interval=15,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=False,
        deferrable=False,
        retries=0,
        execution_timeout=timedelta(hours=2),
    )

    @task(task_id="export_marts", retries=0)
    def export_marts_task(**context):
        dag_run = context.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}
        return run_export_marts(conf=conf)

    export_marts = export_marts_task()
    run_bronze_ingestion >> run_silver_pipeline >> run_gold_pipeline >> export_marts
