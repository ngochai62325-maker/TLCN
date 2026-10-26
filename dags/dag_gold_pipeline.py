"""Manual Gold pipeline; never triggers Bronze/Silver or overlapping Spark writers."""
from pathlib import Path
import sys

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from gold.admission import success_context
from gold import BUILD_ORDER
from gold.orchestration import run_stage


with DAG(
    "rice_lakehouse_gold_pipeline", schedule=None, catchup=False,
    start_date=pendulum.datetime(2026, 10, 26, tz="Asia/Bangkok"),
    max_active_runs=1, max_active_tasks=1, default_args={"owner": "data_engineering", "retries": 0},
    description="Pinned Silver -> dimensions -> serial facts -> DQ/semantic SQL -> publication",
    tags=["gold", "local_nine", "manual"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(task_id="verify_silver_readiness_and_pin")
    def pin_inputs(**context):
        conf = context["dag_run"].conf or {}
        if set(conf) - {"silver_run_id"}:
            raise ValueError("Gold accepts only optional silver_run_id; scope/namespace fixed")
        proof = success_context(conf.get("silver_run_id"))
        return {"silver_run_id": proof["silver_run_id"], "inputs": proof["pins"]}

    @task(task_id="validate_source_schemas_and_snapshots")
    def preflight(pin, **context):
        return run_stage("preflight", context["run_id"], pin["silver_run_id"])

    @task(task_id="build_dimensions")
    def dimensions(pin, admitted, **context):
        if admitted["status"] != "READY":
            raise ValueError("Gold admission did not pass")
        return run_stage("dimensions", context["run_id"], pin["silver_run_id"])

    @task
    def fact(source, pin, **context):
        return run_stage("fact", context["run_id"], pin["silver_run_id"], source)

    @task(task_id="dq_reconciliation_views_and_snapshot_readback")
    def verify(pin, **context):
        return run_stage("validate", context["run_id"], pin["silver_run_id"])

    @task(task_id="publish_gold_batch_manifest")
    def publish(pin, verified, **context):
        if verified["status"] != "VERIFIED":
            raise ValueError("Cannot publish unverified Gold outputs")
        return run_stage("publish", context["run_id"], pin["silver_run_id"])

    pin = pin_inputs()
    check = preflight(pin)
    dims = dimensions(pin, check)
    last = dims
    for source in BUILD_ORDER:
        item = fact.override(task_id="fact_" + source)(source, pin)
        last >> item
        last = item
    validation = verify(pin)
    last >> validation
    published = publish(pin, validation)
    start >> pin
    published >> end
