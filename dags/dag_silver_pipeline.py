"""Contract-driven Silver DAG with real Bronze readiness and Silver audits."""
from datetime import timedelta
from pathlib import Path
import sys
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from silver.contract.loader import SilverContractLoader
from silver.orchestration import trigger_spark_job

CONTRACT_DIR = ROOT / "contracts/silver"
loader = SilverContractLoader(str(CONTRACT_DIR))
DATASETS = [p.stem for p in sorted(CONTRACT_DIR.glob("*.yaml")) if loader.load_contract(p.stem).status == "READY"]

with DAG(
    dag_id="rice_lakehouse_silver_pipeline",
    default_args={"owner": "data_engineering", "retries": 1, "retry_delay": timedelta(seconds=30)},
    description="Contract cleaning, quarantine, MERGE and real quality audit",
    schedule="0 8 15 * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Bangkok"),
    catchup=False, max_active_runs=1, max_active_tasks=1,
    tags=["silver", "spark", "iceberg"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(task_id="gate_check_bronze")
    def check_bronze_readiness():
        return trigger_spark_job(action="check-bronze")

    @task
    def transform_dataset(dataset_id, **context):
        dag_run = context.get("dag_run")
        conf = dag_run.conf or {} if dag_run else {}
        return trigger_spark_job(dataset=dataset_id, mode=conf.get("mode", "full"),
                                 run_id=conf.get("bronze_run_id"), pipeline_run_id=context.get("run_id"))

    @task(task_id="silver_data_quality_audit")
    def silver_dq_audit(results):
        for result in results:
            for metrics in result.values():
                if metrics["prepared_count"] != metrics["valid_count"] + metrics["quarantine_count"]:
                    raise ValueError("Silver reconciliation failed")
                if metrics["valid_count"] != metrics["dedup_count"] + metrics["duplicate_count"]:
                    raise ValueError("Silver dedup reconciliation failed")
        return trigger_spark_job(action="audit")

    @task(task_id="publish_silver_signal")
    def publish_signal(audit):
        if audit["status"] != "PASSED":
            raise ValueError("Silver audit did not pass")
        return {"silver_status": "READY_FOR_GOLD", "datasets": DATASETS,
                "pending_datasets": ["nso_vietnam"]}

    gate = check_bronze_readiness()
    transformations = [transform_dataset.override(task_id=f"transform_{dataset}")(dataset) for dataset in DATASETS]
    audit = silver_dq_audit(transformations)
    signal = publish_signal(audit)
    start >> gate
    gate >> transformations
    signal >> end
