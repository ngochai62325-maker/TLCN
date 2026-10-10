"""Orchestrate every business source; publish readiness only after live checks."""
from datetime import datetime, timedelta
import json
import logging
import urllib.request

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator

from silver.registry import TRANSFORMERS
from silver.contract.loader import SilverContractLoader

logger = logging.getLogger("airflow.silver_pipeline")
SPARK_API_URL = "http://spark-iceberg:5005/run"
TRINO_HOST = "trino"
TRINO_PORT = 8080
BUSINESS_SOURCES = tuple(TRANSFORMERS)
CONTRACT_DIR = "/opt/airflow/contracts/silver"


def query(sql):
    from ingestion.storage.bronze_writer import BronzeIcebergWriter
    return BronzeIcebergWriter(trino_host=TRINO_HOST, trino_port=TRINO_PORT).execute_query(sql)


def trigger_spark_job(job_file, mode="full", run_id=None, dataset=None, pipeline_run_id=None):
    payload = json.dumps(dict(job=job_file, mode=mode, run_id=run_id, dataset=dataset,
                              pipeline_run_id=pipeline_run_id)).encode("utf-8")
    request = urllib.request.Request(SPARK_API_URL, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=900) as response:
        data = json.loads(response.read().decode("utf-8"))
    if data.get("status") != "SUCCESS" or (dataset and (data.get("result") or {}).get(dataset, {}).get("status") != "SUCCESS"):
        raise AirflowException(f"Silver job failed or supplied no validated report: {dataset}")
    return data["result"][dataset] if dataset else data


with DAG(
    dag_id="rice_lakehouse_silver_pipeline",
    default_args=dict(owner="data_engineering", depends_on_past=False, email_on_failure=False,
                      email_on_retry=False, retries=1, retry_delay=timedelta(seconds=30)),
    description="Contract/DQ/quarantine/reconciliation for all business Bronze sources",
    schedule="0 8 15 * *", start_date=datetime(2026, 10, 1), catchup=False,
    max_active_runs=1, max_active_tasks=2, tags=["silver", "transformation", "spark", "iceberg"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(task_id="gate_check_bronze")
    def check_bronze_readiness():
        loader = SilverContractLoader(CONTRACT_DIR)
        for source in BUSINESS_SOURCES:
            contract = loader.load_contract(source)
            if contract.bronze_input in ("", "UNKNOWN") or contract.silver_output in ("", "UNKNOWN"):
                raise AirflowException(f"{source}: contract/recovery pending; cannot run a complete Silver batch")
            _, rows = query(f"SELECT count(*) FROM {contract.bronze_input}")
            if not rows or not rows[0][0]:
                raise AirflowException(f"{source}: Bronze is empty")
        with urllib.request.urlopen("http://spark-iceberg:5005/health", timeout=10) as response:
            if json.loads(response.read()).get("status") != "ok":
                raise AirflowException("Spark API is unhealthy")
        return {"ready": True, "sources": list(BUSINESS_SOURCES)}

    @task
    def transform_source(source, **context):
        dag_run = context.get("dag_run")
        config = dag_run.conf or {} if dag_run else {}
        mode = config.get("mode", "full")
        bronze_run_id = config.get("bronze_run_ids", {}).get(source)
        if mode == "incremental" and not bronze_run_id:
            raise AirflowException(f"{source}: incremental execution requires conf.bronze_run_ids[source]")
        orchestration_id = f"{context['run_id']}:{source}"
        from ingestion.storage.metadata_repository import MetadataRepository
        with MetadataRepository().source_lock(source):
            return trigger_spark_job("run_all_silver.py", mode, bronze_run_id, source, orchestration_id)

    @task(task_id="silver_data_quality_audit")
    def silver_dq_audit(reports):
        loader = SilverContractLoader(CONTRACT_DIR)
        results = {}
        for source, report in zip(BUSINESS_SOURCES, reports):
            if report.get("status") != "SUCCESS":
                raise AirflowException(f"{source}: successful pipeline report required")
            if report["observation_count"] != report["valid_count"] + report["quarantine_count"] + report["excluded_count"]:
                raise AirflowException(f"{source}: DQ reconciliation mismatch")
            if report["valid_count"] != report["dedup_count"] + report["deduplicated_count"]:
                raise AirflowException(f"{source}: dedup reconciliation mismatch")
            contract = loader.load_contract(source)
            table, keys = contract.silver_output, contract.business_key
            _, rows = query(f"SELECT count(*) FROM {table}")
            if report["dedup_count"] and rows[0][0] < report["dedup_count"]:
                raise AirflowException(f"{source}: Trino output count below accepted batch")
            _, duplicates = query(f"SELECT count(*) FROM (SELECT {','.join(keys)} FROM {table} "
                                   f"GROUP BY {','.join(keys)} HAVING count(*) > 1)")
            if duplicates[0][0]:
                raise AirflowException(f"{source}: duplicate Silver keys")
            _, missing = query(f"SELECT count(*) FROM {table} WHERE " + " OR ".join(f"{key} IS NULL" for key in keys))
            if missing[0][0]:
                raise AirflowException(f"{source}: null Silver keys")
            if report["quarantine_count"]:
                _, dead = query(f"SELECT count(*) FROM iceberg.silver.dead_letters WHERE source_dataset = '{source}'")
                if not dead[0][0]:
                    raise AirflowException(f"{source}: quarantine not queryable through Trino")
            results[source] = {"trino_rows": rows[0][0], "validated": True}
        return {"status": "PASSED", "sources": results}

    @task(task_id="publish_silver_signal")
    def publish_signal(audit):
        if audit.get("status") != "PASSED" or set(audit.get("sources", {})) != set(BUSINESS_SOURCES):
            raise AirflowException("All business sources must pass before publishing readiness")
        # Gold readiness also requires separately reviewed source/master mappings.
        return {"silver_status": "E2E_VALIDATED", "gold_status": "REQUIRES_MAPPING_REVIEW", "audit": audit}

    gate = check_bronze_readiness()
    transformations = []
    task_names = {"usda_rice_yearbook": "usda_export_price", "worldbank_pinksheet": "worldbank_commodity"}
    for source in BUSINESS_SOURCES:
        transform = transform_source.override(task_id="transform_" + task_names.get(source, source))(source)
        gate >> transform
        transformations.append(transform)
    audit = silver_dq_audit(transformations)
    signal = publish_signal(audit)
    start >> gate
    transformations >> audit >> signal >> end
