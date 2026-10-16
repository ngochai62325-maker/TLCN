"""Contract-driven Silver DAG with real Bronze readiness and Silver audits."""
from datetime import timedelta
from pathlib import Path
import sys
import os
from contextlib import nullcontext
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from silver.contract.loader import SilverContractLoader
from silver.orchestration import trigger_spark_job
from ingestion.orchestration.pipeline_tasks import REGISTRY_SHA256
from silver.local_profile import active_profile, DATASETS as LOCAL_DATASETS, NAMESPACE as LOCAL_NAMESPACE, mapping

LOCAL_PROFILE = active_profile()
CONTRACT_DIR = ROOT / "contracts/silver"
loader = SilverContractLoader(str(CONTRACT_DIR))
DATASETS = ["faostat_production", "faostat_monthly_price", "faostat_supply_utilization", "faostat_trade",
            "usda_psd", "usda_rice_yearbook", "worldbank_pinksheet", "thitruongnongsan"]
if LOCAL_PROFILE:
    from silver.local_profile import local_contract, validate_registry
    validate_registry()
    DATASETS = list(LOCAL_DATASETS)
for dataset in DATASETS:
    (local_contract(loader, dataset) if LOCAL_PROFILE else loader.load_contract(dataset)).validate_executable()
# Set only at owner-approved cutover, after CLI acceptance. No default V2 routing.
RELEASE = None if LOCAL_PROFILE else os.environ.get("SILVER_ACCEPTED_RELEASE")
if RELEASE not in (None, "silver_v2_release_20261013"):
    raise ValueError("Unapproved Silver release routing")
CANARY_ONLY = os.environ.get("SILVER_CANARY_ONLY", "0")
if CANARY_ONLY not in ("0", "1") or (CANARY_ONLY == "1" and RELEASE != "silver_v2_release_20261013"):
    raise ValueError("Manual canary requires the approved V2 release and SILVER_CANARY_ONLY=1")
PENDING_DATASETS = [p.stem for p in sorted(CONTRACT_DIR.glob("*.yaml")) if loader.load_contract(p.stem).status != "READY"]

with DAG(
    dag_id="rice_lakehouse_silver_pipeline",
    default_args={"owner": "data_engineering", "retries": 0 if RELEASE or LOCAL_PROFILE else 1, "retry_delay": timedelta(seconds=30)},
    description="Contract cleaning, quarantine, MERGE and real quality audit",
    schedule=None,  # Bronze is the only production schedule owner.
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Bangkok"),
    params={"silver_release": RELEASE, "registry_sha256": REGISTRY_SHA256 if RELEASE or LOCAL_PROFILE else None,
            **({"local_profile": LOCAL_PROFILE, "silver_namespace": LOCAL_NAMESPACE} if LOCAL_PROFILE else {})},
    catchup=False, max_active_runs=1, max_active_tasks=1,
    tags=["silver", "spark", "iceberg"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(task_id="gate_check_bronze")
    def check_bronze_readiness(**context):
        if LOCAL_PROFILE:
            from ingestion.orchestration.pipeline_tasks import verify_local_silver_origin
            verify_local_silver_origin(context["dag_run"].conf or {}, context["run_id"])
        return [trigger_spark_job(dataset=d, action="check-bronze", release=RELEASE,
                                  **({"profile": LOCAL_PROFILE} if LOCAL_PROFILE else {})) for d in DATASETS]

    @task(retries=0)
    def transform_dataset(dataset_id, **context):
        dag_run = context.get("dag_run")
        conf = dag_run.conf or {} if dag_run else {}
        if LOCAL_PROFILE:
            from silver.local_profile import require_profile
            from ingestion.orchestration.pipeline_tasks import silver_trigger_run_id
            require_profile(conf.get("profile"), conf.get("release"))
            batch = conf.get("bronze_batch") or {}
            if silver_trigger_run_id(batch.get("bronze_dag_run_id"), profile=LOCAL_PROFILE) != context.get("run_id"):
                raise ValueError("Local Silver must be triggered by its verified Bronze batch")
            proof = batch["bronze_inputs"][mapping(dataset_id)[0]]
            return trigger_spark_job(dataset=dataset_id, mode=conf.get("mode", "full"),
                                     run_id=proof["proved_run_id"] if conf.get("mode") == "incremental" else None,
                                     pipeline_run_id=context["run_id"], profile=LOCAL_PROFILE,
                                     bronze_snapshot_id=proof["snapshot_id"])
        if conf.get("release") and conf["release"] != RELEASE:
            raise ValueError("Release routing requires owner-approved cutover; use CLI acceptance first")
        from ingestion.storage.metadata_repository import MetadataRepository
        # V2 runner owns its lock; legacy execution gets the same source lock here.
        with nullcontext() if RELEASE else MetadataRepository().source_lock(dataset_id):
            return trigger_spark_job(dataset=dataset_id, mode=conf.get("mode", "full"),
                                     run_id=conf.get("bronze_run_id"), pipeline_run_id=context.get("run_id"),
                                     watermark=conf.get("watermark"), release=RELEASE)

    @task(task_id="silver_data_quality_audit")
    def silver_dq_audit(results):
        for result in results:
            for metrics in result.values():
                if metrics["prepared_count"] != metrics["valid_count"] + metrics["quarantine_count"]:
                    raise ValueError("Silver reconciliation failed")
                if metrics["valid_count"] != metrics["dedup_count"] + metrics["duplicate_count"]:
                    raise ValueError("Silver dedup reconciliation failed")
                if "observation_count" in metrics and metrics["observation_count"] != metrics["prepared_count"] + metrics["excluded_count"]:
                    raise ValueError("Silver observation reconciliation failed")
        return [trigger_spark_job(dataset=d, action="audit", release=RELEASE,
                                  **({"profile": LOCAL_PROFILE} if LOCAL_PROFILE else {})) for d in DATASETS]

    @task(task_id="publish_silver_signal")
    def publish_signal(audit):
        if any(a["status"] != "PASSED" for a in audit):
            raise ValueError("Silver audit did not pass")
        return {"silver_status": "DQ_PASSED", "acceptance": "CLI_AND_OWNER_REQUIRED",
                "release": RELEASE, "datasets": DATASETS, "pending_datasets": PENDING_DATASETS,
                **({"profile": LOCAL_PROFILE, "production_nso_contract": "DRAFT"} if LOCAL_PROFILE else {})}

    gate = check_bronze_readiness()
    transformations = [transform_dataset.override(task_id=f"transform_{dataset}")(dataset) for dataset in DATASETS]
    if LOCAL_PROFILE:
        for previous, following in zip(transformations, transformations[1:]):
            previous >> following  # A partial/unknown commit blocks subsequent writers.
    audit = silver_dq_audit(transformations)
    signal = publish_signal(audit)
    start >> gate
    gate >> transformations
    signal >> end
