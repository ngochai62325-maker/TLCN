import pytest
from airflow.models import DagBag

def test_dag_loaded_and_dependencies():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"
    
    dag_id = "rice_lakehouse_ingestion"
    # Inspect the imported graph without querying an Airflow metadata database.
    dag = dagbag.dags.get(dag_id)
    
    assert dag is not None, f"DAG {dag_id} not found"
    
    # Check that initialize_metadata is upstream of start_lakehouse_ingestion
    init_task = dag.get_task("initialize_metadata")
    start_all = dag.get_task("start_lakehouse_ingestion")
    assert start_all.task_id in init_task.downstream_task_ids
    
    # Check faostat_production ingestion task
    ingest_faostat = dag.get_task("ingest_faostat_production")
    assert start_all.task_id in ingest_faostat.upstream_task_ids
    
    # Check that silver runs AFTER ingestion for faostat_production
    silver_faostat = dag.get_task("silver_faostat_production")
    assert silver_faostat is not None, "Silver task should be dynamically generated"
    assert ingest_faostat.task_id in silver_faostat.upstream_task_ids
    
    # Check that silver is upstream of end_lakehouse_ingestion
    end_all = dag.get_task("end_lakehouse_ingestion")
    assert end_all.task_id in silver_faostat.downstream_task_ids
    
    # Every business source has Silver dependency; fixtures remain manual tests.
    ingest_usda = dag.get_task("ingest_usda_psd")
    assert ingest_usda is not None
    assert "silver_usda_psd" in ingest_usda.downstream_task_ids
    with pytest.raises(Exception):
        dag.get_task("ingest_faostat_trade_validation")
    from silver.registry import TRANSFORMERS
    for source in TRANSFORMERS:
        assert f"ingest_{source}" in dag.get_task(f"silver_{source}").upstream_task_ids
    silver = dagbag.dags.get("rice_lakehouse_silver_pipeline")
    transforms = [task for task in silver.tasks if task.task_id.startswith("transform_")]
    assert len(transforms) == len(TRANSFORMERS) == 9
    assert "publish_silver_signal" in silver.get_task("silver_data_quality_audit").downstream_task_ids
