import os
import sys
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

# Make src available for imports if running on host/mounted correctly
# Note: In docker-compose, you should either mount ./src to /opt/airflow/src 
# and add to PYTHONPATH, or install the ingestion package in the Airflow container.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

try:
    from ingestion.core.enums import IngestionStatus
    from ingestion.core.result import IngestionResult
    from ingestion.core.source_registry import SourceRegistry
    from ingestion.core.readiness_checker import ReadinessChecker
    from ingestion.engine.ingestion_engine import IngestionEngine
    from ingestion.metadata.metadata_repository import MetadataRepository
except ImportError as e:
    logging.warning(f"Ingestion package not found in sys.path. Details: {e}")
    # Define dummy classes to allow DAG parsing if package isn't installed
    class IngestionStatus:
        SUCCESS = "SUCCESS"
        FAILED = "FAILED"
        NOT_READY = "NOT_READY"
        SKIPPED = "SKIPPED"
        
    class SourceRegistry:
        def __init__(self, *args, **kwargs): pass
        def load_from_yaml(self, *args, **kwargs): pass
        def get_all_sources(self): return []
        def get_source(self, *args, **kwargs): return None

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='rice_lakehouse_ingestion',
    default_args=default_args,
    description='Ingestion Pipeline for Vietnam Rice Market Data Lakehouse',
    schedule='0 6 15 * *',  # 15th of each month at 6 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ingestion', 'bronze', 'lakehouse'],
) as dag:

    @task(task_id="initialize_metadata")
    def initialize_metadata_task():
        try:
            repo = MetadataRepository()
            repo.initialize()
            logging.info("Metadata repository initialized successfully.")
        except Exception as e:
            logging.error(f"Failed to initialize metadata repo: {e}")
            raise AirflowException(f"Metadata init failed: {e}")

    init_task = initialize_metadata_task()
    
    # Load registry
    # Use environment variable or default path for registry
    registry_path = os.environ.get("INGESTION_CONFIG_PATH", "/opt/airflow/src/ingestion/config/sources.yaml")
    
    try:
        registry = SourceRegistry()
        if os.path.exists(registry_path):
            registry.load_from_yaml(registry_path)
            sources = registry.get_all_sources()
        else:
            sources = []
            logging.warning(f"Source registry config not found at {registry_path}")
    except Exception as e:
        logging.error(f"Error loading source registry: {e}")
        sources = []

    # Group sources
    faostat_sources = [s for s in sources if 'faostat' in s.source_id.lower()]
    other_sources = [s for s in sources if 'faostat' not in s.source_id.lower()]
    
    def process_source(source, upstream_task):
        @task(task_id=f"check_readiness_{source.source_id}")
        def check_readiness(source_id: str):
            registry = SourceRegistry()
            registry.load_from_yaml(registry_path)
            source_config = registry.get_source(source_id)
            if not source_config:
                raise AirflowException(f"Source {source_id} not found in registry")
                
            checker = ReadinessChecker()
            result = checker.check_readiness(source_config)
            
            if not result.ready:
                logging.info(f"Source {source_id} is not ready: {result.reason}")
                raise AirflowSkipException(f"Source not ready: {result.reason}")
                
            return True

        @task(task_id=f"ingest_{source.source_id}", retries=0)  # Engine handles its own application retries
        def ingest(source_id: str, is_ready: bool):
            if not is_ready:
                raise AirflowSkipException("Source was not ready")
                
            engine = IngestionEngine()
            result = engine.run(source_id)
            
            # Serialize result for XCom
            return result.__dict__ if hasattr(result, '__dict__') else str(result)

        @task(task_id=f"validate_{source.source_id}")
        def validate(source_id: str, result_dict: dict):
            status = result_dict.get('status', 'UNKNOWN')
            error_message = result_dict.get('error_message', '')
            
            logging.info(f"Validation for {source_id} - Status: {status}")
            
            if status == IngestionStatus.FAILED:
                logging.error(f"Ingestion failed: {error_message}")
                raise AirflowException(f"Ingestion for {source_id} failed: {error_message}")
            elif status == IngestionStatus.NOT_READY:
                logging.info("Source was not ready during ingestion.")
                raise AirflowSkipException("Source not ready")
            elif status == IngestionStatus.SKIPPED:
                logging.info("Source data was skipped (e.g. idempotent run).")
                return True
            elif status == IngestionStatus.SUCCESS:
                logging.info(f"Ingestion successful for {source_id}.")
                return True
            else:
                raise AirflowException(f"Unknown status: {status}")

        t1 = check_readiness(source.source_id)
        t2 = ingest(source.source_id, t1)
        t3 = validate(source.source_id, t2)
        
        upstream_task >> t1
        
        return t3

    # Grouping operators
    start_faostat = EmptyOperator(task_id="start_faostat_sources")
    start_other = EmptyOperator(task_id="start_other_sources")
    end_all = EmptyOperator(task_id="end_ingestion")
    
    init_task >> [start_faostat, start_other]
    
    for src in faostat_sources:
        end_task = process_source(src, start_faostat)
        end_task >> end_all
        
    for src in other_sources:
        end_task = process_source(src, start_other)
        end_task >> end_all
