from typing import Any
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import re

from silver.contract.loader import SilverContractLoader
from silver.engine.quality import SilverQualityEngine
from silver.engine.quarantine import SilverQuarantineManager
from silver.engine.merge import IcebergMergeEngine

class SilverTransformationFramework:
    def __init__(self, spark: SparkSession, contract_dir: str = "contracts/silver"):
        self.spark = spark
        self.contract_loader = SilverContractLoader(contract_dir)
        self.quarantine_manager = SilverQuarantineManager(spark)
        self.merge_engine = IcebergMergeEngine(spark)
        
    def run(self, dataset_id: str, pipeline_run_id: str):
        # 1. Load Contract
        contract = self.contract_loader.load_contract(dataset_id)
        
        # 2. Input Reader
        bronze_table = contract.bronze_input
        df_bronze = self.spark.table(bronze_table)
        bronze_count = df_bronze.count()
        
        # 2.5 Dataset-Specific Preprocessing (Adapter Pattern)
        try:
            import importlib
            module = importlib.import_module(f"silver.transformers.{dataset_id}")
            transformer_class_name = "".join(word.capitalize() for word in dataset_id.split('_')) + "Transformer"
            transformer_class = getattr(module, transformer_class_name)
            transformer = transformer_class()
            df_bronze = transformer.preprocess(df_bronze)
            # Re-eval count after unpivot/filtering
            bronze_count = df_bronze.count()
        except ImportError:
            pass  # No custom transformer for this dataset
        except Exception as e:
            raise RuntimeError(f"Failed to execute transformer for {dataset_id}: {str(e)}")

        # 3. Schema Normalization & Type Casting
        df_silver = df_bronze
        
        audit_cols = [
            "_ingestion_run_id", "_source_file", "_source_checksum", 
            "_source_snapshot_id", "_ingestion_timestamp"
        ]
        
        cast_error_cols = []
        for col_def in contract.columns:
            target_name = col_def["name"]
            source_col = col_def["source_column"]
            
            # Bronze Iceberg writer sanitizes columns
            sanitized_src = self._sanitize_column_name(source_col)
            target_type = self._map_type(col_def["data_type"])
            
            if sanitized_src in df_silver.columns:
                # Add a cast error check
                is_cast_error = F.col(sanitized_src).isNotNull() & F.col(sanitized_src).cast(target_type).isNull()
                cast_error_struct = F.when(
                    is_cast_error, 
                    F.struct(
                        F.lit("SYS_CAST_ERR").alias("rule_id"),
                        F.lit(f"Cast failed from string for {target_name}").alias("error_message"),
                        F.lit(target_name).alias("failed_column")
                    )
                ).otherwise(F.lit(None))
                cast_error_cols.append(cast_error_struct)
                
                df_silver = df_silver.withColumn(target_name, F.col(sanitized_src).cast(target_type))
            else:
                df_silver = df_silver.withColumn(target_name, F.lit(None).cast(target_type))

        if cast_error_cols:
            df_silver = df_silver.withColumn("_sys_cast_errors", F.array(*cast_error_cols))
            df_silver = df_silver.withColumn("_sys_cast_errors", F.expr("filter(_sys_cast_errors, x -> x is not null)"))
        else:
            df_silver = df_silver.withColumn("_sys_cast_errors", F.array().cast("array<struct<rule_id:string,error_message:string,failed_column:string>>"))
                
        # Select target columns and actual audit columns
        target_cols = [c["name"] for c in contract.columns]
        actual_audit_cols = [c for c in audit_cols if c in df_silver.columns]
        
        df_silver = df_silver.select(*target_cols, *actual_audit_cols, "_sys_cast_errors")
        
        # 4. Data Quality Engine
        quality_engine = SilverQualityEngine(contract.data_quality_rules)
        valid_df, quarantine_df = quality_engine.apply_rules(df_silver)
        
        valid_count = valid_df.count()
        quarantine_count = quarantine_df.count()
        
        # 5. Quarantine
        self.quarantine_manager.ensure_table()
        self.quarantine_manager.route_quarantine(
            quarantine_df, 
            dataset=dataset_id, 
            run_id=pipeline_run_id, 
            business_keys=contract.business_key
        )
        
        # 6. Deduplication
        dedup_df = self.merge_engine.deduplicate(
            valid_df, 
            business_keys=contract.business_key
        )
        dedup_count = dedup_df.count()
        
        # 7. Merge into Silver Iceberg Table
        target_table = contract.silver_output
        self.merge_engine.merge(
            dedup_df, 
            target_table=target_table, 
            business_keys=contract.business_key
        )
        
        return {
            "status": "SUCCESS",
            "bronze_count": bronze_count,
            "valid_count": valid_count,
            "quarantine_count": quarantine_count,
            "dedup_count": dedup_count
        }

    def _sanitize_column_name(self, col_name: str) -> str:
        cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", str(col_name).strip())
        cleaned = re.sub(r"_+", "_", cleaned)
        if cleaned and cleaned[0].isdigit():
            cleaned = f"col_{cleaned}"
        return cleaned.lower()
        
    def _map_type(self, type_str: str) -> str:
        # Pyspark casting needs simpler type mappings if they are decimal(x,y)
        # decimal(18,2) works in cast("decimal(18,2)")
        # Just return the type_str directly as it aligns with spark SQL types
        return type_str
