import os
import yaml
from typing import Any, Dict, List

class SilverContract:
    def __init__(self, data: Dict[str, Any]):
        self._data = data
        self._validate()

    def _validate(self):
        if not isinstance(self._data, dict):
            raise ValueError("Contract data must be a valid dictionary. Empty or malformed YAML is not allowed.")
            
        dataset = self._data.get("dataset")
        if not isinstance(dataset, dict):
            raise ValueError("Contract is missing required field: dataset (must be a dictionary)")
            
        if not dataset.get("name"):
            raise ValueError("Contract is missing required field: dataset.name")
        if not dataset.get("source_system"):
            raise ValueError("Contract is missing required field: dataset.source_system")
        if not dataset.get("bronze_input"):
            raise ValueError("Contract is missing required field: dataset.bronze_input")
        if not dataset.get("silver_output"):
            raise ValueError("Contract is missing required field: dataset.silver_output")
            
        grain = self._data.get("grain")
        if not isinstance(grain, dict):
            raise ValueError("Contract is missing required field: grain (must be a dictionary)")
            
        business_key = grain.get("business_key")
        if not isinstance(business_key, list) or len(business_key) == 0:
            raise ValueError("Contract is missing required field: grain.business_key (must be a non-empty list)")
            
        schema = self._data.get("schema")
        if not isinstance(schema, dict):
            raise ValueError("Contract is missing required field: schema (must be a dictionary)")
            
        columns = schema.get("columns")
        if not isinstance(columns, list) or len(columns) == 0:
            raise ValueError("Contract is missing required field: schema.columns (must be a non-empty list)")
            
        for col in columns:
            if not isinstance(col, dict):
                raise ValueError(f"Contract column definition must be a dictionary: {col}")
            if not col.get("name") or not col.get("data_type"):
                raise ValueError(f"Contract column definition missing name or data_type: {col}")

    @property
    def dataset_name(self) -> str:
        return self._data.get("dataset", {}).get("name", "")

    @property
    def source_system(self) -> str:
        return self._data.get("dataset", {}).get("source_system", "")

    @property
    def bronze_input(self) -> str:
        return self._data.get("dataset", {}).get("bronze_input", "")

    @property
    def silver_output(self) -> str:
        return self._data.get("dataset", {}).get("silver_output", "")

    @property
    def business_key(self) -> List[str]:
        return self._data.get("grain", {}).get("business_key", [])

    @property
    def columns(self) -> List[Dict[str, Any]]:
        return self._data.get("schema", {}).get("columns", [])

    @property
    def data_quality_rules(self) -> List[Dict[str, Any]]:
        return self._data.get("data_quality", [])

    @property
    def deduplication_key(self) -> List[str]:
        return self._data.get("deduplication", {}).get("key", [])
        
    @property
    def deduplication_strategy(self) -> str:
        return self._data.get("deduplication", {}).get("strategy", "")

    @property
    def incremental_strategy(self) -> str:
        return self._data.get("incremental", {}).get("strategy", "")

    @property
    def quarantine_required_fields(self) -> List[str]:
        return self._data.get("quarantine", {}).get("required_fields", [])


class SilverContractLoader:
    def __init__(self, contract_dir: str = "contracts/silver"):
        self.contract_dir = contract_dir

    def load_contract(self, dataset_id: str) -> SilverContract:
        file_path = os.path.join(self.contract_dir, f"{dataset_id}.yaml")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Silver contract not found: {file_path}")
            
        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            
        return SilverContract(data)
