import pytest
import os
import tempfile
import yaml
from silver.contract.loader import SilverContractLoader, SilverContract

@pytest.fixture
def mock_contract_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        contract_data = {
            "dataset": {
                "name": "Test Dataset",
                "source_system": "TEST",
                "bronze_input": "bronze.test",
                "silver_output": "silver.test"
            },
            "grain": {
                "business_key": ["id1", "id2"]
            },
            "schema": {
                "columns": [
                    {"name": "col1", "source_column": "Col 1", "data_type": "string"},
                    {"name": "col2", "source_column": "Col 2", "data_type": "integer"}
                ]
            },
            "data_quality": [
                {"rule_id": "RQ_1", "rule": "col2 >= 0"}
            ],
            "deduplication": {
                "key": ["id1", "id2"],
                "strategy": "Timestamp-based survivorship"
            }
        }
        
        file_path = os.path.join(temp_dir, "test_dataset.yaml")
        with open(file_path, "w", encoding="utf-8") as f:
            yaml.dump(contract_data, f)
            
        yield temp_dir

def test_load_contract(mock_contract_dir):
    loader = SilverContractLoader(contract_dir=mock_contract_dir)
    contract = loader.load_contract("test_dataset")
    
    assert contract.dataset_name == "Test Dataset"
    assert contract.source_system == "TEST"
    assert contract.bronze_input == "bronze.test"
    assert contract.silver_output == "silver.test"
    assert contract.business_key == ["id1", "id2"]
    
    assert len(contract.columns) == 2
    assert contract.columns[0]["name"] == "col1"
    
    assert len(contract.data_quality_rules) == 1
    assert contract.deduplication_strategy == "Timestamp-based survivorship"
