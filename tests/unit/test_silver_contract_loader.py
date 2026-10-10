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

def test_load_contract_missing_fields(mock_contract_dir):
    loader = SilverContractLoader(contract_dir=mock_contract_dir)
    
    # Empty YAML
    with open(os.path.join(mock_contract_dir, "empty.yaml"), "w", encoding="utf-8") as f:
        f.write("")
    with pytest.raises(ValueError, match="Contract data must be a valid dictionary"):
        loader.load_contract("empty")

    # Invalid field types
    with open(os.path.join(mock_contract_dir, "invalid_type.yaml"), "w", encoding="utf-8") as f:
        yaml.dump({"dataset": "not_a_dict"}, f)
    with pytest.raises(ValueError, match="dataset \\(must be a dictionary\\)"):
        loader.load_contract("invalid_type")
        
    # Missing required field
    with open(os.path.join(mock_contract_dir, "missing_req.yaml"), "w", encoding="utf-8") as f:
        yaml.dump({"dataset": {"name": "Invalid Dataset"}}, f)
    with pytest.raises(ValueError, match="missing required field: dataset.source_system"):
        loader.load_contract("missing_req")
        
    # Empty business keys
    invalid_bk = {
        "dataset": {"name": "Test", "source_system": "SYS", "bronze_input": "b", "silver_output": "s"},
        "grain": {"business_key": []}
    }
    with open(os.path.join(mock_contract_dir, "empty_bk.yaml"), "w", encoding="utf-8") as f:
        yaml.dump(invalid_bk, f)
    with pytest.raises(ValueError, match="grain.business_key \\(must be a non-empty list\\)"):
        loader.load_contract("empty_bk")
        
    # Invalid schema column
    invalid_col = {
        "dataset": {"name": "Test", "source_system": "SYS", "bronze_input": "b", "silver_output": "s"},
        "grain": {"business_key": ["id"]},
        "schema": {"columns": [{"name": "col1"}]} # missing data_type
    }
    with open(os.path.join(mock_contract_dir, "invalid_col.yaml"), "w", encoding="utf-8") as f:
        yaml.dump(invalid_col, f)
    with pytest.raises(ValueError, match="missing name or data_type"):
        loader.load_contract("invalid_col")
