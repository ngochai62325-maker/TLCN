import os
import yaml
import sys

def load_yaml(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def validate_contracts():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    registry_path = os.path.join(project_root, 'config', 'ingestion', 'source_registry.yaml')
    contracts_dir = os.path.join(project_root, 'contracts', 'silver')

    if not os.path.exists(registry_path):
        print(f"Error: Registry not found at {registry_path}")
        return False

    registry = load_yaml(registry_path)
    registered_sources = set(registry.get('sources', {}).keys())
    
    if not os.path.exists(contracts_dir):
        print(f"Error: Contracts directory not found at {contracts_dir}")
        return False

    all_passed = True
    for filename in os.listdir(contracts_dir):
        if not filename.endswith('.yaml'):
            continue
        
        filepath = os.path.join(contracts_dir, filename)
        print(f"Validating {filename}...")
        
        try:
            contract = load_yaml(filepath)
        except Exception as e:
            print(f"  [FAIL] YAML parsing error: {e}")
            all_passed = False
            continue

        dataset = contract.get('dataset', {})
        source_id = dataset.get('source_id')
        
        if not source_id:
            print(f"  [FAIL] Missing dataset.source_id")
            all_passed = False
        elif source_id not in registered_sources:
            print(f"  [FAIL] source_id '{source_id}' not found in source_registry.yaml")
            all_passed = False
            
        status = contract.get('contract_status')
        if status not in ['READY', 'NEEDS_PROFILING', 'DRAFT']:
            print(f"  [FAIL] Invalid contract_status: {status}")
            all_passed = False

        required_sections = ['dataset', 'contract_status', 'grain', 'schema', 'normalization', 'data_quality', 'deduplication', 'quarantine', 'metadata', 'incremental', 'gold_dependency']
        for sec in required_sections:
            if sec not in contract:
                print(f"  [FAIL] Missing required section: {sec}")
                all_passed = False

        if status == 'READY':
            columns = contract.get('schema', {}).get('columns', [])
            col_names = [c.get('name') for c in columns]
            if len(col_names) != len(set(col_names)):
                print(f"  [FAIL] Duplicate column names found in schema")
                all_passed = False

        if all_passed:
            print(f"  [PASS] {filename} is valid.")

    return all_passed

if __name__ == '__main__':
    if validate_contracts():
        print("\nAll contracts validated successfully!")
        sys.exit(0)
    else:
        print("\nValidation failed.")
        sys.exit(1)
