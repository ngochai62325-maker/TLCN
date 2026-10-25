"""Validate registry linkage and executable READY Silver contracts without Spark."""
import sys
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from silver.contract.loader import SilverContractLoader


def validate_contracts():
    sources = yaml.safe_load((ROOT / "config/ingestion/source_registry.yaml").read_text(encoding="utf-8"))["sources"]
    loader = SilverContractLoader(str(ROOT / "contracts/silver"))
    passed = True
    for path in sorted((ROOT / "contracts/silver").glob("*.yaml")):
        try:
            contract = loader.load_contract(path.stem)
            if contract.source_id not in sources:
                raise ValueError("dataset.source_id not in source_registry")
            if contract.status == "READY":
                contract.validate_executable()
                print(f"[PASS] {path.stem}: executable READY contract")
            elif contract.status in {"DRAFT", "NEEDS_PROFILING"}:
                print(f"[PENDING] {path.stem}: {contract.status}; excluded from production runner")
            else:
                raise ValueError(f"Unknown status {contract.status}")
        except Exception as exc:
            print(f"[FAIL] {path.stem}: {exc}")
            passed = False
    return passed


if __name__ == "__main__":
    sys.exit(0 if validate_contracts() else 1)
