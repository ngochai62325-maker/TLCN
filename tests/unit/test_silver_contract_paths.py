"""CLI/Spark API must work from the container's default working directory."""
from silver.contract.loader import SilverContractLoader


def test_default_contract_location_is_independent_of_working_directory(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    contract = SilverContractLoader().load_contract('nso_vietnam')
    contract.validate_executable()
    assert contract.silver_output == 'iceberg.silver.nso_rice_statistics'
