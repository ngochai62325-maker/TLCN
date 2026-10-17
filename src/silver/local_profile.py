"""Explicit manual-only nine-source routing; independent of the accepted release."""
import copy
import hashlib
import os
from pathlib import Path

PROFILE = "local_nine"
NAMESPACE = "silver_local"
# Bronze source -> Silver dataset -> physical target. Never derive scope from enabled entries.
MAPPINGS = (
    ("faostat_production", "faostat_production", "faostat_production"),
    ("faostat_monthly_price", "faostat_monthly_price", "faostat_monthly_price"),
    ("faostat_supply_utilization", "faostat_supply_utilization", "faostat_supply_utilization"),
    ("faostat_trade", "faostat_trade", "faostat_trade"),
    ("usda_psd", "usda_psd", "usda_rice_psd"),
    ("usda_rice_yearbook", "usda_rice_yearbook", "usda_rice_yearbook"),
    ("worldbank_pinksheet", "worldbank_pinksheet", "worldbank_commodity_monthly"),
    ("thitruongnongsan", "thitruongnongsan", "thitruongnongsan"),
    ("nso", "nso", "nso_rice_statistics"),
)
BRONZE_SOURCES = tuple(m[0] for m in MAPPINGS)
DATASETS = tuple(m[1] for m in MAPPINGS)
REGISTRY_SHA256 = "c8ae17ad7e5a538ef9a6e44427bc5ec2c443dc285788c34fb2275bd8acb7b1f3"
NSO_CONTRACT_SHA256 = "9246124b891138c18d851356a95d62b2fe177adb2c73df7968fb6d7e5ee0eb8b"


def active_profile():
    profile, enabled = os.getenv("SILVER_LOCAL_PROFILE"), os.getenv("SILVER_LOCAL_ENABLED", "0")
    if not profile and enabled == "0":
        return None
    if profile != PROFILE or enabled != "1":
        raise ValueError("local_nine requires explicit SILVER_LOCAL_PROFILE and SILVER_LOCAL_ENABLED=1")
    if (os.getenv("SILVER_ACCEPTED_RELEASE") or os.getenv("SILVER_PRODUCTION_ENABLED", "0") != "0"
            or os.getenv("SILVER_CANARY_ONLY", "0") != "0" or os.getenv("SILVER_BRONZE_SCHEDULE")):
        raise ValueError("local_nine conflicts with release/production/canary/schedule flags")
    return PROFILE


def require_profile(profile, release=None):
    if profile != PROFILE or release or active_profile() != PROFILE:
        raise ValueError("Explicit activated local_nine profile required; no release/default fallback")


def mapping(dataset):
    for source, name, target in MAPPINGS:
        if dataset == name:
            return source, target
    raise ValueError(f"Dataset outside local_nine: {dataset}")


def validate_registry(registry=None):
    require_profile(PROFILE)
    from ingestion.config.registry import SourceRegistry
    registry = registry or SourceRegistry()
    registry.load()
    sources = registry.get_all_sources()
    if hashlib.sha256(Path(registry.registry_path).read_bytes()).hexdigest() != REGISTRY_SHA256:
        raise ValueError("local_nine registry checksum drift")
    for name in BRONZE_SOURCES:
        config = sources.get(name)
        if (config is None or not config.enabled or config.source_id != name
                or config.source_type.value != "LOCAL_FILE" or "fixtures" in (config.local_path or "").split("/")):
            raise ValueError(f"Missing/disabled/unapproved local_nine registry source: {name}")
    return [sources[name] for name in BRONZE_SOURCES]


def local_contract(loader, dataset):
    require_profile(PROFILE)
    from silver.contract.loader import SilverContract
    source, target = mapping(dataset)
    original = loader.load_contract(dataset)
    if original.bronze_input != f"iceberg.bronze.{source}" or original.silver_output != f"iceberg.silver.{target}":
        raise ValueError(f"local_nine source/target contract routing mismatch: {dataset}")
    data = copy.deepcopy(original._data)
    if dataset == "nso":
        path = Path(loader.contract_dir) / (dataset + ".yaml")
        if original.status != "READY" or hashlib.sha256(path.read_bytes()).hexdigest() != NSO_CONTRACT_SHA256:
            raise ValueError("local_nine NSO contract requires the exact approved READY contract")
    original.validate_executable()
    data["dataset"]["silver_output"] = f"iceberg.{NAMESPACE}.{target}"
    contract = SilverContract(data)
    contract.validate_executable()
    return contract
