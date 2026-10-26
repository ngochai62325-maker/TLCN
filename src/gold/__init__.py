"""Source-scoped local Gold model. No catalog connections at import time."""

MODEL_VERSION = "gold-20261025.1"
INPUT_NAMESPACE = "iceberg.silver_local"
OUTPUT_NAMESPACE = "iceberg.gold_local"
OWNER = "tlcn-local"
ACTIONS = ("preflight", "dimensions", "fact", "validate", "publish")
BUILD_ORDER = ("worldbank_pinksheet", "faostat_production", "faostat_supply_utilization", "faostat_trade", "usda_psd", "nso", "thitruongnongsan", "faostat_monthly_price", "usda_rice_yearbook")
