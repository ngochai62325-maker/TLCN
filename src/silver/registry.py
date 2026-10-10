"""Production Silver source registry; safe to import without Spark."""
# Canonical Bronze source IDs; fixtures have no production entry.
TRANSFORMERS = {
    "faostat_production": ("faostat_production", "FaostatProductionTransformer"),
    "faostat_monthly_price": ("faostat_monthly_price", "FaostatMonthlyPriceTransformer"),
    "faostat_supply_utilization": ("faostat_supply_utilization", "FaostatSupplyUtilizationTransformer"),
    "faostat_trade": ("faostat_trade_transformer", "FaostatTradeTransformer"),
    "usda_psd": ("usda_psd_transformer", "UsdaPsdTransformer"),
    "usda_rice_yearbook": ("usda_export_price_transformer", "UsdaExportPriceTransformer"),
    "worldbank_pinksheet": ("worldbank_transformer", "WorldBankTransformer"),
    "thitruongnongsan": ("thitruongnongsan", "ThitruongnongsanTransformer"),
    "nso_vietnam": ("nso_vietnam", "NsoVietnamTransformer"),
}

