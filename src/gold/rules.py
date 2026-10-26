"""Finite, reviewed source metric/unit rules (U1, U2, A1)."""
from decimal import Decimal, localcontext

SUA = {"5016": "loss_t", "5023": "processed_t", "5071": "stock_variation_t", "5113": "opening_stocks_t", "5141": "food_supply_quantity_t", "5165": "other_uses_t", "5166": "residuals_t", "5510": "production_t", "5520": "feed_t", "5525": "seed_t", "5610": "import_quantity_t", "5910": "export_quantity_t"}
PSD = {"Area Harvested": "1000 HA", "Beginning Stocks": "1000 MT", "Domestic Consumption": "1000 MT", "Ending Stocks": "1000 MT", "Exports": "1000 MT", "Imports": "1000 MT", "Milling Rate (.9999)": "fraction", "Production": "1000 MT", "Rough Production": "1000 MT", "TY Exports": "1000 MT", "TY Imp. from U.S.": "1000 MT", "TY Imports": "1000 MT", "Total Distribution": "1000 MT", "Total Supply": "1000 MT", "Yield": "MT/HA"}
PRODUCTION = {"5312": "hectare", "5510": "tonne", "5412": "kg/hectare"}
TRADE = {"5610": "tonne", "5910": "tonne", "5622": "1000 USD", "5922": "1000 USD"}
PRICE_UNITS = {"5530": "LCU/tonne", "5531": "SLC/tonne", "5532": "USD/tonne", "5539": "index(2014-2016=100)"}


def metric_rule(source, metric, unit):
    expected = {"faostat_production": PRODUCTION, "faostat_supply_utilization": {c: "tonne" for c in SUA}, "faostat_trade": TRADE, "usda_psd": PSD}[source]
    if metric not in expected or unit != expected[metric]:
        raise ValueError(f"Unapproved metric/unit: {source}, {metric!r}, {unit!r}")
    factor = 1000 if unit in {"1000 USD", "1000 MT", "1000 HA"} else 1
    gold_unit = {"1000 USD": "USD", "1000 MT": "tonne", "1000 HA": "hectare", "MT/HA": "tonne/hectare"}.get(unit, unit)
    rule = "AREA_OR_FLOW_NONOVERLAP"
    if (source == "faostat_production" and metric == "5412") or metric in {"Yield", "Milling Rate (.9999)"}:
        rule = "NEVER_SUM"
    elif (source == "faostat_supply_utilization" and metric == "5113") or metric in {"Beginning Stocks", "Ending Stocks"}:
        rule = "NO_SUM_OVER_TIME"
    elif metric in {"Total Supply", "Total Distribution"}:
        rule = "NO_SUM_WITH_COMPONENTS"
    elif source == "faostat_supply_utilization" and metric in {"5071", "5166"}:
        rule = "SIGNED_FLOW"
    elif source == "usda_psd" and metric in {"Imports", "Exports"}:
        rule = "MY_FLOW"
    elif metric in {"TY Imports", "TY Exports"}:
        rule = "TY_FLOW"
    elif metric == "TY Imp. from U.S.":
        rule = "TY_SUBSET"
    return gold_unit, Decimal(factor), rule


def checked_decimal(value, factor, precision, scale):
    if value is None:
        return None
    with localcontext() as ctx:
        ctx.prec = 60
        result = Decimal(value) * Decimal(factor)
        quantized = result.quantize(Decimal(1).scaleb(-scale))
        if not result.is_finite() or result != quantized or abs(result) >= Decimal(10) ** (precision - scale):
            raise ValueError(f"Decimal overflow/rounding: decimal({precision},{scale})")
        return quantized
