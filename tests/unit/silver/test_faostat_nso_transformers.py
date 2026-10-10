"""Business behavior of Hoang's adapters with the existing contract/DQ engines."""

import json
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession, functions as F

from silver.engine.merge import IcebergMergeEngine
from silver.transformers.faostat_production import FaostatProductionTransformer
from silver.transformers.faostat_monthly_price import FaostatMonthlyPriceTransformer
from silver.transformers.faostat_supply_utilization import FaostatSupplyUtilizationTransformer
from silver.transformers.nso_vietnam import NsoVietnamTransformer, NATIONAL_MEASURES
from tests.unit.silver.faostat_nso_fixtures import observations, sql_rows


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("HoangSourceTests")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield session
    session.stop()


@pytest.mark.parametrize("cls", [FaostatProductionTransformer, FaostatMonthlyPriceTransformer,
                                 FaostatSupplyUtilizationTransformer])
def test_contract_types_and_lineage(spark, cls):
    transformer = cls()
    raw = observations(spark)
    result = transformer.transform(raw)
    row = result.first()
    assert row.country_code == "704"
    assert row.commodity_code == "0113"
    assert row.year == 2020
    assert isinstance(row.value, Decimal)
    for c in transformer.contract.columns:
        assert result.schema[c["name"]].dataType.simpleString() == {"integer": "int"}.get(c["data_type"], c["data_type"])
    for c in raw.columns:
        if c.startswith("_"):
            assert row[c] == raw.first()[c]
    payload = json.loads(row._source_payload)
    assert payload["item_code_cpc_"] == "113.0"
    assert payload["area_code_m49_"] == "'704"
    assert payload["flag"] == "E"


@pytest.mark.parametrize("element,name,unit,value,expected,canonical", [
    ("5510", "Production", "kg", "2500", "2.50", "tonne"),
    ("5312", "Area harvested", "ha", "7", "7.00", "hectare"),
    ("5412", "Yield", "hg/ha", "25000", "2500.00", "kg/hectare"),
    ("5412", "Yield", "t/ha", "2.5", "2500.00", "kg/hectare"),
])
def test_measure_unit_conversion(spark, element, name, unit, value, expected, canonical):
    transformer = FaostatProductionTransformer()
    df = transformer.transform(observations(spark, {"element_code": element, "element": name, "unit": unit, "value": value}))
    valid, quarantine = transformer.validate(df)
    assert quarantine.count() == 0
    row = valid.first()
    assert row.value == Decimal(expected)
    assert row.unit == canonical


@pytest.mark.parametrize("override,rule", [
    ({"unit": "USD"}, "HOANG_PROD_UNIT"),
    ({"year": "2020.5"}, "FAO_YEAR"),
    ({"year": None}, "FAO_REQUIRED"),
    ({"value": "broken"}, "FAO_NUMBER"),
    ({"value": "1e100"}, "FAO_NUMBER"),
    ({"area_code_m49_": None}, "FAO_REQUIRED"),
    ({"item_code_cpc_": "9999"}, "FAO_CPC"),
    ({"element": "Yield"}, "HOANG_PROD_ELEMENT"),
    ({"value": "-1"}, "HOANG_PROD_VALUE"),
    ({"value": "-0.001"}, "HOANG_PROD_VALUE"),
    ({"value": "-0.0000000000001"}, "HOANG_PROD_VALUE"),
])
def test_invalid_observations_reach_shared_quarantine_partition(spark, override, rule):
    transformer = FaostatProductionTransformer()
    valid, quarantine = transformer.validate(transformer.transform(observations(spark, override)))
    assert valid.count() == 0
    row = quarantine.first()
    assert rule in {error.rule_id for error in row.dq_errors}
    assert json.loads(row._source_payload)[next(iter(override))] == next(iter(override.values()))


def test_missing_value_remains_null(spark):
    transformer = FaostatProductionTransformer()
    df = transformer.transform(observations(spark, {"value": None, "flag": "M"}))
    valid, quarantine = transformer.validate(df)
    assert valid.first().value is None
    assert valid.first().flag == "M"
    assert quarantine.count() == 0


def test_price_annual_monthly_index_and_currency(spark):
    transformer = FaostatMonthlyPriceTransformer()
    base = {"domain_code": "PP", "unit": "LCU", "element_code": "5530", "element": "Producer Price (LCU/tonne)"}
    rows = [base, dict(base, months_code="7001", months="January"),
            dict(base, element_code="5531", element="Producer Price (SLC/tonne)", unit="SLC"),
            dict(base, element_code="5532", element="Producer Price (USD/tonne)", unit="USD"),
            dict(base, element_code="5539", element="Producer Price Index (2014-2016 = 100)", unit=None)]
    valid, quarantine = transformer.validate(transformer.transform(observations(spark, *rows)))
    assert quarantine.count() == 0
    output = valid.collect()
    annual = [r for r in output if r.time_grain == "annual"]
    assert len(annual) == 4
    assert all(r.month is None for r in annual)
    assert [r.month for r in output if r.time_grain == "monthly"] == [1]
    assert {r.currency for r in output} == {"LCU", "SLC", "USD", None}
    index = [r for r in output if r.element_code == "5539"][0]
    assert index.price_kind == "producer_price_index"
    assert index.unit is None and index.currency is None
    assert len({tuple(r[k] for k in transformer.contract.business_key) for r in output}) == 5


@pytest.mark.parametrize("code,label,unit", [("7013", "January", "LCU"), ("7001", "February", "LCU"), ("7021", "Annual value", "USD")])
def test_price_date_and_currency_conflicts(spark, code, label, unit):
    transformer = FaostatMonthlyPriceTransformer()
    df = transformer.transform(observations(spark, {"element_code": "5530", "element": "Producer Price (LCU/tonne)",
                                                   "months_code": code, "months": label, "unit": unit}))
    valid, quarantine = transformer.validate(df)
    assert valid.count() == 0 and quarantine.count() == 1


def test_sua_products_stock_variation_and_residual_contract(spark):
    transformer = FaostatSupplyUtilizationTransformer()
    df = transformer.transform(observations(spark,
        {"value": "-7", "element_code": "5071", "element": "Stock Variation"},
        {"item_code_cpc_": "23161.02", "item": "Rice, milled"},
        {"value": "-2", "element_code": "5166", "element": "Residuals"}))
    valid, quarantine = transformer.validate(df)
    assert {r.commodity_code for r in valid.collect()} == {"0113", "23161.02"}
    assert valid.filter(F.col("element_code") == "5071").first().value == Decimal("-7")
    assert quarantine.first().value == Decimal("-2")
    assert quarantine.first().note == "source note"
    assert "HOANG_SUA_VALUE" in {e.rule_id for e in quarantine.first().dq_errors}


def test_reconciliation_and_deterministic_reprocessing(spark):
    transformer = FaostatProductionTransformer()
    raw = observations(spark, {"value": "10"}, {"value": "15", "_ingestion_timestamp": "2026-10-02 00:00:00"},
                       {"value": "-5"}, {"item_code_cpc_": "unknown"})
    outputs = []
    for _ in range(2):
        valid, quarantine = transformer.validate(transformer.transform(raw))
        dedup = IcebergMergeEngine(spark).deduplicate(valid, transformer.contract.business_key)
        assert raw.count() == valid.count() + quarantine.count() == 4
        assert valid.count() - dedup.count() == 1
        assert quarantine.count() == 2
        outputs.append(dedup.collect())
    assert outputs[0] == outputs[1]
    assert outputs[0][0].value == Decimal("15")


def test_missing_input_schema_fails_before_silent_null_projection(spark):
    with pytest.raises(ValueError, match="missing Bronze columns"):
        FaostatProductionTransformer().transform(observations(spark).drop("unit"))


def test_nso_lost_bronze_matrix_columns_fail_explicitly(spark):
    with pytest.raises(ValueError, match="repair Bronze schema"):
        NsoVietnamTransformer().preprocess(sql_rows(spark, [{"_source_file": "V06.13.csv", "nam": None}]))


def test_nso_national_quantities_indices_season_and_nulls(spark):
    row = {"_source_file": "V06.12.csv", "nam": "So b? 2024", "gi_tr_v_ch_s_ph_t_tri_n": "Giá tr?",
           **{c: "2.5" for c in NATIONAL_MEASURES}}
    row["di_n_t_ch_l_a_m_a_ngh_n_ha_"] = ".."
    index = dict(row, gi_tr_v_ch_s_ph_t_tri_n="Ch? s? phát tri?n (Nam tru?c =100) - %")
    transformer = NsoVietnamTransformer()
    result = transformer.preprocess(sql_rows(spark, [row, index]))
    valid, quarantine = transformer.validate(result)
    assert valid.count() == 16 and quarantine.count() == 0
    quantities = valid.filter(F.col("statistic_kind") == "quantity").collect()
    assert all(r.geography_level == "national" and r.province_code is None for r in quantities)
    assert all(r.year == 2024 and r.is_provisional for r in quantities)
    assert {r.season for r in quantities} == {"annual", "winter_spring", "summer_autumn", "mua"}
    assert {r.value for r in quantities} == {Decimal("2500"), None}
    assert valid.filter(F.col("statistic_kind") == "development_index").first().value == Decimal("2.5")


def test_nso_matrix_unknown_province_and_unconfirmed_unit_are_visible(spark):
    df = sql_rows(spark, [{"_source_file": "V06.18.csv", "t_nh_th_nh_ph_": "C? NU?C", "so_b_2024": "15"},
                         {"_source_file": "V06.18.csv", "t_nh_th_nh_ph_": "Hà N?i", "so_b_2024": "10"}])
    transformer = NsoVietnamTransformer()
    result = transformer.preprocess(df)
    assert result.count() == 2
    assert result.filter(F.col("geography_level") == "national").first().season == "winter_spring"
    unknown = result.filter(F.col("geography_level") == "unknown").first()
    assert unknown.geography_name_raw == "Hà N?i"
    assert unknown.province_code is None and unknown.mapping_status == "needs_review"
    assert unknown.year == 2024 and unknown.is_provisional
    valid, quarantine = transformer.validate(result)
    assert valid.count() == 0 and quarantine.count() == 2
