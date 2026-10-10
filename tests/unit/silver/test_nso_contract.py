"""E1 contract, source columns, approved units and dated geography behavior."""
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession, functions as F

from silver.contract.loader import SilverContract, SilverContractLoader
from silver.framework import SilverTransformationFramework
from silver.engine.merge import IcebergMergeEngine
from silver.mappings.nso_geography import verified_entries
from silver.transformers.nso_vietnam import NsoVietnamTransformer, NATIONAL_MEASURES
from tests.unit.silver.faostat_nso_fixtures import sql_rows


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("NSOE1Unit")
        .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield session
    session.stop()


def national(**updates):
    return dict({"_source_file":"V06.12.csv", "nam":"2020", "gi_tr_v_ch_s_ph_t_tri_n":"GiÃ¡ tr?",
        **{c:"2.5" for c in NATIONAL_MEASURES}}, **updates)


def matrix_transformer(files=(13,), approved=False):
    transformer = NsoVietnamTransformer()
    for n in files:
        source = transformer.inventory[f"V06.{n}.csv"]
        source["source_columns"] = ["t_nh_th_nh_ph_", "col_2007", "so_b_2024"]
        source["measure_columns"] = ["col_2007", "so_b_2024"]
        if approved:
            source["status"] = "VERIFIED"
    return transformer


def test_draft_gate_before_any_read_or_write():
    framework = SilverTransformationFramework(MagicMock())
    framework.read_input = MagicMock()
    with pytest.raises(ValueError, match="pending contract"):
        framework.run("nso_vietnam", "never-write")
    framework.read_input.assert_not_called()
    contract = SilverContractLoader().load_contract("nso_vietnam")
    assert contract.status == "DRAFT" and len(contract.columns) == 47
    assert contract.silver_output == "iceberg.silver.nso_rice_statistics"
    with pytest.raises(ValueError):
        SilverContract({"contract_status":"READY", "critical_blockers":["unit"]}).require_ready()


@pytest.mark.parametrize("n,factor,canonical", [(13,1000,"hectare"),(14,100,"kg/hectare"),(15,1000,"tonne")])
def test_unit_conversion_only_verified(spark, n, factor, canonical):
    raw = sql_rows(spark,[{"_source_file":f"V06.{n}.csv","t_nh_th_nh_ph_":"C? NU?C","col_2007":"2.5","so_b_2024":".."}])
    transformer = matrix_transformer((n,), approved=True)
    result = transformer.transform(raw)
    row = result.filter("year=2007").first()
    assert row.value_source == Decimal("2.5") and row.value == Decimal("2.5")*factor
    assert row.unit == canonical and row.conversion_factor == Decimal(factor)
    missing = result.filter("year=2024").first()
    assert missing.is_provisional and missing._missing_value and missing.value is None
    transformer.inventory[f"V06.{n}.csv"]["status"] = "NEEDS_APPROVAL"
    unresolved = transformer.transform(raw).filter("year=2007").first()
    assert unresolved.value is None and unresolved.value_source == Decimal("2.5")
    assert unresolved.unit is None and unresolved.proposed_factor == factor


def test_index_provisional_and_all_five_seasons(spark):
    rows = [national(nam="So b? 2024"), national(nam="So b? 2024", gi_tr_v_ch_s_ph_t_tri_n="Ch? s? phÃ¡t tri?n (Nam tru?c =100) - %")]
    result = NsoVietnamTransformer().transform(sql_rows(spark,rows))
    assert {r.season for r in result.collect()} == {"annual","winter_spring","summer_autumn","mua"}
    index = result.filter("statistic_kind='development_index'").first()
    assert index.unit == index.source_unit == "percent" and index.conversion_factor == 1 and index.value == Decimal("2.5")
    assert index.proposed_factor == 1 and index.proposed_unit == "percent"
    assert index.is_provisional and index.year == 2024
    raw = sql_rows(spark,[{"_source_file":"V06.19.csv","t_nh_th_nh_ph_":"C? NU?C","col_2007":"1","so_b_2024":"2"}])
    assert matrix_transformer((19,)).transform(raw).first().season == "summer_autumn_autumn_winter"


def test_unknown_statistic_never_gets_quantity_conversion(spark):
    transformer = NsoVietnamTransformer()
    result = transformer.transform(sql_rows(spark, [national(gi_tr_v_ch_s_ph_t_tri_n="unknown index")]))
    row = result.first()
    assert row.statistic_kind is None and row.value_source == Decimal("2.5")
    assert row.value is None and row.unit is None and row.conversion_factor is None
    valid, dead = transformer.validate(result)
    assert valid.count() == 0 and dead.count() == 8


def test_normalized_decimal_overflow_is_quarantined(spark):
    transformer = NsoVietnamTransformer()
    raw = national(**{c:"99999999999999999999" for c in NATIONAL_MEASURES})
    result = transformer.transform(sql_rows(spark,[raw]))
    row = result.first()
    assert row.value_source is not None and row.conversion_factor == 1000
    assert row.value is None and not row._missing_value
    valid, dead = transformer.validate(result)
    assert valid.count() == 0 and dead.count() == 8


def test_actual_null_marker_blank_and_footer_audit(spark):
    transformer = matrix_transformer()
    rows = [{"_source_file":"V06.13.csv","t_nh_th_nh_ph_":"Hà N?i","col_2007":None,"so_b_2024":"..","_source_checksum":"checksum"},
            {"_source_file":"V06.13.csv","t_nh_th_nh_ph_":"An Giang","col_2007":"","so_b_2024":"0","_source_checksum":"checksum"},
            {"_source_file":"V06.13.csv","t_nh_th_nh_ph_":None,"col_2007":None,"so_b_2024":None,"_source_checksum":"checksum"}]
    raw = sql_rows(spark,rows)
    result = transformer.transform(raw)
    assert result.count() == 4
    assert {r.missing_kind for r in result.collect()} == {"source_null","source_marker","source_blank","present"}
    assert all(r.value is None for r in result.collect())  # Unit remains unapproved, even actual zero.
    audit = transformer.observation_audit(raw)
    assert audit["observation_count"] == 6 and audit["excluded_count"] == 2 and audit["excluded_record_count"] == 1
    empty_national = dict.fromkeys(national(), None)
    empty_national.update(_source_file="V06.12.csv", _source_checksum="checksum")
    footer = sql_rows(spark,[empty_national])
    assert NsoVietnamTransformer().transform(footer).count() == 0
    assert NsoVietnamTransformer().observation_audit(footer)["excluded_count"] == 8


def test_absent_source_column_does_not_generate_observation(spark):
    transformer = matrix_transformer((13,14))
    transformer.inventory["V06.14.csv"]["source_columns"] = ["t_nh_th_nh_ph_","col_2007"]
    transformer.inventory["V06.14.csv"]["measure_columns"] = ["col_2007"]
    raw = sql_rows(spark,[{"_source_file":"V06.13.csv","t_nh_th_nh_ph_":"C? NU?C","col_2007":None,"so_b_2024":".."},
                         {"_source_file":"V06.14.csv","t_nh_th_nh_ph_":"C? NU?C","col_2007":"10","so_b_2024":None}])
    result = transformer.transform(raw)
    assert result.count() == 3 and result.filter("source_table='V06.14.csv' AND year=2024").count() == 0
    assert result.filter("missing_kind='source_null'").count() == 1
    with pytest.raises(ValueError,match="repair Bronze schema"):
        transformer.transform(raw.drop("col_2007"))


def mapping(**updates):
    return dict({"source_table":"V06.13.csv","raw_label":"Hà N?i","canonical_name":"Hà Nội",
        "code":"HISTORICAL_FIXTURE_ONLY","code_scheme":"TEST","geography_level":"province",
        "parent_geography":"TEST_PARENT","valid_from":"2000-01-01","valid_to":"2007-12-31",
        "source_reference":"TEST reviewed source","status":"VERIFIED"}, **updates)


def test_historical_mapping_full_year_exact_and_unapproved(spark):
    transformer = matrix_transformer()
    transformer.geographies = [mapping(), mapping(valid_from="2008-08-01",valid_to="2024-12-31",code="TEST_EXPANDED")]
    raw = sql_rows(spark,[{"_source_file":"V06.13.csv","t_nh_th_nh_ph_":"Hà N?i","col_2007":"1","so_b_2024":"2"}])
    result = transformer.transform(raw)
    assert result.filter("year=2007").first().province_code == "HISTORICAL_FIXTURE_ONLY"
    assert result.filter("year=2024").first().province_code == "TEST_EXPANDED"
    transformer.inventory["V06.13.csv"]["measure_columns"] = ["col_2008"]
    transformer.inventory["V06.13.csv"]["source_columns"] = ["t_nh_th_nh_ph_","col_2008"]
    transition = sql_rows(spark,[{"_source_file":"V06.13.csv","t_nh_th_nh_ph_":"Hà N?i","col_2008":"3"}])
    assert transformer.transform(transition).first().mapping_status == "needs_review"
    transformer.geographies = [mapping(status="NEEDS_APPROVAL")]
    assert transformer.transform(transition).first().province_code is None
    not_exact = sql_rows(spark,[{"_source_file":"V06.13.csv","t_nh_th_nh_ph_":" Hà N?i","col_2008":"3"}])
    assert transformer.transform(not_exact).first().mapping_status == "needs_review"


def test_mapping_overlap_and_incomplete_verified_fail():
    with pytest.raises(ValueError,match="Overlapping"):
        verified_entries([mapping(), mapping(valid_from="2007-12-31")])
    with pytest.raises(ValueError,match="source reference"):
        verified_entries([mapping(code=None)])
    assert verified_entries([{"status":"NEEDS_APPROVAL"}]) == []


def test_grain_keeps_sources_geography_seasons_and_statistics(spark):
    transformer = NsoVietnamTransformer()
    rows = [national(), national(gi_tr_v_ch_s_ph_t_tri_n="Ch? s? phÃ¡t tri?n (Nam tru?c =100) - %")]
    result = transformer.transform(sql_rows(spark,rows))
    assert result.select(*transformer.business_keys).distinct().count() == result.count() == 16
    assert result.filter("gold_ready").count() == 0
    # An exact repeated source row generates repeated observations, and the
    # shared survivor logic removes only duplicates within the complete NSO key.
    repeated = result.unionByName(result)
    survivors = IcebergMergeEngine(spark).deduplicate(repeated, transformer.business_keys)
    assert repeated.count() == 32 and survivors.count() == 16
    assert survivors.select(*transformer.business_keys).distinct().count() == 16
    matrix = matrix_transformer()
    raw = sql_rows(spark, [{"_source_file":"V06.13.csv", "t_nh_th_nh_ph_":label,
                          "col_2007":"10", "so_b_2024":"20"}
                          for label in ("C? NU?C", "Tây Nguyên", "An Giang")])
    scoped = matrix.transform(raw)
    assert scoped.select(*matrix.business_keys).distinct().count() == scoped.count() == 6
    assert {r.geography_level for r in scoped.collect()} == {"national", "region", "unknown"}
