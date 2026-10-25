from decimal import Decimal
from pyspark.sql import functions as F
from silver.transformers.usda_psd import UsdaPsdTransformer
from silver.framework import SilverTransformationFramework
from silver.contract.loader import SilverContractLoader
from silver.engine.quality import SilverQualityEngine
from tests.unit.silver.faostat_nso_fixtures import sql_rows
from tests.unit.silver.test_faostat_nso_transformers import spark


def test_milling_rate_fraction_retains_source_value_and_unit(spark):
    raw = sql_rows(spark, [dict(country='Vietnam', commodity='Rice, Milled', attribute='Milling Rate (.9999)',
                               unit_description='(1000 MT)', col_2020_2021='6250')])
    transformer = UsdaPsdTransformer()
    contract = SilverContractLoader().load_contract('usda_psd')
    df = SilverTransformationFramework(spark).normalize(transformer.preprocess(raw), contract)
    valid, invalid = SilverQualityEngine(contract.data_quality_rules + transformer.quality_rules()).apply_rules(df)
    assert invalid.count() == 0
    row = valid.first()
    assert row.value == Decimal('0.6250') and row.value_source == Decimal('6250')
    assert row.unit == 'fraction' and row.source_unit == '(1000 MT)'
    assert row.conversion_factor == Decimal('0.0001')


def test_milling_invalid_fraction_and_raw_cast_error_are_quarantined(spark):
    raw = sql_rows(spark, [dict(country='Vietnam', commodity='Rice, Milled', attribute='Milling Rate (.9999)',
                               unit_description='1000 MT', col_2020_2021=v) for v in ['10001', 'bad', None]])
    transformer = UsdaPsdTransformer()
    contract = SilverContractLoader().load_contract('usda_psd')
    df = SilverTransformationFramework(spark).normalize(transformer.preprocess(raw), contract)
    valid, invalid = SilverQualityEngine(contract.data_quality_rules + transformer.quality_rules()).apply_rules(df)
    assert valid.count() == 1 and valid.first().value is None
    assert invalid.count() == 2
    assert 'SYS_CAST_ERR' in {e.rule_id for row in invalid.collect() for e in row.dq_errors}
