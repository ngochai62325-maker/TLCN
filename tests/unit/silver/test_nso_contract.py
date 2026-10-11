"""NSO source semantics independent of shared catalogs."""
from decimal import Decimal
import pytest
from pyspark.sql import functions as F
from silver.transformers.nso_vietnam import NsoVietnamTransformer, NATIONAL_MEASURES
from tests.unit.silver.faostat_nso_fixtures import sql_rows
from tests.unit.silver.test_faostat_nso_transformers import spark


def matrix_transformer(*columns):
    specs = NsoVietnamTransformer().inventory
    return NsoVietnamTransformer({f: dict(s, source_columns=['t_nh_th_nh_ph_', *columns], measure_columns=list(columns))
                                  for f, s in specs.items() if f != 'V06.12.csv'})


@pytest.mark.parametrize('file,raw,expected,unit', [
    ('V06.13.csv', '2.5', '2500', 'hectare'),
    ('V06.14.csv', '52.1', '5210', 'kg/hectare'),
    ('V06.15.csv', '2.5', '2500', 'tonne'),
])
def test_verified_matrix_units(spark, file, raw, expected, unit):
    transformer = matrix_transformer('col_2020')
    valid, invalid = transformer.validate(transformer.preprocess(sql_rows(spark, [
        {'_source_file': file, 't_nh_th_nh_ph_': ' Hà N?i ', 'col_2020': raw}])))
    assert invalid.count() == 0
    row = valid.first()
    assert row.value == Decimal(expected) and row.unit == unit
    assert row.geography_name_raw == ' Hà N?i ' and row.geography_code is None
    assert row.geography_name == 'Hà Nội' and row.geography_level == 'province'
    assert row.mapping_status == 'needs_review' and row._dq_warnings


def test_null_cells_are_kept_but_empty_records_are_audited(spark):
    transformer = matrix_transformer('col_2020', 'so_b_2024')
    raw = sql_rows(spark, [
        {'_source_file': 'V06.19.csv', 't_nh_th_nh_ph_': 'An Giang', 'col_2020': None, 'so_b_2024': '..'},
        {'_source_file': 'V06.19.csv', 't_nh_th_nh_ph_': None, 'col_2020': None, 'so_b_2024': None}])
    assert transformer.observation_audit(raw) == {'observation_count': 4, 'excluded_count': 2, 'excluded_record_count': 1}
    assert transformer.excluded_records(raw).count() == 1
    valid, invalid = transformer.validate(transformer.preprocess(raw))
    assert valid.count() == 2 and invalid.count() == 0
    assert {r.missing_kind for r in valid.collect()} == {'source_null', 'source_marker'}
    assert all(r.value is None and r.season == 'summer_autumn_autumn_winter' for r in valid.collect())


@pytest.mark.parametrize('raw', ['-0.00000000001', 'bad', '1e100'])
def test_bad_and_negative_raw_values_quarantined_under_ansi(spark, raw):
    transformer = matrix_transformer('col_2020')
    previous = spark.conf.get('spark.sql.ansi.enabled')
    spark.conf.set('spark.sql.ansi.enabled', 'true')
    try:
        valid, invalid = transformer.validate(transformer.preprocess(sql_rows(spark, [
            {'_source_file': 'V06.14.csv', 't_nh_th_nh_ph_': 'C? NU?C', 'col_2020': raw}])))
        assert valid.count() == 0 and invalid.count() == 1
    finally:
        spark.conf.set('spark.sql.ansi.enabled', previous)


def test_union_padding_never_creates_extra_observations(spark):
    transformer = matrix_transformer('col_2020')
    raw = sql_rows(spark, [{'_source_file': 'V06.13.csv', 't_nh_th_nh_ph_': 'C? NU?C',
                            'col_2020': '2', 'col_1990': None}])
    assert transformer.preprocess(raw).count() == 1


def test_unknown_file_and_missing_year_columns_fail(spark):
    with pytest.raises(ValueError, match='unprofiled'):
        NsoVietnamTransformer().preprocess(sql_rows(spark, [{'_source_file': 'V06.99.csv'}]))
    with pytest.raises(ValueError, match='repair Bronze schema'):
        NsoVietnamTransformer().preprocess(sql_rows(spark, [{'_source_file': 'V06.13.csv', 'col_2020': '1'}]))


def test_new_unprofiled_period_is_not_silently_dropped(spark):
    transformer = matrix_transformer('col_2020')
    with pytest.raises(ValueError, match='unprofiled period columns'):
        transformer.preprocess(sql_rows(spark, [{'_source_file': 'V06.13.csv', 't_nh_th_nh_ph_': 'C? NU?C',
                                                'col_2020': '2', 'col_2025': '3'}]))


def test_geography_candidates_keep_history_and_do_not_guess_unknown_labels(spark):
    transformer = matrix_transformer('col_2020')
    labels = [' Bà R?a - Vtng Tàu ', 'Ð?ng b?ng sông H?ng', 'Hà Tây', 'Hà Nội', 'Tỉnh không có trong mapping']
    valid, invalid = transformer.validate(transformer.preprocess(sql_rows(spark, [
        {'_source_file': 'V06.13.csv', 't_nh_th_nh_ph_': label, 'col_2020': '1'} for label in labels])))
    assert invalid.count() == 0
    rows = {r.geography_name_raw: r for r in valid.collect()}
    assert rows[labels[0]].geography_name == 'Bà Rịa - Vũng Tàu'
    assert rows[labels[1]].geography_name == 'Đồng bằng sông Hồng'
    assert rows[labels[1]].geography_level == 'region'
    assert rows['Hà Tây'].geography_name == 'Hà Tây'
    assert rows['Hà Tây'].geography_level == 'province'
    assert rows['Hà Nội'].geography_name == 'Hà Nội'
    assert rows[labels[-1]].geography_name is None
    assert rows[labels[-1]].geography_level == 'unknown'
    assert all(r.geography_code is None and r.province_code is None and r.mapping_status == 'needs_review'
               and r._dq_warnings for r in rows.values())


def test_geography_candidates_reject_ambiguous_or_malformed_mappings():
    with pytest.raises(ValueError, match='Ambiguous'):
        NsoVietnamTransformer(geography_candidates={'Hà N?i': ['Hà Nội', 'province'],
                                                   'Hà Nội': ['Tên khác', 'province']})
    with pytest.raises(ValueError, match='Invalid'):
        NsoVietnamTransformer(geography_candidates={'Hà N?i': ['Hà Nội', 'invalid-level']})
