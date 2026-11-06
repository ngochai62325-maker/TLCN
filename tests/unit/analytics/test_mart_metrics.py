from decimal import Decimal
import pytest
from analytics.marts.metrics import paired_ratio,period_level,endpoints,yoy


def test_ratio_of_sums_uses_same_population_not_average_of_ratios():
    rows=[{'q':Decimal(1),'v':Decimal(10)},{'q':Decimal(9),'v':Decimal(18)},{'q':Decimal(50),'v':None},{'q':Decimal(0),'v':Decimal(999)}]
    assert paired_ratio(rows,'v','q')==Decimal('2.8')
    assert paired_ratio(rows[:1],'v','q')==10
    assert paired_ratio(rows[2:],'v','q') is None


def test_weighted_yield_and_country_filters():
    rows=[{'p':Decimal(2),'a':Decimal(1)},{'p':Decimal(24),'a':Decimal(3)},{'p':None,'a':Decimal(9)}]
    assert paired_ratio(rows,'p','a',1000)==6500
    assert paired_ratio(rows[:1],'p','a',1000)==2000
    assert paired_ratio(rows[1:2],'p','a',1000)==8000


@pytest.mark.parametrize('values',[[{'y':2020,'v':None}],[{'y':2020,'v':1},{'y':2021,'v':2}],[]])
def test_undefined_multi_year_or_missing_levels(values):assert period_level(values,'y','v') is None


def test_stock_and_price_single_period_zero_identity_policy():
    assert period_level([{'y':2020,'v':Decimal(0),'unit':'tonne'}],'y','v',['unit'])==0
    assert period_level([{'y':2020,'v':1,'unit':'tonne'},{'y':2020,'v':2,'unit':'USD'}],'y','v',['unit']) is None


def test_yoy_requires_calendar_previous_not_previous_observed():
    rows=[{'y':2020,'v':Decimal(100)},{'y':2022,'v':Decimal(120)},{'y':2023,'v':Decimal(0)}]
    assert yoy(rows,'y','v',2022) is None
    assert yoy(rows,'y','v',2023)==-100
    assert yoy(rows+[{'y':2024,'v':Decimal(10)}],'y','v',2024) is None


def test_endpoints_follow_scope_ignore_null_and_peak_ties():
    rows=[{'y':2020,'v':None},{'y':2021,'v':Decimal(10)},{'y':2022,'v':Decimal(20)},{'y':2023,'v':Decimal(20)}]
    a=endpoints(rows,'y','v');b=endpoints(rows[2:],'y','v')
    assert a['first_period']==2021 and a['peak_period']==2022 and a['change_pct']==100
    assert b['first_period']==2022 and b['change_pct']==0
    assert endpoints(rows[:1],'y','v') is None
    assert endpoints(rows[1:2],'y','v')['change_pct'] is None


def test_project_measures_reference_paired_bases_and_single_periods():
    from analytics.marts.report import MEASURES
    assert 'FAO[paired_production_t]' in MEASURES['FAO Paired Production']['dax']
    assert 'Trade[paired_value_usd]' in MEASURES['Trade Paired Value']['dax']
    assert 'DIVIDE([Trade Paired Value],[Trade Paired Quantity])'==MEASURES['Trade Unit Value']['dax']
    assert 'HASONEVALUE(PSD[market_year])' in MEASURES['PSD Selected Value']['dax']
