import pandas as pd
import pytest

from analytics.methods import calendar_yoy, monthly_calendar, endpoint_change


def test_yoy_calendar_gap_null_zero_and_ambiguity():
    df=pd.DataFrame({"year_key":[2020,2022,2023,2024],"value":[10,20,0,3]})
    out=calendar_yoy(df,[])
    assert pd.isna(out.loc[1,'yoy_pct']) and out.loc[2,'yoy_pct']==-100 and pd.isna(out.loc[3,'yoy_pct'])
    with pytest.raises(ValueError,match='Ambiguous'):calendar_yoy(pd.concat([df,df]),[])


def test_monthly_absent_row_is_distinct_from_present_null_and_no_fill():
    frame=pd.DataFrame({'period':[202401,202403,202404],'value':[10,None,0]})
    out=monthly_calendar(frame)
    assert list(out['observed_row'])==[True,False,True,True]
    assert out['value'].isna().sum()==2 and out.iloc[-1]['value']==0
    assert out['rolling_mean_12m'].isna().all()


def test_cagr_interval_and_zero_endpoint():
    assert endpoint_change(pd.DataFrame({'year_key':[2020,2022],'value':[10,40]}))['cagr_pct']==100
    assert endpoint_change(pd.DataFrame({'year_key':[2020,2021],'value':[0,40]}))['cagr_pct'] is None


def test_empty_scope_cannot_produce_a_blank_analytical_chart():
    from analytics.notebook import annual_plot
    with pytest.raises(ValueError,match='No numeric observations'):
        annual_plot(pd.DataFrame(columns=['year_key','value']),'value','test','tonne')
