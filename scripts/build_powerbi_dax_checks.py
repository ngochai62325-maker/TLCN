"""Generate a real Desktop DAX assertion query with independent CSV references."""
import argparse
from decimal import Decimal
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))


def build(bundle):
    from analytics.powerbi import verify_bundle,csv_read,SCHEMAS as old
    from analytics.marts.powerbi import SCHEMAS
    from analytics.marts.metrics import paired_ratio
    verify_bundle(bundle);bundle=Path(bundle)
    data={n:csv_read(bundle/n,s) for n,s in {**old,**SCHEMAS}.items()}
    tests=[]
    def add(name,expression,expected,tolerance='0.000001'):
        if expected is None:ok=f'ISBLANK({expression})'
        else:ok=f'NOT ISBLANK({expression}) && ABS(({expression})-({expected}))<={tolerance}'
        tests.append((name,ok))
    def cy(measure,year):return f'CALCULATE([{measure}],CalendarYear[Year]={year})'
    add('overview_total','[Total Observations]',sum(r['rows_total'] for r in data['fact_overview.csv']))
    add('overview_numeric','[Numeric Observations]',sum(r['numeric_rows'] for r in data['fact_overview.csv']))
    add('overview_null','[Present NULL]',sum(r['present_null_rows'] for r in data['fact_overview.csv']))
    add('overview_ratio','[Numeric Coverage]',Decimal(1402167)/Decimal(1414527))
    for year in (1990,2015,2024):
        r=next(r for r in data['nso_national_annual.csv'] if r['year']==year)
        add('production_'+str(year),cy('Annual Production',year),r['production_t'])
        add('area_'+str(year),cy('Annual Area',year),r['area_ha'])
    add('no_invented_2025',cy('Annual Production',2025),None)
    add('annual_multi_year','[Annual Production]',None)
    add('reported_yield_2024',cy('Reported Yield',2024),6100)
    add('observed_peak','[Peak Production]',45091000)
    add('observed_peak_year','[Peak Year]',2015)
    a=next(r['production_t'] for r in data['nso_national_annual.csv'] if r['year']==2023)
    b=next(r['production_t'] for r in data['nso_national_annual.csv'] if r['year']==2024)
    add('calendar_yoy',cy('Production YoY',2024),b/a-1)
    add('latest_yoy','[Latest Production YoY]',b/a-1)
    add('first_endpoint','[First Production]',19225100)
    add('last_endpoint','[Latest Production]',43451600)
    add('endpoint_change','[Production Change Pct]',Decimal(43451600)/Decimal(19225100)-1)
    fao=[r for r in data['fao_production.csv'] if r['year']==2024]
    add('weighted_four_areas',cy('FAO Weighted Yield',2024),paired_ratio(fao,'paired_production_t','paired_area_ha',1000))
    vn=[r for r in fao if r['area_code']=='704']
    add('weighted_vietnam',f'CALCULATE([FAO Weighted Yield],CalendarYear[Year]=2024,FAO[area_code]="704")',paired_ratio(vn,'paired_production_t','paired_area_ha',1000))
    add('weighted_multiple_years','[FAO Weighted Yield]',None)
    for name,code in [('SUA Food Supply','5141'),('SUA Opening Stocks','5113'),('SUA Stock Variation','5071')]:
        r=next(r for r in data['sua_components.csv'] if r['year']==2023 and r['element_code']==code)
        add(name.replace(' ','_'),cy(name,2023),r['value_t'])
    add('stock_no_time_sum','[SUA Opening Stocks]',None)
    for measure,attr in [('PSD Production','Production'),('PSD Consumption','Domestic Consumption'),('PSD Ending Stocks','Ending Stocks')]:
        r=next(r for r in data['usda_psd.csv'] if r['market_year']==2025 and r['attribute']==attr)
        add(measure.replace(' ','_'),f'CALCULATE([{measure}],PSD[market_year]=2025)',r['value'])
    add('PSD_stock_multi_year','[PSD Ending Stocks]',None)
    add('WB_peak_month','CALCULATE([WB Price],BenchmarkMonth[Date]=DATE(2008,4,1))',907)
    add('WB_min_month','CALCULATE([WB Price],BenchmarkMonth[Date]=DATE(2001,4,1))',Decimal('163.75'))
    add('WB_level_multi_month','[WB Price]',None)
    add('WB_peak','[WB Peak]',907)
    add('WB_minimum','[WB Minimum]',Decimal('163.75'))
    row=next(r for r in data['wb_benchmark.csv'] if r['month_key']==200901)
    add('WB_volatility','CALCULATE([WB Volatility],BenchmarkMonth[Date]=DATE(2009,1,1))',row['volatility_12m_pct']/100)
    add('WB_mom','CALCULATE([WB MoM],BenchmarkMonth[Date]=DATE(2009,1,1))',row['monthly_change_pct']/100)
    add('quote_dates','[Domestic Quoted Dates]',484)
    add('quote_multi_date','[Domestic Quote]',None)
    trade=data['trade_exports.csv']
    for column,measure in [('quantity_t','Trade Quantity'),('value_usd','Trade Value'),('paired_quantity_t','Trade Paired Quantity'),('paired_value_usd','Trade Paired Value')]:
        add(measure.replace(' ','_'),'['+measure+']',sum((r[column] for r in trade if r[column] is not None),Decimal(0)),'.01')
    add('trade_ratio_multi_partner_multi_year','[Trade Unit Value]',paired_ratio(trade,'paired_value_usd','paired_quantity_t'))
    for year in (2019,2023):
        rows=[r for r in trade if r['partner_code']=='608' and r['year']==year]
        add('trade_philippines_'+str(year),f'CALCULATE([Trade Unit Value],CalendarYear[Year]={year},Trade[partner_code]="608")',paired_ratio(rows,'paired_value_usd','paired_quantity_t'))
    add('no_trade_2024',cy('Trade Quantity',2024),None)
    add('empty_trade_group_count',cy('Trade Paired Groups',2024),0)
    add('WB_absent_count','[WB Absent Months]',sum(not r['observation_present'] for r in data['wb_benchmark.csv']))
    add('FAO_common_year','[FAO Common Year]',2024)
    add('FAO_selected_rank_sum','SUMX(SUMMARIZECOLUMNS(FAO[area_name],"rank",[FAO Common Rank]),[rank])',10)
    for partner in ('156','458','608'):
        rows=[r for r in trade if r['partner_code']==partner]
        add('trade_multi_year_'+partner,f'CALCULATE([Trade Unit Value],Trade[partner_code]="{partner}")',paired_ratio(rows,'paired_value_usd','paired_quantity_t'))
    start=next(r['production_t'] for r in data['nso_national_annual.csv'] if r['year']==2015)
    add('selected_endpoints_2015_2024','CALCULATE([Production Change Pct],CalendarYear[Year] IN {2015,2024})',b/start-1)
    add('single_year_change_blank',cy('Production Change Pct',1990),None)
    tests.append(('single_year_card_label',f'{cy("Production Change Pct Display",1990)}="Không áp dụng"'))
    query='EVALUATE\nVAR Checks = UNION(\n'+',\n'.join('    ROW("Test",'+json.dumps(n)+',"Passed",'+ok+')' for n,ok in tests)+'\n)\nRETURN ROW("Tests",COUNTROWS(Checks),"Failures",COALESCE(COUNTROWS(FILTER(Checks,NOT [Passed])),0),"Failed tests",CONCATENATEX(FILTER(Checks,NOT [Passed]),[Test],", "))\n'
    return query,len(tests)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--bundle',type=Path,required=True);a=p.parse_args();query,n=build(a.bundle)
    out=ROOT/'powerbi/validation.dax';out.write_text(query,encoding='utf-8');print(str(out),n,'DAX assertions')
