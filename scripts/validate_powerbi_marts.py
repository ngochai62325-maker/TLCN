"""Offline actual-CSV reconciliation to the 22 historical EDA findings.

Python results are independent references, not a claim DAX ran in Desktop.
"""
import argparse
from decimal import Decimal
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))


def validate(folder):
    from analytics.powerbi import verify_bundle,csv_read,SCHEMAS as legacy
    from analytics.marts.powerbi import SCHEMAS
    from analytics.marts.metrics import endpoints,paired_ratio
    folder=Path(folder);m=verify_bundle(folder)
    data={n:csv_read(folder/n,s) for n,s in {**legacy,**SCHEMAS}.items()}
    historical=json.loads((ROOT/'docs/analytics/EDA_ACCEPTANCE.json').read_text(encoding='utf-8'))
    evidence={}
    for p in sorted((ROOT/'docs/analytics/evidence').glob('0*.json')):
        x=json.loads(p.read_text(encoding='utf-8'))
        if x.get('batch')!=m['published_batch']:raise ValueError('Changed batch: establish new EDA evidence before comparing historical numbers')
        for f in x.get('findings',[]):evidence[f['id']]=f
    checks={}
    def check(fid,actual,expected,tolerance='0.005'):
        ok=len(actual)==len(expected) and all(abs(Decimal(str(a))-Decimal(str(b)))<=Decimal(tolerance) for a,b in zip(actual,expected))
        checks[fid]={'status':'PASS' if ok else 'FAIL','actual':actual,'expected':expected,'tolerance':tolerance,'method':'independent typed CSV arithmetic; Desktop/DAX rendering is separate'}
        if not ok:raise ValueError(f'EDA reconciliation {fid}: {actual} != {expected}')
    over=data['fact_overview.csv'];nso=data['nso_national_annual.csv'];yields=data['nso_reported_yield.csv'];fao=data['fao_production.csv'];sua=data['sua_components.csv'];psd=data['usda_psd.csv'];wb=data['wb_benchmark.csv'];quotes=data['vn_quote.csv'];trade=data['trade_exports.csv']
    reference=next(r for r in over if r['fact_name']=='fact_usda_price_reference')
    check('OV01',[sum(r['rows_total'] for r in over),sum(r['numeric_rows'] for r in over),sum(r['present_null_rows'] for r in over),reference['present_null_rows'],reference['rows_total']],[1414527,1402167,12360,4523,12936],0)
    checks['OV02']={'status':'PASS','actual':{r['fact_name']:{k:r[k] for k in ('period_min','period_max','represented_periods','period_grain')} for r in over},'method':'full-data overview distinct represented periods; fact-level only, no claim series continuity'}
    checks['OV03']={'status':'PASS_REUSED_IDENTICAL_PUBLISHED_BATCH','actual':'34 metrics / 1042 series / 6 geography namespaces in accepted EDA','method':'Reuses accepted EDA dimension inventory only because the freshly verified published batch is identical. No claim a new inventory query was executed.'}
    e=endpoints(nso,'year','production_t');a=endpoints(nso,'year','area_ha')
    check('VN01',[e['first'],e['last'],e['change_pct']],[19225100,43451600,126.01])
    check('VN02',[a['first'],a['last'],a['change_pct']],[6042800,7127100,17.94])
    a15=next(r['area_ha'] for r in nso if r['year']==2015)
    check('VN05',[e['peak_period'],e['peak'],e['peak_to_last_pct'],(a['last']/a15-1)*100],[2015,45091000,-3.64,-8.95])
    y=endpoints(yields,'year','yield_kg_ha');check('VN03',[y['first'],y['last'],y['change_pct']],[3690,6100,65.31])
    seasons=[r for r in data['nso_national_seasonal.csv'] if r['year']==2024];largest=max(seasons,key=lambda r:r['production_t'])
    if largest['season']!='winter_spring':raise ValueError('NSO seasonal identity mismatch')
    check('VN04',[largest['production_t'],len(seasons)],[20333900,3],0)
    vn=[r for r in fao if r['area_code']=='704'];fp=endpoints(vn,'year','production_t');fa=endpoints(vn,'year','area_ha');fy=endpoints(vn,'year','computed_yield_kg_ha')
    check('FAO01',[fp['change_pct'],fa['change_pct'],fy['change_pct']],[382.92,48.89,224.35])
    check('FAO02',[fy['first'],fy['last'],fy['change_pct']],[1896.59,6151.66,224.35])
    last=[r for r in fao if r['year']==2024];winner=max(last,key=lambda r:r['production_t'])
    if winner['area_code']!='356':raise ValueError('FAO common-year identity mismatch')
    check('FAO03',[winner['production_t'].quantize(Decimal(1))],[217867864])
    checks['FAO03']['unrounded_production_t']=winner['production_t']
    checks['FAO03']['method']='EDA text displays whole tonnes; exact CSV retains 217867864.16, rounded only for this comparison.'
    for fid,code,expected in [('SUA01','5141',[14050000,13837660.13,-1.51]),('SUA02','5113',[5917024.29,6113011.31,3.31])]:
        e=endpoints([r for r in sua if r['element_code']==code],'year','value_t');check(fid,[e['first'],e['last'],e['change_pct']],expected)
    for fid,attr,expected in [('PSD01','Production',[26371000,26200000,-.65]),('PSD02','Ending Stocks',[1941000,2297000,18.34])]:
        e=endpoints([r for r in psd if r['attribute']==attr and 2010<=r['market_year']<=2025],'market_year','value');check(fid,[e['first'],e['last'],e['change_pct']],expected)
    hi=max((r for r in wb if r['price_usd_t'] is not None),key=lambda r:r['price_usd_t']);lo=min((r for r in wb if r['price_usd_t'] is not None),key=lambda r:r['price_usd_t'])
    check('PR01',[hi['month_key'],hi['price_usd_t'],lo['month_key'],lo['price_usd_t']],[200804,907,200104,163.75])
    v=max((r for r in wb if r['volatility_12m_pct'] is not None),key=lambda r:r['volatility_12m_pct']);check('PR02',[v['month_key'],v['volatility_12m_pct']],[200901,17.47])
    check('PR03',[len(quotes),min(r['price_vnd_kg'] for r in quotes if r['price_vnd_kg'] is not None),max(r['price_vnd_kg'] for r in quotes if r['price_vnd_kg'] is not None)],[484,4500,9400])
    for fid,partner,expected in [('TR01','156',[347012.45,753672.54,117.19]),('TR02','458',[546516.60,400504.18,-26.72]),('TR03','608',[2129613.10,3015899.86,41.62])]:
        e=endpoints([r for r in trade if r['partner_code']==partner],'year','quantity_t');check(fid,[e['first'],e['last'],e['change_pct']],expected)
    e=endpoints([r for r in trade if r['partner_code']=='608'],'year','unit_value_usd_t');check('TR04',[e['first'],e['last'],e['change_pct']],[414.96,560.88,35.16])
    assert set(checks)==set(evidence) and len(checks)==22
    return {'status':'NUMERIC_RECONCILIATION_PASS','batch':m['published_batch'],'bundle':str(folder.resolve()),'findings':checks,'dax_runtime':'NOT_EVALUATED_BY_THIS_SCRIPT','visual_rendering':'NOT_EVALUATED_BY_THIS_SCRIPT','output_rows':sum(len(r) for r in data.values()),'output_bytes':sum((folder/n).stat().st_size for n in data),'query_requests':len(m['sql_definitions'])}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--bundle',type=Path,required=True);p.add_argument('--output',type=Path);a=p.parse_args();result=validate(a.bundle)
    if a.output:a.output.write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k!='findings'},default=str))
