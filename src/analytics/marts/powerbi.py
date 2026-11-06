"""Bounded source-scoped extracts for the six-page Power BI report.

All reads use ExportSession; these functions never access Trino/catalog directly.
"""
from datetime import date
from decimal import Decimal
import json
import math

from analytics.powerbi import require, number, integer
from gold.rules import SUA, metric_rule

CONTRACT_VERSION='powerbi-marts-1'

SCHEMAS = {
 'fao_production.csv': [('year','integer'),('geography_key','text'),('area_code','text'),('area_name','text'),('product_key','text'),('product_code','text'),('production_t','decimal?'),('area_ha','decimal?'),('reported_yield_kg_ha','decimal?'),('computed_yield_kg_ha','decimal?'),('production_present','boolean'),('area_present','boolean'),('yield_present','boolean'),('paired_valid','boolean'),('paired_production_t','decimal?'),('paired_area_ha','decimal?')],
 'wb_benchmark.csv': [('month_date','text'),('year','integer'),('month_key','integer'),('series_key','text'),('series_code','text'),('unit','text'),('price_usd_t','decimal?'),('observation_present','boolean'),('numeric_present','boolean'),('previous_month_numeric','boolean'),('monthly_change_pct','decimal?'),('volatility_12m_pct','decimal?')],
 'vn_quote.csv': [('date','text'),('year','integer'),('series_key','text'),('market','text'),('commodity','text'),('price_type','text'),('unit','text'),('currency','text'),('information_source','text'),('price_vnd_kg','decimal?'),('observation_present','boolean'),('numeric_present','boolean'),('days_since_previous_quote','integer?')],
 'trade_exports.csv': [('year','integer'),('reporter_key','text'),('reporter_code','text'),('partner_key','text'),('partner_code','text'),('partner_name','text'),('product_key','text'),('product_code','text'),('direction','text'),('quantity_t','decimal?'),('value_usd','decimal?'),('unit_value_usd_t','decimal?'),('quantity_present','boolean'),('value_present','boolean'),('denominator_valid','boolean'),('paired_quantity_t','decimal?'),('paired_value_usd','decimal?')],
 'sua_components.csv': [('year','integer'),('geography_key','text'),('area_code','text'),('product_key','text'),('product_code','text'),('element_code','text'),('metric','text'),('unit','text'),('value_t','decimal?'),('observation_present','boolean'),('aggregation_rule','text')],
 'usda_psd.csv': [('market_year','integer'),('crop_year','text'),('geography_key','text'),('country','text'),('product_key','text'),('product','text'),('attribute','text'),('unit','text'),('value','decimal?'),('observation_present','boolean'),('aggregation_rule','text')],
}
KEYS = {'fao_production.csv':['year','geography_key','product_key'], 'wb_benchmark.csv':['month_key','series_key'],
 'vn_quote.csv':['date','series_key'], 'trade_exports.csv':['year','reporter_key','partner_key','product_key','direction'],
 'sua_components.csv':['year','geography_key','product_key','element_code'], 'usda_psd.csv':['market_year','geography_key','product_key','attribute']}


def labels(value):
    data=json.loads(value)
    return ' | '.join(dict.fromkeys(str(x['label']) if isinstance(x,dict) else str(x) for x in data if x is not None))


def unique(frame, fields):
    require(frame,fields)
    if frame.duplicated(fields).any():raise ValueError('Ambiguous report source grain')
    return frame


def member(frame, **filters):
    out=frame
    for col,val in filters.items():out=out[out[col].eq(val)]
    if len(out)!=1:raise ValueError('Unavailable/ambiguous report source identity: '+str(filters))
    return out.iloc[0]


def present(value):
    n=integer(value)
    if n not in (0,1):raise ValueError('Semantic cell has multiple contributors')
    return n==1


def ratio(n,d,factor=1):
    return None if n is None or d is None or d<=0 else n*Decimal(factor)/d


def check(name,rows):
    if not rows or len(rows)>5000:raise ValueError('Empty/oversized required report extract')
    keys=[tuple(r[k] for k in KEYS[name]) for r in rows]
    if len(keys)!=len(set(keys)):raise ValueError('Duplicate report business grain')
    for r in rows:
        if name=='fao_production.csv':
            if r['area_code'] not in ('356','360','704','764') or r['product_code']!='0113' or not 1961<=r['year']<=2024:raise ValueError('FAO source scope mismatch')
            for col,flag in [('production_t','production_present'),('area_ha','area_present'),('reported_yield_kg_ha','yield_present')]:
                if not r[flag] and r[col] is not None:raise ValueError('Absent production metric has value')
            if r['paired_valid']!=(r['production_t'] is not None and r['area_ha'] is not None and r['area_ha']>0):raise ValueError('Invalid paired yield coverage')
            expected=ratio(r['production_t'],r['area_ha'],1000)
            if expected!=r['computed_yield_kg_ha']:raise ValueError('Paired yield mismatch')
            if r['paired_production_t']!=(r['production_t'] if r['paired_valid'] else None) or r['paired_area_ha']!=(r['area_ha'] if r['paired_valid'] else None):raise ValueError('FAO paired population mismatch')
        if name=='trade_exports.csv':
            if r['direction']!='export' or r['reporter_code']!='704' or r['partner_code'] not in ('156','458','608') or r['product_code']!='23161.02' or not 2019<=r['year']<=2023:raise ValueError('Trade export scope drift')
            if (not r['quantity_present'] and r['quantity_t'] is not None) or (not r['value_present'] and r['value_usd'] is not None):raise ValueError('Absent trade cell has value')
            if r['denominator_valid']!=(r['quantity_t'] is not None and r['quantity_t']>0):raise ValueError('Trade denominator mismatch')
            if r['unit_value_usd_t']!=ratio(r['value_usd'],r['quantity_t']):raise ValueError('Trade pairing mismatch')
            eligible=r['denominator_valid'] and r['value_usd'] is not None
            if r['paired_quantity_t']!=(r['quantity_t'] if eligible else None) or r['paired_value_usd']!=(r['value_usd'] if eligible else None):raise ValueError('Trade paired population mismatch')
        if name in ('wb_benchmark.csv','vn_quote.csv'):
            value=r['price_usd_t' if name=='wb_benchmark.csv' else 'price_vnd_kg']
            if r['numeric_present']!=(value is not None) or not r['observation_present'] and value is not None:raise ValueError('Price presence mismatch')
        if name=='sua_components.csv':
            if r['area_code']!='704' or r['product_code']!='23161.02' or not 2010<=r['year']<=2023 or r['element_code'] not in SUA or r['metric']!=SUA[r['element_code']] or r['unit']!='tonne' or not r['observation_present']:raise ValueError('SUA metric/unit scope drift')
            if r['aggregation_rule']!=metric_rule('faostat_supply_utilization',r['element_code'],'tonne')[2]:raise ValueError('SUA aggregation policy drift')
        if name=='usda_psd.csv':
            from gold.rules import PSD
            unit,_,rule=metric_rule('usda_psd',r['attribute'],PSD.get(r['attribute']))
            if r['country']!='Vietnam' or r['product']!='Rice, Milled' or not 1960<=r['market_year']<=2025 or r['unit']!=unit or r['aggregation_rule']!=rule or not r['observation_present']:raise ValueError('PSD semantics drift')
        if name=='wb_benchmark.csv':
            d=date.fromisoformat(r['month_date'])
            if d.day!=1 or d.year!=r['year'] or d.year*100+d.month!=r['month_key'] or r['series_code']!='rice_thai_5' or r['unit']!='USD/tonne' or not 200001<=r['month_key']<=202512:raise ValueError('WB month/unit scope mismatch')
        if name=='vn_quote.csv':
            d=date.fromisoformat(r['date'])
            identity=[r[k] for k in ('market','commodity','price_type','unit','currency','information_source')]
            if identity!=['An Giang','Lúa IR 50404 (lúa tươi)','Thương lái thu mua','VND/kg','VND','CTV địa phương'] or d.year!=r['year'] or not date(2021,1,1)<=d<=date(2026,8,3) or not r['observation_present']:raise ValueError('Domestic source scope drift')
    if keys!=sorted(keys):raise ValueError('Unstable report row ordering')
    if name=='wb_benchmark.csv':
        expected=[y*100+m for y in range(2000,2026) for m in range(1,13)]
        if [r['month_key'] for r in rows]!=expected:raise ValueError('Incomplete WB calendar coverage')
        for i,r in enumerate(rows):
            prior=rows[i-1]['price_usd_t'] if i else None
            change=(ratio(r['price_usd_t'],prior)-1)*100 if ratio(r['price_usd_t'],prior) is not None else None
            if r['previous_month_numeric']!=(prior is not None) or change!=r['monthly_change_pct']:raise ValueError('Non-calendar monthly change')
            window=[v['price_usd_t'] for v in rows[max(0,i-12):i+1]]
            eligible=len(window)==13 and all(v is not None and v>0 for v in window)
            if (r['volatility_12m_pct'] is not None)!=eligible:raise ValueError('Invalid volatility window coverage')
            if eligible:
                import statistics
                vol=statistics.stdev(math.log(float(b/a)) for a,b in zip(window,window[1:]))*100
                if abs(float(r['volatility_12m_pct'])-vol)>1e-10:raise ValueError('Volatility formula mismatch')
    if name=='vn_quote.csv':
        for i,r in enumerate(rows):
            gap=(date.fromisoformat(r['date'])-date.fromisoformat(rows[i-1]['date'])).days if i else None
            if r['days_since_previous_quote']!=gap:raise ValueError('Quote gap mismatch')


def contracts():
    """Small executable contract exported verbatim with every v2 mart bundle."""
    return {'version':CONTRACT_VERSION,'namespace':'iceberg.gold_local','datasets':{
        name:{'schema':[{'name':n,'type':t} for n,t in schema],'business_key':KEYS[name],
              'policy':'check() enforces source domains, units, required coverage, NULL/presence and eligible-pair equality',
              'ratio_policy':'Use paired base sums; never average row ratios. Levels require one compatible period.'}
        for name,schema in SCHEMAS.items()}}


def build(session):
    geos=unique(session.query('catalogue',kind='geographies'),['geography_key'])
    products=unique(session.query('catalogue',kind='products'),['product_key'])
    series=unique(session.query('catalogue',kind='series'),['price_series_key'])
    def geo(code):return member(geos,identity_namespace='faostat_m49',identity_scope='faostat',source_identity=code)
    paddy=member(products,identity_namespace='faostat_cpc',source_identity='0113')
    milled=member(products,identity_namespace='faostat_cpc',source_identity='23161.02')
    selected=[geo(c) for c in ('356','360','704','764')];vn=geo('704');files={};lineage={};scopes={}
    params=dict(geographies=[g.geography_key for g in selected],product=paddy.product_key,start_year=1961,end_year=2024)
    f=unique(session.query('fao_production',**params),['year_key','geography_key','product_key'])
    rows=[];lineage['fao_production.csv']=[]
    for x in f.to_dict('records'):
        g=member(geos,geography_key=x['geography_key'])
        if g.source_identity not in ('356','360','704','764') or x['product_key']!=paddy.product_key or x['product_identity']!='0113':raise ValueError('FAO scope drift')
        values=[number(x[c]) for c in ('production_t','area_ha','reported_yield_kg_ha')]
        rows.append(dict(year=integer(x['year_key']),geography_key=g.geography_key,area_code=g.source_identity,area_name=labels(g.source_labels_json),product_key=paddy.product_key,product_code='0113',production_t=values[0],area_ha=values[1],reported_yield_kg_ha=values[2],computed_yield_kg_ha=ratio(values[0],values[1],1000),production_present=present(x['present_5510']),area_present=present(x['present_5312']),yield_present=present(x['present_5412']),paired_valid=values[0] is not None and values[1] is not None and values[1]>0))
        lineage['fao_production.csv'].append({k:x[k] for k in ('year_key','geography_key','observation_5312','observation_5510','observation_5412')})
    files['fao_production.csv']=rows;scopes['fao_production.csv']=params
    # Use source series identities, never infer an exporter from a benchmark name.
    wb=series[series.series_identity_json.map(lambda s:json.loads(s)==['rice_thai_5'])]
    if len(wb)!=1:raise ValueError('WB benchmark series ambiguous')
    domestic_identity=['An Giang','Lúa IR 50404 (lúa tươi)','Thương lái thu mua','VND/kg','VND','CTV địa phương']
    domestic=series[series.series_identity_json.map(lambda s:json.loads(s)==domestic_identity)]
    if len(domestic)!=1:raise ValueError('Domestic quote series unavailable')
    for name,sr,fact,start,end,unit in [('wb_benchmark.csv',wb.iloc[0],'fact_price_monthly',200001,202512,'USD/tonne'),('vn_quote.csv',domestic.iloc[0],'fact_vn_market_quote_daily',20210101,20260803,'VND/kg')]:
        params=dict(fact=fact,series=sr.price_series_key,start_period=start,end_period=end)
        data=unique(session.query('price_observations',**params),['period','price_series_key'])
        if not data.unit.eq(unit).all() or not data.price_series_key.eq(sr.price_series_key).all():raise ValueError('Price series/unit drift')
        lineage[name]=data[['observation_key','period']].to_dict('records');scopes[name]=params
        rows=[]
        if name=='wb_benchmark.csv':
            cells={integer(x['period']):number(x['value']) for x in data.to_dict('records')};history=[]
            for n in range(start//100*12+start%100-1,end//100*12+end%100):
                y,m=divmod(n,12);m+=1;k=y*100+m;value=cells.get(k);prev=history[-1] if history else None
                history.append(value)
                window=history[-13:];vol=None
                if len(window)==13 and all(v is not None and v>0 for v in window):
                    returns=[math.log(float(b/a)) for a,b in zip(window,window[1:])]
                    mean=sum(returns)/12;vol=Decimal(str((sum((v-mean)**2 for v in returns)/11)**.5*100))
                rows.append(dict(month_date=date(y,m,1).isoformat(),year=y,month_key=k,series_key=sr.price_series_key,series_code='rice_thai_5',unit=unit,price_usd_t=value,observation_present=k in cells,numeric_present=value is not None,previous_month_numeric=prev is not None,monthly_change_pct=(ratio(value,prev)-1)*100 if ratio(value,prev) is not None else None,volatility_12m_pct=vol))
        else:
            last=None
            for x in data.sort_values('period').to_dict('records'):
                raw=str(integer(x['period']));d=date(int(raw[:4]),int(raw[4:6]),int(raw[6:]));value=number(x['value'])
                rows.append(dict(date=d.isoformat(),year=d.year,series_key=sr.price_series_key,**dict(zip(['market','commodity','price_type','unit','currency','information_source'],domestic_identity)),price_vnd_kg=value,observation_present=True,numeric_present=value is not None,days_since_previous_quote=(d-last).days if last else None));last=d
        files[name]=rows
    params=dict(geography=vn.geography_key,product=milled.product_key,start_year=2010,end_year=2023)
    data=unique(session.query('sua',**params),['year_key','geography_key','product_key']);rows=[];lineage['sua_components.csv']=[]
    for x in data.to_dict('records'):
        if x['geography_key']!=vn.geography_key or x['product_key']!=milled.product_key:raise ValueError('SUA scope drift')
        for code,metric in SUA.items():
            if present(x['present_'+code]):
                rows.append(dict(year=integer(x['year_key']),geography_key=vn.geography_key,area_code='704',product_key=milled.product_key,product_code='23161.02',element_code=code,metric=metric,unit='tonne',value_t=number(x[metric]),observation_present=True,aggregation_rule=metric_rule('faostat_supply_utilization',code,'tonne')[2]))
                lineage['sua_components.csv'].append(dict(year=x['year_key'],element_code=code,observation_key=x['observation_'+code]))
    files['sua_components.csv']=rows;scopes['sua_components.csv']=params
    g=member(geos,identity_namespace='usda_psd_country',source_identity='Vietnam');p=member(products,identity_namespace='usda_psd',source_identity='Rice, Milled')
    params=dict(geography=g.geography_key,product=p.product_key,start_year=1960,end_year=2025)
    data=unique(session.query('psd',**params),['market_year','geography_key','product_key','metric']);rows=[]
    from gold.rules import PSD
    for x in data.to_dict('records'):
        unit,_,rule=metric_rule('usda_psd',x['metric'],PSD.get(x['metric']))
        if x['geography_key']!=g.geography_key or x['product_key']!=p.product_key or x['unit']!=unit:raise ValueError('PSD scope/unit drift')
        rows.append(dict(market_year=integer(x['market_year']),crop_year=x['crop_year'],geography_key=g.geography_key,country='Vietnam',product_key=p.product_key,product='Rice, Milled',attribute=x['metric'],unit=x['unit'],value=number(x['value']),observation_present=True,aggregation_rule=rule))
    files['usda_psd.csv']=rows;lineage['usda_psd.csv']=data[['observation_key','market_year','metric']].to_dict('records');scopes['usda_psd.csv']=params
    rows=[];lineage['trade_exports.csv']=[];scopes['trade_exports.csv']=[]
    for year in range(2019,2024):
        params=dict(reporter=vn.geography_key,product=milled.product_key,flow='export',start_year=year,end_year=year)
        data=unique(session.query('trade',**params),['year_key','reporter_geography_key','partner_geography_key','product_key'])
        for x in data.to_dict('records'):
            if x['partner_identity'] not in ('156','458','608'):continue
            if x['reporter_geography_key']!=vn.geography_key or x['product_key']!=milled.product_key or integer(x['year_key'])!=year:raise ValueError('Trade identity drift')
            q,v=number(x['quantity_t']),number(x['value_usd'])
            rows.append(dict(year=year,reporter_key=vn.geography_key,reporter_code='704',partner_key=x['partner_geography_key'],partner_code=x['partner_identity'],partner_name=labels(x['partner_labels']),product_key=milled.product_key,product_code='23161.02',direction='export',quantity_t=q,value_usd=v,unit_value_usd_t=ratio(v,q),quantity_present=present(x['present_quantity']),value_present=present(x['present_value']),denominator_valid=q is not None and q>0))
            lineage['trade_exports.csv'].append({k:x[k] for k in ('year_key','partner_geography_key','observation_quantity','observation_value')})
        scopes['trade_exports.csv'].append(params)
    files['trade_exports.csv']=rows
    for name,rows in files.items():
        if name in ('fao_production.csv','trade_exports.csv'):
            for row in rows:
                if name=='fao_production.csv':
                    row['paired_production_t']=row['production_t'] if row['paired_valid'] else None
                    row['paired_area_ha']=row['area_ha'] if row['paired_valid'] else None
                else:
                    eligible=row['denominator_valid'] and row['value_usd'] is not None
                    row['paired_quantity_t']=row['quantity_t'] if eligible else None
                    row['paired_value_usd']=row['value_usd'] if eligible else None
        rows.sort(key=lambda r:tuple(r[k] for k in KEYS[name]));check(name,rows)
    return files,lineage,{'report_scopes':scopes,'report_reconciliation':{'status':'PASS','method':'typed source projections, presence/contributor identities, exact Decimal ratios, unique grains, CSV roundtrip','rows':{n:len(v) for n,v in files.items()}},'derived_precision':'Ratios use Decimal precision 28; volatility uses float log returns/sample std, 12 consecutive returns (13 numeric positive monthly prices); not annualized.'}
