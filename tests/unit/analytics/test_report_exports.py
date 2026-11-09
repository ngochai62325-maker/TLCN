"""Synthetic edge cases prove transforms, not physical report acceptance."""
from decimal import Decimal
import copy
import json

import pandas as pd
import pytest

from analytics.marts.powerbi import build,check,ratio,SCHEMAS,KEYS
from analytics.powerbi import csv_read,write_csv,verify_bundle
from gold.keys import key
from gold.rules import SUA


class Session:
    def __init__(self,mutate=None):self.mutate=mutate;self.calls=[]
    def query(self,name,**p):
        self.calls.append((name,p))
        def geo(code):return key('dim_source_geography',['faostat_m49','faostat',code])
        product=lambda code:key('dim_source_product',['faostat_cpc',code])
        if name=='catalogue':
            if p['kind']=='geographies':
                rows=[dict(geography_key=geo(c),identity_namespace='faostat_m49',identity_scope='faostat',source_identity=c,source_labels_json=json.dumps([{'label':'Area '+c}])) for c in ('356','360','704','764','156','458','608')]
                rows.append(dict(geography_key='usda',identity_namespace='usda_psd_country',identity_scope='usda_psd',source_identity='Vietnam',source_labels_json='[]'))
            elif p['kind']=='products':rows=[dict(product_key=product(c),identity_namespace='faostat_cpc',source_identity=c) for c in ('0113','23161.02')]+[dict(product_key='rice',identity_namespace='usda_psd',source_identity='Rice, Milled')]
            else:rows=[dict(price_series_key='wb',series_identity_json='["rice_thai_5"]'),dict(price_series_key='vn',series_identity_json=json.dumps(['An Giang','Lúa IR 50404 (lúa tươi)','Thương lái thu mua','VND/kg','VND','CTV địa phương']))]
        elif name=='fao_production':
            rows=[dict(year_key=y,geography_key=g,product_key=product('0113'),product_identity='0113',production_t='12',area_ha='3',reported_yield_kg_ha=None,present_5312=1,present_5510=1,present_5412=1,observation_5312=f'{g}-{y}-a',observation_5510=f'{g}-{y}-p',observation_5412=f'{g}-{y}-y') for g in p['geographies'] for y in (1961,1962)]
        elif name=='price_observations':
            rows=[dict(period=200001+i if i<12 else 200101,price_series_key='wb',value=str(10+i),unit='USD/tonne',observation_key='wb'+str(i)) for i in range(13)] if p['series']=='wb' else [dict(period=d,price_series_key='vn',value='0' if i==0 else None,unit='VND/kg',observation_key='vn'+str(i)) for i,d in enumerate((20210401,20210403))]
        elif name=='sua':rows=[dict(year_key=2010,geography_key=geo('704'),product_key=product('23161.02'),**{k:v for c,m in SUA.items() for k,v in [(m,'-2' if c in ('5071','5166') else None),('present_'+c,1),('observation_'+c,'sua'+c)]})]
        elif name=='psd':rows=[dict(market_year=2024,crop_year='2024/2025',geography_key='usda',product_key='rice',metric='Ending Stocks',unit='tonne',value='23',observation_key='psd')]
        elif name=='trade':rows=[dict(year_key=p['start_year'],reporter_geography_key=geo('704'),partner_geography_key=geo(c),product_key=product('23161.02'),partner_identity=c,partner_labels=json.dumps([{'label':c}]),quantity_t='0' if c=='156' else '3',value_usd=None if c=='458' else '10',present_quantity=1,present_value=1,observation_quantity='q'+c,observation_value='v'+c) for c in ('156','458','608')]
        else:raise AssertionError(name)
        df=pd.DataFrame(rows)
        return self.mutate(name,p,df) if self.mutate else df


def test_all_six_extracts_decimal_null_zero_sign_and_utf8(tmp_path):
    files,lineage,proof=build(Session())
    assert set(files)==set(SCHEMAS)
    assert files['fao_production.csv'][0]['computed_yield_kg_ha']==4000
    assert any(r['value_t']==-2 for r in files['sua_components.csv'])
    assert files['vn_quote.csv'][0]['price_vnd_kg']==0
    assert files['vn_quote.csv'][1]['price_vnd_kg'] is None
    assert files['vn_quote.csv'][1]['days_since_previous_quote']==2
    assert len(files['trade_exports.csv'])==15
    assert any(r['quantity_t']==0 and r['unit_value_usd_t'] is None for r in files['trade_exports.csv'])
    assert len(files['wb_benchmark.csv'])==312
    assert files['wb_benchmark.csv'][13]['observation_present'] is False
    assert files['wb_benchmark.csv'][12]['volatility_12m_pct'] is not None
    for name,rows in files.items():
        write_csv(tmp_path/name,rows,SCHEMAS[name])
        assert csv_read(tmp_path/name,SCHEMAS[name])==rows
    assert 'An Giang' in (tmp_path/'vn_quote.csv').read_text(encoding='utf-8-sig')


@pytest.mark.parametrize('mode',['duplicate','unit','geo','product','multiple_contributors','source_missing','timeout'])
def test_bad_live_results_rejected(mode):
    def mutate(name,p,df):
        if name=='fao_production':
            if mode=='duplicate':return pd.concat([df,df.iloc[:1]])
            if mode=='geo':df.loc[0,'geography_key']='wrong'
            if mode=='product':df.loc[0,'product_identity']='wrong'
            if mode=='multiple_contributors':df.loc[0,'present_5510']=2
            if mode=='timeout':raise TimeoutError('budget')
        if name=='price_observations' and mode=='unit':df.loc[0,'unit']='wrong'
        if name=='catalogue' and p['kind']=='geographies' and mode=='source_missing':return df[df.source_identity!='704']
        return df
    with pytest.raises((ValueError,TimeoutError)):build(Session(mutate))


def test_gap_and_present_null_do_not_generate_returns():
    def mutate(name,p,df):
        if name=='price_observations' and p['series']=='wb':
            df.loc[3,'value']=None
            return df[df.period!=200006]
        return df
    rows=build(Session(mutate))[0]['wb_benchmark.csv']
    assert rows[3]['observation_present'] and not rows[3]['numeric_present']
    assert not rows[5]['observation_present'] and rows[5]['price_usd_t'] is None
    assert rows[6]['monthly_change_pct'] is None
    assert rows[12]['volatility_12m_pct'] is None


@pytest.mark.parametrize('numerator,denominator,expected',[(None,3,None),(0,3,Decimal(0)),(3,0,None),(3,-1,None),(3,None,None)])
def test_denominator_policy(numerator,denominator,expected):assert ratio(numerator,denominator)==expected


def test_v2_bundle_failure_cleans_all_files(tmp_path,monkeypatch):
    from tests.unit.analytics.test_powerbi import Client
    from analytics.powerbi import export_bundle
    from contextlib import nullcontext
    monkeypatch.setattr('analytics.marts.powerbi.build',lambda s:(_ for _ in ()).throw(TimeoutError('report read failed')))
    with pytest.raises(TimeoutError):export_bundle(tmp_path,Client(),fence=lambda p:nullcontext(),report=True)
    assert not list(tmp_path.iterdir())


def test_report_grain_duplicate_and_tampered_ratio_fail():
    rows=build(Session())[0]['trade_exports.csv']
    with pytest.raises(ValueError):check('trade_exports.csv',rows+[rows[0]])
    altered=copy.deepcopy(rows);altered[-1]['unit_value_usd_t']=Decimal(999)
    with pytest.raises(ValueError):check('trade_exports.csv',altered)


@pytest.mark.parametrize('tamper',[None,'definitions','snapshots','contract','compiled_sql'])
def test_complete_v2_bundle_provenance_and_failure_cleanup(tmp_path,tamper):
    from contextlib import nullcontext
    import hashlib
    from tests.unit.analytics.test_powerbi import Client,BATCH,MANIFEST
    from analytics import VERSION
    from analytics.powerbi import export_bundle
    from analytics.queries import compile_query
    from gold.spec import load_spec
    class ReportClient(Client):
        def query(self,name,**params):
            if name in ('publication','fact_overview','nso_scope_catalogue','nso_observations'):
                return super().query(name,**params)
            identities={n:key('fixture_identity',[n]) for n in ('wb','vn','usda','rice')}
            stub_params={k:next((n for n,h in identities.items() if h==v),v) for k,v in params.items()}
            frame=Session().query(name,**stub_params).replace(identities)
            bound={'outputs':{n:{'uuid':'uuid-'+n,'snapshot_id':'123'} for n in load_spec()}}
            sql,digest=compile_query(name,params,bound)
            self.evidence.append(dict(query=name,parameters=params,version=VERSION,cache='DISABLED',batch=BATCH,
                manifest_sha256=MANIFEST,definition_sha256=digest,compiled_sql_sha256=hashlib.sha256(sql.encode()).hexdigest()))
            return frame
    folder,m=export_bundle(tmp_path,ReportClient(),fence=lambda p:nullcontext(),report=True)
    assert len(m['outputs'])==10 and m['performance']['analytics_requests']==28
    assert verify_bundle(folder)['status']=='COMPLETE'
    path=folder/'export_manifest.json'
    if tamper=='definitions':m['sql_definitions']=[]
    if tamper=='snapshots':m['snapshots']['wrong']=m['snapshots'].pop('dim_year')
    if tamper=='contract':m['mart_contracts']['version']='old'
    if tamper=='compiled_sql':m['sql_definitions'][-1]['compiled_sql_sha256']='0'*64
    path.write_text(json.dumps(m),encoding='utf-8')
    if tamper:
        with pytest.raises(ValueError):verify_bundle(folder)
    else:assert verify_bundle(folder)['status']=='COMPLETE'


@pytest.mark.parametrize('arguments,code',[(['--help'],0),(['--unsupported-option'],2)])
def test_mart_cli_parses_before_any_export_or_docker_access(arguments,code):
    import subprocess
    import sys
    from pathlib import Path
    result=subprocess.run([sys.executable,'-m','jobs.analytics.build_powerbi_marts',*arguments],
        cwd=Path(__file__).resolve().parents[3],capture_output=True,text=True,timeout=10)
    assert result.returncode==code
    assert 'usage:' in (result.stdout+result.stderr).lower()
    assert 'Traceback' not in result.stderr and 'RESOURCE_BLOCKED' not in result.stderr
