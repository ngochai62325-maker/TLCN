from contextlib import contextmanager,nullcontext
from decimal import Decimal
import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from analytics import VERSION
from analytics.powerbi import (export_bundle,verify_bundle,csv_read,SCHEMAS,number,ExportSession)
from analytics.queries import PERIODS,compile_query
from gold.spec import load_spec

BATCH='a'*64
MANIFEST='b'*64
GEO12='c'*64
GEO14='d'*64


def source_rows(table):
    rows=[]
    def add(year,measure,season,value,unit):
        rows.append(dict(observation_key=f'{table}-{year}-{measure}-{season}',year_key=year,measure=measure,season=season,value=value,unit=unit,
            source_table=table,source_level='national',geography_key=GEO12 if table=='V06.12.csv' else GEO14,
            source_geography_key='__national_table_scope',statistic_kind='quantity',release_status='provisional',quality_status='warn'))
    if table=='V06.12.csv':
        for year,p,a in [(2020,'123456789.12345678','5'),(2021,None,'6'),(2022,'0',None)]:
            add(year,'production','annual',p,'tonne')
            if year!=2022:add(year,'area','annual',a,'hectare')
            add(year,'production','winter_spring',p,'tonne')
            add(year,'area','winter_spring',a,'hectare')
    else:
        for year,y in [(2020,'3690'),(2021,None),(2022,'6100')]:add(year,'yield','annual',y,'kg/hectare')
    return pd.DataFrame(rows)


class Client:
    def __init__(self, mutate=None):self.batch=None;self.evidence=[];self.mutate=mutate;self.calls=[]
    def query(self,name,**params):
        self.calls.append((name,params));self.batch=BATCH
        m={'outputs':{n:{'uuid':'uuid-'+n,'snapshot_id':'123'} for n in load_spec()}}
        sql,definition=compile_query(name,params,m)
        self.evidence.append(dict(query=name,parameters=params,version=VERSION,cache='DISABLED',batch=BATCH,manifest_sha256=MANIFEST,
            definition_sha256=definition,compiled_sql_sha256=__import__('hashlib').sha256((sql or '').encode()).hexdigest(),
            publication_info=dict(status='PUBLISHED',dq='ACCEPT',model='test-model',source_count=9)))
        if name=='publication':
            frame=pd.DataFrame([dict(table=n,uuid=v['uuid'],snapshot_id=v['snapshot_id'],rows=3) for n,v in m['outputs'].items()])
        elif name=='fact_overview':
            fact=params['fact'];a,b=(202001,202003) if PERIODS[fact]=='month_key' else (20200101,20200103) if PERIODS[fact]=='date_key' else (2020,2022)
            frame=pd.DataFrame([dict(rows=3,numeric_observations=2,missing_observations=1,first_period=a,last_period=b,represented_periods=3)])
        elif name=='nso_scope_catalogue':
            table=params['source_table'];df=source_rows(table)
            frame=df.drop_duplicates(['measure','season']).drop(columns=['year_key','value','observation_key']).assign(first_year=2020,last_year=2022)
        elif name=='nso_observations':frame=source_rows(params['source_table'])
        else:raise AssertionError(name)
        return self.mutate(self,name,params,frame) if self.mutate else frame


def test_complete_export_exact_cells_utf8_and_manifest(tmp_path):
    folder,m=export_bundle(tmp_path,Client(),fence=lambda p:nullcontext())
    assert folder.parent==tmp_path and verify_bundle(folder)['status']=='COMPLETE'
    assert m['published_batch']==BATCH and len(m['snapshots'])==17
    assert m['reconciliation']['fact_observations']==27
    assert {n:v['rows'] for n,v in m['outputs'].items()}=={'fact_overview.csv':9,'nso_national_annual.csv':3,'nso_national_seasonal.csv':3,'nso_reported_yield.csv':3}
    annual=csv_read(folder/'nso_national_annual.csv',SCHEMAS['nso_national_annual.csv'])
    assert annual[0]['production_t']==Decimal('123456789.12345678')
    assert annual[1]['production_t'] is None and annual[1]['production_present'] is True
    assert annual[2]['production_t']==0 and annual[2]['area_ha'] is None and annual[2]['area_present'] is False
    seasonal=(folder/'nso_national_seasonal.csv').read_text(encoding='utf-8-sig')
    assert 'Đông xuân' in seasonal and 'annual' not in seasonal and '123456789.12345678' in seasonal
    assert all((folder/n).read_bytes().startswith(b'\xef\xbb\xbf') for n in SCHEMAS)
    assert m['typed_scope']['annual_and_seasonal']['source_table']=='V06.12.csv'
    assert m['typed_scope']['reported_yield']['source_table']=='V06.14.csv'


@pytest.mark.parametrize('mode',['schema','count','batch','manifest','model','unpublished','snapshot','version','query_failure','resource','unit','duplicate','geography','publication','fractional_year'])
def test_rejected_inputs_leave_no_successful_manifest_or_partial_bundle(tmp_path,mode):
    def mutate(c,name,p,df):
        if name=='fact_overview':
            if mode=='schema':return df.rename(columns={'rows':'wrong'})
            if mode=='count':df.loc[0,'numeric_observations']=9
            if mode=='batch':c.batch='e'*64;c.evidence[-1]['batch']=c.batch
            if mode=='manifest':c.evidence[-1]['manifest_sha256']='e'*64
            if mode=='version':c.evidence[-1]['version']='tampered'
            if mode=='query_failure':raise TimeoutError('Trino query timed out')
            if mode=='resource':raise RuntimeError('RESOURCE_BLOCKED')
        if name=='publication':
            if mode=='unpublished':c.evidence[-1]['publication_info']['status']='FAILED'
            if mode=='model':c.evidence[-1]['publication_info']['model']=None
            if mode=='snapshot':df.loc[0,'snapshot_id']='0'
        if name=='nso_observations':
            if mode=='unit':df.loc[0,'unit']='1000 tonnes'
            if mode=='duplicate':return pd.concat([df,df.iloc[:1]])
            if mode=='geography':df.loc[0,'geography_key']='e'*64
            if mode=='publication':df.loc[0,'source_table']='V06.15.csv'
            if mode=='fractional_year':df['year_key']=df.year_key.astype(float);df.loc[0,'year_key']=2020.5
        return df
    with pytest.raises(Exception):export_bundle(tmp_path,Client(mutate),fence=lambda p:nullcontext())
    assert list(tmp_path.iterdir())==[]


def test_missing_and_ambiguous_national_domain_fail_closed(tmp_path):
    for ambiguous in [False,True]:
        def mutate(c,n,p,df):
            if n=='nso_scope_catalogue':
                if not ambiguous:return df.iloc[:0]
                extra=df.iloc[:1].copy();extra['geography_key']='f'*64
                return pd.concat([df,extra])
            return df
        with pytest.raises(ValueError):export_bundle(tmp_path,Client(mutate),fence=lambda p:nullcontext())
        assert not list(tmp_path.iterdir())


def test_exception_after_promotion_removes_candidate_but_preserves_old_export(tmp_path):
    previous,_=export_bundle(tmp_path,Client(),fence=lambda p:nullcontext())
    original=(previous/'export_manifest.json').read_bytes()
    @contextmanager
    def lost_response(payload):
        yield
        raise RuntimeError('Guard response lost/publication changed')
    with pytest.raises(RuntimeError):export_bundle(tmp_path,Client(),fence=lost_response)
    assert list(tmp_path.iterdir())==[previous] and (previous/'export_manifest.json').read_bytes()==original


def test_write_failure_and_final_publication_change_do_not_publish(tmp_path):
    import analytics.powerbi as p
    with patch.object(p,'write_csv',side_effect=OSError('disk full')):
        with pytest.raises(OSError):export_bundle(tmp_path,Client(),fence=lambda p:nullcontext())
    assert not list(tmp_path.iterdir())
    def changed(c,n,p,df):
        if n=='publication' and len(c.calls)>1:c.evidence[-1]['manifest_sha256']='e'*64
        return df
    with pytest.raises(RuntimeError):export_bundle(tmp_path,Client(changed),fence=lambda p:nullcontext())
    assert not list(tmp_path.iterdir())


def test_failure_after_one_csv_written_has_no_success_manifest(tmp_path):
    import analytics.powerbi as p
    original=p.write_csv;counter=[0]
    def fail_second(path,rows,schema):
        counter[0]+=1
        if counter[0]==2:raise OSError('disk full after first CSV')
        original(path,rows,schema)
    with patch.object(p,'write_csv',fail_second):
        with pytest.raises(OSError):export_bundle(tmp_path,Client(),fence=lambda p:nullcontext())
    assert not list(tmp_path.iterdir())


def test_missing_required_production_field_is_a_blocker(tmp_path):
    def mutate(c,n,p,df):
        if n=='nso_scope_catalogue' and p['source_table']=='V06.12.csv':return df[df.measure.eq('area')]
        return df
    with pytest.raises(ValueError,match='measure unavailable'):export_bundle(tmp_path,Client(mutate),fence=lambda p:nullcontext())
    assert not list(tmp_path.iterdir())


def test_host_fence_requires_ready_and_post_promotion_ack():
    from analytics.powerbi_fence import publication_fence
    from unittest.mock import Mock
    payload=dict(batch=BATCH,manifest_sha256=MANIFEST,snapshots={},definitions=[])
    def process():
        proc=Mock();proc.poll.return_value=None;proc.wait.return_value=0;return proc
    p=process()
    ready=dict(status='LOCKED',batch=BATCH,manifest_sha256=MANIFEST)
    accepted=dict(ready,status='VERIFIED')
    with patch('analytics.powerbi_fence.windows_available_bytes',return_value=3*1024**3),patch('analytics.powerbi_fence.subprocess.Popen',return_value=p),patch('analytics.powerbi_fence.read_message',side_effect=[ready,accepted]):
        with publication_fence(payload):pass
    assert p.stdin.write.call_args_list[-1].args[0]=='FINISH\n'
    p=process()
    with patch('analytics.powerbi_fence.windows_available_bytes',return_value=3*1024**3),patch('analytics.powerbi_fence.subprocess.Popen',return_value=p),patch('analytics.powerbi_fence.read_message',side_effect=[ready,dict(accepted,batch='e'*64)]):
        with pytest.raises(RuntimeError,match='readback'):
            with publication_fence(payload):pass
    p.kill.assert_called_once()


def test_host_fence_resource_failure_never_starts_subprocess():
    from analytics.powerbi_fence import publication_fence
    with patch('analytics.powerbi_fence.windows_available_bytes',return_value=1024),patch('analytics.powerbi_fence.subprocess.Popen') as launch:
        with pytest.raises(RuntimeError,match='RESOURCE_BLOCKED'):
            with publication_fence({}):pass
        launch.assert_not_called()


@pytest.mark.parametrize('line,accepted',[('FINISH\n',True),('FINISH\r\n',True),('',False),('FINISH',False),('FINISH SQL\n',False)])
def test_windows_linux_guard_newline_protocol(line,accepted):
    from analytics.powerbi_fence import valid_finish
    assert valid_finish(line) is accepted


@pytest.mark.parametrize('tamper',['checksum','schema','row_count','partial'])
def test_offline_verifier_rejects_tampering(tmp_path,tamper):
    folder,_=export_bundle(tmp_path,Client(),fence=lambda p:nullcontext())
    path=folder/'export_manifest.json';m=json.loads(path.read_text(encoding='utf-8'))
    if tamper=='checksum':(folder/'nso_national_annual.csv').write_text('wrong',encoding='utf-8')
    if tamper=='schema':m['outputs']['nso_national_annual.csv']['schema'][0]['name']='renamed'
    if tamper=='row_count':m['outputs']['nso_national_annual.csv']['rows']=7
    if tamper=='partial':m['status']='PENDING'
    path.write_text(json.dumps(m),encoding='utf-8')
    with pytest.raises(ValueError):verify_bundle(folder)


def test_query_allowlist_invalid_filters_fail_before_client_call():
    client=Client();session=ExportSession(client)
    for name,params in [('SELECT 1',{}),('trade',{}),('nso_observations',dict(source_table="x'; DROP",level='national',start_year=2020,end_year=2022))]:
        with pytest.raises(ValueError):session.query(name,**params)
    assert not client.calls


def test_guard_refuses_tampered_definition_and_changed_pins():
    from analytics.powerbi_fence import check_manifest
    from gold.keys import fingerprint
    m={'outputs':{n:{'uuid':'uuid-'+n,'snapshot_id':'123'} for n in load_spec()},'context':{'code_fingerprint':'code'},'batch':BATCH}
    sql,h=compile_query('fact_overview',{'fact':'fact_nso_observation'},m)
    payload=dict(batch=BATCH,manifest_sha256=fingerprint(m),snapshots=m['outputs'],definitions=[dict(query='fact_overview',parameters={'fact':'fact_nso_observation'},
        version=VERSION,definition_sha256=h,compiled_sql_sha256=__import__('hashlib').sha256(sql.encode()).hexdigest())])
    with patch('gold.admission.code_fingerprint',return_value='code'):
        check_manifest(m,payload)
        payload['definitions'][0]['definition_sha256']='0'*64
        with pytest.raises(RuntimeError,match='definition'):check_manifest(m,payload)
        payload['snapshots']={}
        with pytest.raises(RuntimeError,match='snapshot'):check_manifest(m,payload)


def test_no_active_streamlit_or_plotly_dependencies_and_eda_preserved():
    assert not Path('dashboard').exists() and not Path('tests/unit/dashboard').exists()
    assert not Path('requirements/dashboard.txt').exists() and not Path('scripts/validate_dashboard.py').exists()
    assert len(list(Path('notebooks/analytics').glob('*.ipynb')))==6
    for root in ['src/analytics','jobs/analytics','requirements']:
        for path in Path(root).rglob('*'):
            if path.suffix in ('.py','.txt'):assert 'import streamlit' not in path.read_text(encoding='utf-8').lower()
