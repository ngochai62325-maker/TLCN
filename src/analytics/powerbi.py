"""Small deterministic Power BI extracts from one published Gold snapshot set."""
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import uuid

import pandas as pd

from analytics import VERSION
from analytics.client import EDAClient
from analytics.queries import PERIODS, validate
from analytics.powerbi_fence import publication_fence, OPERATIONS

FORMAT_VERSION = 'powerbi-csv-1'
SCHEMAS = {
    'fact_overview.csv': [('fact_name','text'),('rows_total','integer'),('numeric_rows','integer'),('present_null_rows','integer'),
        ('numeric_coverage_pct','decimal?'),('period_min','text?'),('period_max','text?'),('represented_periods','integer'),('period_grain','text')],
    'nso_national_annual.csv': [('year','integer'),('production_t','decimal?'),('area_ha','decimal?'),('publication','text'),
        ('geography_scope','text'),('production_present','boolean'),('area_present','boolean')],
    'nso_national_seasonal.csv': [('year','integer'),('season','text'),('production_t','decimal?'),('publication','text'),
        ('geography_scope','text'),('production_present','boolean'),('season_label_vi','text')],
    'nso_reported_yield.csv': [('year','integer'),('yield_kg_ha','decimal?'),('publication','text'),('yield_present','boolean')],
}
SEASONS = {'winter_spring':'Đông xuân','summer_autumn':'Hè thu','summer_autumn_autumn_winter':'Hè thu và Thu đông','main_season':'Vụ mùa'}
DEFINITION_FIELDS = ('query','parameters','version','definition_sha256','compiled_sql_sha256')


def require(frame, fields):
    if not isinstance(frame, pd.DataFrame) or frame.columns.duplicated().any() or not set(fields) <= set(frame.columns):
        raise ValueError('Analytics output schema drift')
    if len(frame)>5000:
        raise ValueError('Analytics row budget exceeded')
    return frame


def number(value):
    if value is None or (not isinstance(value,str) and pd.isna(value)):
        return None
    if isinstance(value,bool):
        raise ValueError('Boolean is not an observation value')
    try: out=Decimal(str(value))
    except InvalidOperation: raise ValueError('Invalid observation number') from None
    if not out.is_finite():raise ValueError('Non-finite observation')
    return out


def integer(value):
    out=number(value)
    if out is None or out<0 or out!=out.to_integral_value():raise ValueError('Invalid non-negative integer')
    return int(out)


def period_text(value, kind):
    if value is None or pd.isna(value):return None
    raw=str(integer(value))
    fmt = '%Y%m%d' if kind=='date_key' else '%Y%m' if kind=='month_key' else '%Y'
    parsed=datetime.strptime(raw,fmt)
    if parsed.strftime(fmt)!=raw:raise ValueError('Invalid period key')
    return parsed.strftime('%Y-%m-%d' if kind=='date_key' else '%Y-%m' if kind=='month_key' else '%Y')


def sha256(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def scalar(value):
    if value is None:return ''
    if type(value) is bool:return 'true' if value else 'false'
    if isinstance(value,Decimal):return format(value,'f')
    return str(value)


def write_csv(path, rows, schema):
    names=[n for n,_ in schema]
    with path.open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.writer(stream,lineterminator='\n');writer.writerow(names)
        for row in rows:
            if set(row)!=set(names):raise ValueError('Export row schema mismatch')
            writer.writerow([scalar(row[n]) for n in names])


def csv_read(path, schema):
    if not path.read_bytes().startswith(b'\xef\xbb\xbf'):raise ValueError('CSV missing UTF-8 BOM')
    with path.open(encoding='utf-8-sig',newline='') as stream:
        reader=csv.DictReader(stream)
        if reader.fieldnames != [n for n,_ in schema]:raise ValueError('CSV header/schema drift')
        rows=[]
        for raw in reader:
            if None in raw or any(v is None for v in raw.values()):raise ValueError('Malformed CSV row')
            row={}
            for name,kind in schema:
                val=raw[name]
                if val=='':
                    if not kind.endswith('?'):raise ValueError('Unexpected empty required cell')
                    row[name]=None
                elif kind=='integer':
                    if not re.fullmatch(r'0|[1-9][0-9]*',val):raise ValueError('Invalid integer encoding')
                    row[name]=int(val)
                elif kind.startswith('decimal'):
                    if not re.fullmatch(r'-?[0-9]+(?:\.[0-9]+)?',val):raise ValueError('Invalid decimal encoding')
                    row[name]=number(val)
                elif kind=='boolean':
                    if val not in ('true','false'):raise ValueError('Invalid presence encoding')
                    row[name]=val=='true'
                else:row[name]=val
            rows.append(row)
            if len(rows)>5000:raise ValueError('CSV row budget exceeded')
    return rows


def check_rows(name, rows):
    if name=='fact_overview.csv':
        if len(rows)!=9 or {r['fact_name'] for r in rows}!=set(PERIODS):raise ValueError('Missing/duplicate fact inventory')
        for r in rows:
            if r['rows_total']!=r['numeric_rows']+r['present_null_rows']:raise ValueError('Count reconciliation failed')
            expected=(Decimal(r['numeric_rows'])*100/r['rows_total']).quantize(Decimal('.00000001')) if r['rows_total'] else None
            if r['numeric_coverage_pct']!=expected:raise ValueError('Coverage reconciliation failed')
    else:
        keys=[(r['year'],r['season']) if name=='nso_national_seasonal.csv' else r['year'] for r in rows]
        if len(keys)!=len(set(keys)):raise ValueError('Duplicate exported business key')
        if not rows:raise ValueError('Required NSO dataset is empty')
        publication='V06.14.csv' if name=='nso_reported_yield.csv' else 'V06.12.csv'
        for r in rows:
            if r['publication']!=publication or r.get('geography_scope','national')!='national':raise ValueError('Mixed publication/geography')
            for value,presence in [('production_t','production_present'),('area_ha','area_present'),('yield_kg_ha','yield_present')]:
                if presence in r and not r[presence] and r[value] is not None:raise ValueError('Absent cell has a value')
            if name=='nso_national_seasonal.csv' and (r['season'] not in SEASONS or r['season_label_vi']!=SEASONS[r['season']] or not r['production_present']):
                raise ValueError('Invalid seasonal identity')


def verify_bundle(folder):
    folder=Path(folder)
    manifest=json.loads((folder/'export_manifest.json').read_text(encoding='utf-8'))
    if manifest.get('status')!='COMPLETE' or manifest.get('format_version')!=FORMAT_VERSION or set(manifest.get('outputs',{}))!=set(SCHEMAS):
        raise ValueError('Incomplete/unknown export manifest')
    if not re.fullmatch(r'[0-9a-f]{64}',manifest.get('published_batch','')):raise ValueError('Missing batch identity')
    for name,schema in SCHEMAS.items():
        record=manifest['outputs'][name];path=folder/name
        if path.is_symlink() or record['filename']!=name or record['schema']!=[{'name':n,'type':t} for n,t in schema] or sha256(path)!=record['sha256']:
            raise ValueError('CSV checksum/schema mismatch')
        rows=csv_read(path,schema)
        if len(rows)!=record['rows']:raise ValueError('CSV row count mismatch')
        check_rows(name,rows)
    return manifest


class ExportSession:
    def __init__(self, client):self.client=client;self.batch=None;self.manifest_sha=None;self.definitions=[]

    def query(self,name,**parameters):
        if name not in OPERATIONS:raise ValueError('Power BI operation outside allowlist')
        validate(name,parameters)
        frame=self.client.query(name,**parameters)
        e=self.client.evidence[-1]
        if (e['query']!=name or e['parameters']!=parameters or e['version']!=VERSION or e['cache']!='DISABLED'
                or not re.fullmatch(r'[0-9a-f]{64}',e['batch']) or e['batch']!=self.client.batch
                or self.batch is not None and (self.batch!=e['batch'] or self.manifest_sha!=e['manifest_sha256'])):
            raise RuntimeError('Published batch/definition fingerprint changed')
        self.batch=e['batch'];self.manifest_sha=e['manifest_sha256']
        for field in ('manifest_sha256','definition_sha256','compiled_sql_sha256'):
            if not re.fullmatch(r'[0-9a-f]{64}',e[field]):raise ValueError('Invalid analytical provenance hash')
        self.definitions.append({k:e[k] for k in DEFINITION_FIELDS})
        return frame

    def publication(self):
        data=require(self.query('publication'),['table','rows','uuid','snapshot_id'])
        if len(data)!=17 or data.table.duplicated().any() or set(data[data.table.str.startswith('fact_')].table)!=set(PERIODS):raise ValueError('Invalid physical publication inventory')
        info=self.client.evidence[-1]['publication_info']
        if info['status']!='PUBLISHED' or info['dq']!='ACCEPT' or info['source_count']!=9 or not info['model']:raise ValueError('Unpublished/incomplete Gold')
        pins={}
        for r in data.to_dict('records'):
            if not r['uuid'] or not 0<integer(r['snapshot_id'])<2**63:raise ValueError('Missing physical snapshot evidence')
            pins[r['table']]={'uuid':r['uuid'],'snapshot_id':str(r['snapshot_id'])}
        return info,pins,{r['table']:integer(r['rows']) for r in data.to_dict('records')}


def observations(session, table, measures):
    domain=require(session.query('nso_scope_catalogue',source_table=table,level='national'),
        ['source_table','source_level','geography_key','source_geography_key','measure','statistic_kind','season','unit','first_year','last_year'])
    eligible=domain[domain.statistic_kind.eq('quantity') & domain.measure.isin(measures)]
    if eligible.empty or not eligible.source_table.eq(table).all() or not eligible.source_level.eq('national').all():raise ValueError('Unavailable NSO scope')
    if not measures <= set(eligible.loc[eligible.season.eq('annual'),'measure']):raise ValueError('Required annual NSO measure unavailable')
    identities=eligible[['geography_key','source_geography_key']].drop_duplicates()
    if len(identities)!=1:raise ValueError('Ambiguous NSO national source identity')
    geo=identities.iloc[0]
    if not re.fullmatch(r'[0-9a-f]{64}',geo.geography_key):raise ValueError('Invalid source geography key')
    start,end=int(eligible.first_year.min()),int(eligible.last_year.max())
    params=dict(source_table=table,level='national',geography_key=geo.geography_key,start_year=start,end_year=end)
    data=require(session.query('nso_observations',**params),['observation_key','year_key','source_table','source_level','geography_key','source_geography_key',
        'measure','statistic_kind','season','value','unit','release_status','quality_status'])
    data['year_key']=data.year_key.map(integer)
    for column,value in [('source_table',table),('source_level','national'),('geography_key',geo.geography_key),('source_geography_key',geo.source_geography_key)]:
        if not data[column].eq(value).all():raise ValueError('NSO query mixed source identities')
    if (data.observation_key.isna().any() or not data.observation_key.map(lambda v:isinstance(v,str) and bool(v)).all()
            or data.observation_key.duplicated().any() or not data.year_key.between(start,end).all()):raise ValueError('Invalid NSO observation identity/time')
    data=data[data.statistic_kind.eq('quantity') & data.measure.isin(measures)].copy()
    for r in data.itertuples():
        if r.unit!={'production':'tonne','area':'hectare','yield':'kg/hectare'}[r.measure]:raise ValueError('NSO unit drift/double conversion')
    if data.duplicated(['year_key','measure','season']).any():raise ValueError('NSO grain collision; no MAX/FIRST repair')
    data['exact_value']=data.value.map(number)
    return data,dict(params,source_geography_key=geo.source_geography_key,statistic_kind='quantity')


def nso_exports(data,yields):
    annual=data[data.season.eq('annual')];seasonal=data[data.measure.eq('production') & data.season.ne('annual')]
    yr=yields[yields.season.eq('annual') & yields.measure.eq('yield')]
    files={};lineage={};expected={}
    rows=[];orig={}
    for year,part in annual.groupby('year_key',sort=True):
        cell={r.measure:r for r in part.itertuples()};row=dict(year=int(year),publication='V06.12.csv',geography_scope='national')
        for measure,target in [('production','production_t'),('area','area_ha')]:
            present=measure in cell;row[target]=cell[measure].exact_value if present else None;row[measure+'_present']=present
            if present:orig[(int(year),measure)]=cell[measure].observation_key
        rows.append(row)
    files['nso_national_annual.csv']=rows;lineage['nso_national_annual.csv']=[{'year':y,'measure':m,'observation_key':key} for (y,m),key in orig.items()]
    files['nso_national_seasonal.csv']=[dict(year=int(r.year_key),season=r.season,production_t=r.exact_value,publication='V06.12.csv',
        geography_scope='national',production_present=True,season_label_vi=SEASONS[r.season]) for r in seasonal.sort_values(['year_key','season']).itertuples()]
    lineage['nso_national_seasonal.csv']=[dict(year=int(r.year_key),season=r.season,observation_key=r.observation_key) for r in seasonal.sort_values(['year_key','season']).itertuples()]
    files['nso_reported_yield.csv']=[dict(year=int(r.year_key),yield_kg_ha=r.exact_value,publication='V06.14.csv',yield_present=True) for r in yr.sort_values('year_key').itertuples()]
    lineage['nso_reported_yield.csv']=[dict(year=int(r.year_key),observation_key=r.observation_key) for r in yr.sort_values('year_key').itertuples()]
    # Exact value/NULL/multiplicity reconciliation, not checksums alone.
    expected['annual']={(int(r.year_key),r.measure):r.exact_value for r in annual.itertuples()}
    actual={(r['year'],m):r[col] for r in rows for m,col in [('production','production_t'),('area','area_ha')] if r[m+'_present']}
    if actual!=expected['annual'] or len(actual)!=len(annual):raise ValueError('Annual exact cell reconciliation failed')
    expected['seasonal']={(int(r.year_key),r.season):r.exact_value for r in seasonal.itertuples()}
    if {(r['year'],r['season']):r['production_t'] for r in files['nso_national_seasonal.csv']}!=expected['seasonal']:raise ValueError('Seasonal reconciliation failed')
    if {r['year']:r['yield_kg_ha'] for r in files['nso_reported_yield.csv']}!={int(r.year_key):r.exact_value for r in yr.itertuples()}:raise ValueError('Reported yield reconciliation failed')
    for name,records in files.items():check_rows(name,records)
    return files,lineage,dict(status='PASS',method='exact keyed cell/value/NULL/presence comparison + unique source observation keys',
        annual_cells=len(annual),seasonal_cells=len(seasonal),reported_yield_cells=len(yr),
        deliberately_excluded_quantity_area_seasonal_rows=len(data)-len(annual)-len(seasonal),
        release_status_counts={table:frame.release_status.value_counts(dropna=False).to_dict() for table,frame in [('V06.12.csv',data),('V06.14.csv',yields)]})


def cleanup_owned(folder,root):
    """Only our known flat files, never recursive deletion of a computed path."""
    if folder is None or not folder.exists():return
    if folder.resolve().parent!=root.resolve() or folder.is_symlink():raise RuntimeError('Unsafe export cleanup target')
    for name in ['export_manifest.json',*SCHEMAS]:
        path=folder/name
        if path.exists() and path.is_file() and not path.is_symlink():path.unlink()
    folder.rmdir()


def export_bundle(root,client=None,fence=publication_fence):
    root=Path(root).resolve();root.mkdir(parents=True,exist_ok=True)
    # Normal inherited workspace ACLs, including on Windows. mkdtemp(mode=700)
    # would make the promoted report folder inaccessible to restricted readers.
    stage=root/('.powerbi-'+uuid.uuid4().hex);stage.mkdir();promoted=None
    try:
        session=ExportSession(client if client is not None else EDAClient())
        info,pins,published_rows=session.publication();overview=[]
        for fact,kind in sorted(PERIODS.items()):
            frame=require(session.query('fact_overview',fact=fact),['rows','numeric_observations','missing_observations','first_period','last_period','represented_periods'])
            if len(frame)!=1:raise ValueError('Expected full-data singleton aggregate')
            r=frame.iloc[0];total,numeric,missing=map(integer,[r['rows'],r.numeric_observations,r.missing_observations])
            if total!=published_rows[fact]:raise ValueError('Fact/manifest row reconciliation failed')
            overview.append(dict(fact_name=fact,rows_total=total,numeric_rows=numeric,present_null_rows=missing,
                numeric_coverage_pct=(Decimal(numeric)*100/total).quantize(Decimal('.00000001')) if total else None,
                period_min=period_text(r.first_period,kind),period_max=period_text(r.last_period,kind),represented_periods=integer(r.represented_periods),
                period_grain={'market_year':'marketing_year','reference_year':'reference_year','month_key':'calendar_month','date_key':'calendar_date','year_key':'calendar_year'}[kind]))
        check_rows('fact_overview.csv',overview)
        data,scope=observations(session,'V06.12.csv',{'production','area'})
        yields,yield_scope=observations(session,'V06.14.csv',{'yield'})
        files,lineage,reconciliation=nso_exports(data,yields);files['fact_overview.csv']=overview
        outputs={}
        for name,schema in SCHEMAS.items():
            write_csv(stage/name,files[name],schema)
            if csv_read(stage/name,schema)!=files[name]:raise ValueError('CSV exact roundtrip reconciliation failed')
            outputs[name]=dict(filename=name,schema=[{'name':n,'type':t} for n,t in schema],rows=len(files[name]),sha256=sha256(stage/name),encoding='utf-8-sig')
        end_info,end_pins,end_rows=session.publication()
        if (info,pins,published_rows)!=(end_info,end_pins,end_rows):raise RuntimeError('Publication changed during export')
        payload=dict(batch=session.batch,manifest_sha256=session.manifest_sha,snapshots=pins,definitions=session.definitions)
        export_id=uuid.uuid4().hex
        final=root/(session.batch[:12]+'_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+export_id[:8])
        if final.exists() or final.parent!=root:raise RuntimeError('Unsafe/non-unique export destination')
        with fence(payload):
            manifest=dict(status='COMPLETE',format_version=FORMAT_VERSION,export_id=export_id,
                export_timestamp=datetime.now(timezone.utc).isoformat(),published_batch=session.batch,gold_model=info['model'],
                publication_manifest_sha256=session.manifest_sha,source_namespace='iceberg.gold_local',snapshots=pins,
                sql_definitions=session.definitions,typed_scope={'annual_and_seasonal':scope,'reported_yield':yield_scope},outputs=outputs,
                source_observation_lineage=lineage,reconciliation=dict(reconciliation,overview_rows=9,
                    fact_observations=sum(r['rows_total'] for r in overview),numeric_observations=sum(r['numeric_rows'] for r in overview),
                    present_null_observations=sum(r['present_null_rows'] for r in overview)),
                semantics={'presence':'true + blank numeric cell = present-NULL; false + blank = absent observation; 0 stays numeric zero',
                    'scope':'source-scoped national; no official geography conversion; quantity only; annual and seasonal kept separate',
                    'coverage_pct':'numeric_rows / rows_total * 100, rounded to eight decimal places; undefined when rows_total=0',
                    'publication_fence':'Gold/source shared locks held during atomic directory promotion; manifest verified again before release'})
            (stage/'export_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
            verify_bundle(stage)
            os.rename(stage,final);promoted=final
        verify_bundle(final)
        return final,manifest
    except BaseException:
        cleanup_owned(promoted,root);cleanup_owned(stage,root)
        raise
