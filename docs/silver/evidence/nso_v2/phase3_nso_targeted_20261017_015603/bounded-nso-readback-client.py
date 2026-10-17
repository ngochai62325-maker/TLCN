CFG={'job': 'nso-recovery-readonly-20261017T021730Z', 'tables': [{'dataset': 'nso_vietnam_v2', 'target': 'iceberg.silver_local.nso_rice_statistics_v2', 'identity': {'uuid': '585ec4d4-d44b-45ad-b6ff-dc55548ebdaf', 'snapshot_id': '833301860238618812', 'schema_id': 0, 'metadata_location': 's3://warehouse/silver_local/nso_rice_statistics_v2/metadata/00001-d570a8a9-4b88-4d18-8ca4-7f84de7b5b9e.metadata.json', 'location': 's3://warehouse/silver_local/nso_rice_statistics_v2', 'schema': {'type': 'struct', 'fields': [{'id': 1, 'name': 'bronze_read_snapshot_id', 'type': 'long', 'required': False}, {'id': 2, 'name': 'source_table', 'type': 'string', 'required': False}, {'id': 3, 'name': 'source_column', 'type': 'string', 'required': False}, {'id': 4, 'name': 'source_geography_key', 'type': 'string', 'required': False}, {'id': 5, 'name': 'geography_raw', 'type': 'string', 'required': False}, {'id': 6, 'name': 'geography_name_candidate', 'type': 'string', 'required': False}, {'id': 7, 'name': 'geography_level', 'type': 'string', 'required': False}, {'id': 8, 'name': 'geography_status', 'type': 'string', 'required': False}, {'id': 9, 'name': 'geography_code', 'type': 'string', 'required': False}, {'id': 10, 'name': 'boundary_version', 'type': 'string', 'required': False}, {'id': 11, 'name': 'geography_mapping_version', 'type': 'string', 'required': False}, {'id': 12, 'name': 'geography_evidence', 'type': 'string', 'required': False}, {'id': 13, 'name': 'period_raw', 'type': 'string', 'required': False}, {'id': 14, 'name': 'year', 'type': 'int', 'required': False}, {'id': 15, 'name': 'measure', 'type': 'string', 'required': False}, {'id': 16, 'name': 'season', 'type': 'string', 'required': False}, {'id': 17, 'name': 'statistic_kind', 'type': 'string', 'required': False}, {'id': 18, 'name': 'release_status', 'type': 'string', 'required': False}, {'id': 19, 'name': 'value_raw', 'type': 'string', 'required': False}, {'id': 20, 'name': 'value_source', 'type': 'decimal(28, 8)', 'required': False}, {'id': 21, 'name': 'unit_raw', 'type': 'string', 'required': False}, {'id': 22, 'name': 'unit_source_reference', 'type': 'string', 'required': False}, {'id': 23, 'name': 'conversion_factor', 'type': 'decimal(8, 0)', 'required': False}, {'id': 24, 'name': 'value', 'type': 'decimal(28, 8)', 'required': False}, {'id': 25, 'name': 'unit', 'type': 'string', 'required': False}, {'id': 26, 'name': 'unit_status', 'type': 'string', 'required': False}, {'id': 27, 'name': 'unit_reference', 'type': 'string', 'required': False}, {'id': 28, 'name': 'unit_evidence', 'type': 'string', 'required': False}, {'id': 29, 'name': 'unit_mapping_version', 'type': 'string', 'required': False}, {'id': 30, 'name': 'metadata_version', 'type': 'string', 'required': False}, {'id': 31, 'name': 'quality_status', 'type': 'string', 'required': False}, {'id': 32, 'name': 'missing_reason', 'type': 'string', 'required': False}, {'id': 33, 'name': 'review_reason', 'type': 'string', 'required': False}, {'id': 34, 'name': 'diagnostic_evidence', 'type': 'string', 'required': False}, {'id': 35, 'name': '_ingestion_run_id', 'type': 'string', 'required': False}, {'id': 36, 'name': '_ingestion_batch_id', 'type': 'string', 'required': False}, {'id': 37, 'name': '_ingestion_chunk_id', 'type': 'long', 'required': False}, {'id': 38, 'name': '_ingestion_timestamp', 'type': 'timestamptz', 'required': False}, {'id': 39, 'name': '_source_id', 'type': 'string', 'required': False}, {'id': 40, 'name': '_source_file', 'type': 'string', 'required': False}, {'id': 41, 'name': '_source_checksum', 'type': 'string', 'required': False}, {'id': 42, 'name': '_source_snapshot_id', 'type': 'long', 'required': False}, {'id': 43, 'name': '_dq_warnings', 'type': {'type': 'list', 'element-id': 45, 'element': {'type': 'struct', 'fields': [{'id': 46, 'name': 'rule_id', 'type': 'string', 'required': False}, {'id': 47, 'name': 'error_message', 'type': 'string', 'required': False}, {'id': 48, 'name': 'failed_column', 'type': 'string', 'required': False}]}, 'element-required': False}, 'required': False}, {'id': 44, 'name': '_silver_processed_at', 'type': 'timestamptz', 'required': False}], 'schema-id': 0, 'identifier-field-ids': []}, 'snapshots': [{'snapshot_id': '6224708582587211745', 'summary': {'spark.app.id': 'local-1792166958709', 'changed-partition-count': '0', 'total-records': '0', 'total-files-size': '0', 'total-data-files': '0', 'total-delete-files': '0', 'total-position-deletes': '0', 'total-equality-deletes': '0', 'engine-version': '3.5.5', 'app-id': 'local-1792166958709', 'engine-name': 'spark', 'iceberg-version': 'Apache Iceberg 1.8.1 (commit 9ce0fcf0af7becf25ad9fc996c3bad2afdcfd33d)'}}, {'snapshot_id': '833301860238618812', 'summary': {'spark.app.id': 'local-1792203238096', 'added-data-files': '35', 'added-records': '23060', 'added-files-size': '1272629', 'changed-partition-count': '35', 'total-records': '23060', 'total-files-size': '1272629', 'total-data-files': '35', 'total-delete-files': '0', 'total-position-deletes': '0', 'total-equality-deletes': '0', 'engine-version': '3.5.5', 'app-id': 'local-1792203238096', 'engine-name': 'spark', 'iceberg-version': 'Apache Iceberg 1.8.1 (commit 9ce0fcf0af7becf25ad9fc996c3bad2afdcfd33d)'}}]}, 'rows': 23060, 'keys': ['source_table', 'source_geography_key', 'year', 'measure', 'season', 'statistic_kind']}, {'dataset': 'dead_letters', 'target': 'iceberg.silver_local.dead_letters', 'identity': {'uuid': '7711cdc0-135f-491c-a153-09c5ff6c1689', 'snapshot_id': '3806092623472970273', 'schema_id': 0, 'metadata_location': 's3://warehouse/silver_local/dead_letters/metadata/00002-febb5d42-a466-43ba-9c82-acbe9f7dc02a.metadata.json', 'location': 's3://warehouse/silver_local/dead_letters', 'schema': {'type': 'struct', 'fields': [{'id': 1, 'name': 'quarantine_record_id', 'type': 'string', 'required': False}, {'id': 2, 'name': 'source_dataset', 'type': 'string', 'required': False}, {'id': 3, 'name': 'pipeline_run_id', 'type': 'string', 'required': False}, {'id': 4, 'name': 'detected_at', 'type': 'timestamptz', 'required': False}, {'id': 5, 'name': 'business_key_hash', 'type': 'string', 'required': False}, {'id': 6, 'name': 'errors', 'type': {'type': 'list', 'element-id': 8, 'element': {'type': 'struct', 'fields': [{'id': 9, 'name': 'rule_id', 'type': 'string', 'required': False}, {'id': 10, 'name': 'error_message', 'type': 'string', 'required': False}, {'id': 11, 'name': 'failed_column', 'type': 'string', 'required': False}]}, 'element-required': False}, 'required': False}, {'id': 7, 'name': 'payload', 'type': 'string', 'required': False}], 'schema-id': 0, 'identifier-field-ids': []}, 'snapshots': [{'snapshot_id': '7512195715552547853', 'summary': {'spark.app.id': 'local-1792166920802', 'added-data-files': '1', 'added-records': '4', 'added-files-size': '9197', 'changed-partition-count': '1', 'total-records': '4', 'total-files-size': '9197', 'total-data-files': '1', 'total-delete-files': '0', 'total-position-deletes': '0', 'total-equality-deletes': '0', 'engine-version': '3.5.5', 'app-id': 'local-1792166920802', 'engine-name': 'spark', 'iceberg-version': 'Apache Iceberg 1.8.1 (commit 9ce0fcf0af7becf25ad9fc996c3bad2afdcfd33d)'}}, {'snapshot_id': '3806092623472970273', 'summary': {'spark.app.id': 'local-1792166958709', 'added-data-files': '1', 'added-records': '13', 'added-files-size': '7530', 'changed-partition-count': '1', 'total-records': '17', 'total-files-size': '16727', 'total-data-files': '2', 'total-delete-files': '0', 'total-position-deletes': '0', 'total-equality-deletes': '0', 'engine-version': '3.5.5', 'app-id': 'local-1792166958709', 'engine-name': 'spark', 'iceberg-version': 'Apache Iceberg 1.8.1 (commit 9ce0fcf0af7becf25ad9fc996c3bad2afdcfd33d)'}}]}, 'rows': 17, 'keys': ['source_dataset', 'quarantine_record_id']}]}
"""One-off read-only client. Configuration is injected by the host, without secrets."""
import os,json,time,urllib.request,hashlib,sqlite3,re,sys
from pathlib import Path
from datetime import datetime,date,timezone
from decimal import Decimal,localcontext
from collections import defaultdict,Counter
import pyarrow as pa
from pyiceberg.expressions import EqualTo,AlwaysTrue
from ingestion.storage.bronze_writer import BronzeIcebergWriter
pa.set_cpu_count(1);pa.set_io_thread_count(1)
ROOT=Path('/tmp')/CFG['job'];ROOT.mkdir(exist_ok=False)
STOP=Path('/tmp')/(CFG['job']+'.stop')
URL='http://trino-test:8080';BATCH=1024;MAX_PAGE=8*1024**2
SESSION={'task_concurrency':'1','iceberg.parquet_max_read_block_size':'1MB','iceberg.parquet_max_read_block_row_count':'512','iceberg.parquet_small_file_threshold':'0B'}
HEADERS={'X-Trino-Client-Capabilities':'PARAMETRIC_DATETIME','X-Trino-User':'phase2-readonly','Content-Type':'text/plain','X-Trino-Session':','.join(k+'='+v for k,v in SESSION.items())}
LAST_NEXT=None

def event(kind,**kw):
 d={'at':datetime.now(timezone.utc).isoformat(),'kind':kind,**kw}
 print('EVENT='+json.dumps(d,ensure_ascii=False,default=str),flush=True)
 with (ROOT/'events.jsonl').open('a',encoding='utf-8') as f:f.write(json.dumps(d,ensure_ascii=False,default=str)+'\n')

def check():
 if STOP.exists():
  if LAST_NEXT:
   try:urllib.request.urlopen(urllib.request.Request(LAST_NEXT,method='DELETE',headers=HEADERS),timeout=5).close()
   except Exception:pass
  raise RuntimeError('RESOURCE_GUARD_STOP_NO_NEW_QUERY')

def stream(sql):
 global LAST_NEXT
 check();event('query_start',sql=sql,session=SESSION)
 req=urllib.request.Request(URL+'/v1/statement',data=sql.encode('utf-8'),headers=HEADERS)
 columns=[];qid=None;rows=pages=maximum=page_rows=0;deadline=time.monotonic()+300;stats={}
 try:
  while True:
   check()
   if time.monotonic()>deadline:raise TimeoutError('READ_ONLY_QUERY_TIMEOUT')
   with urllib.request.urlopen(req,timeout=30) as response:raw=response.read(MAX_PAGE+1)
   assert len(raw)<=MAX_PAGE,'HTTP_PAGE_EXCEEDED_BOUND'
   page=json.loads(raw,parse_float=Decimal);qid=page.get('id',qid);pages+=1;maximum=max(maximum,len(raw));stats=page.get('stats',stats)
   if 'error' in page:raise RuntimeError('TRINO_QUERY_ERROR '+json.dumps(page['error']))
   columns=page.get('columns',columns);data=page.get('data',[]);page_rows=max(page_rows,len(data));rows+=len(data)
   LAST_NEXT=page.get('nextUri')
   for offset in range(0,len(data),BATCH):yield columns,data[offset:offset+BATCH]
   if not LAST_NEXT:break
   req=urllib.request.Request(LAST_NEXT,headers=HEADERS)
  event('query_done',query_id=qid,sql=sql,rows=rows,http_pages=pages,max_page_bytes=maximum,max_page_rows=page_rows,batch_rows=BATCH,stats=stats)
 except Exception as exc:
  event('query_failed',query_id=qid,sql=sql,error=str(exc));raise
 finally:LAST_NEXT=None

def small(sql):
 result=[]
 for cols,rows in stream(sql):
  result.extend(dict(zip([c['name'] for c in cols],row)) for row in rows)
  assert len(result)<1000,'SMALL_QUERY_EXCEEDED_BOUND'
 return result

def ident(name):return '.'.join('"'+x.replace('"','""')+'"' for x in name.split('.'))
def canon(value,t):
 if value is None:return None
 if isinstance(t,dict):
  if t['type']=='list':return [canon(x,t['element']) for x in value]
  if t['type']=='struct':
   return [canon(value.get(f['name']) if isinstance(value,dict) else value[i],f['type']) for i,f in enumerate(t['fields'])]
  raise ValueError('UNSUPPORTED_NESTED_TYPE '+str(t))
 if t=='string':assert isinstance(value,str);return value
 if t in ['long','int']:assert not isinstance(value,bool);return int(value)
 if t.startswith('decimal('):
  precision,scale=[int(x) for x in t[8:-1].split(',')]
  with localcontext() as ctx:
   ctx.prec=60;d=Decimal(value);q=d.quantize(Decimal(1).scaleb(-scale));assert d==q;return format(q,'f')
 if t=='date':return value.isoformat() if isinstance(value,date) else date.fromisoformat(value).isoformat()
 if t in ['timestamp','timestamptz']:
  if isinstance(value,str):value=datetime.fromisoformat(value.replace(' UTC','+00:00').replace('Z','+00:00'))
  if t=='timestamptz':assert value.tzinfo;return value.astimezone(timezone.utc).isoformat(timespec='microseconds')
  assert value.tzinfo is None;return value.isoformat(timespec='microseconds')
 raise ValueError('UNSUPPORTED_TYPE '+str(t))
def packed(row,fields):return json.dumps([canon(row.get(f['name']),f['type']) for f in fields],ensure_ascii=False,separators=(',',':'),allow_nan=False).encode('utf-8')

def arrow_rows(scan):
 with scan.to_arrow_batch_reader() as reader:
  for b in reader:
   check()
   for i in range(0,b.num_rows,BATCH):yield b.slice(i,BATCH).to_pylist()

def trino_type(t):
 if isinstance(t,str):return {'string':'varchar','long':'bigint','int':'integer','timestamptz':'timestamp(6) with time zone','timestamp':'timestamp(6)'}.get(t,t).replace(' ','')
 if t['type']=='list':return 'array('+trino_type(t['element'])+')'
 if t['type']=='struct':return 'row('+','.join(f['name']+trino_type(f['type']) for f in t['fields'])+')'
 raise ValueError('UNSUPPORTED_TRINO_TYPE '+str(t))
def schema_type(f):return trino_type(f['type'])

def aggregate():return {'rows':0,'units':{},'flags':{},'warnings':0,'missing':0,'representative':[],'nso':Counter()}
def observe(row,agg,ds,fields,required,lineages):
 for k in required:assert row.get(k) is not None,(ds,'required',k)
 agg['rows']+=1
 audit=[f for f in fields if f['name'] in AUDIT]
 if audit:assert packed(row,audit) in lineages,(ds,'BRONZE_LINEAGE_MISMATCH')
 if ds=='dead_letters':return
 vname='price' if ds=='worldbank_pinksheet' else 'price_value' if ds=='thitruongnongsan' else 'value'
 value=row[vname];unit=row['unit'];m=row.get('missing_reason');warnings=row.get('_dq_warnings') or []
 agg['warnings']+=bool(warnings);agg['missing']+=m is not None
 assert not(m is not None and value is not None),(ds,'missing_reason_value')
 u=agg['units'].setdefault(unit,{'rows':0,'present':0,'sum_value':Decimal(0)});u['rows']+=1;u['present']+=value is not None
 if value is not None:u['sum_value']+=Decimal(value)
 for f in fields:
  name=f['name']
  if name in ['flag','quality_status','release_status','geography_status','unit_status','statistic_kind','season'] or name.endswith('_flag'):
   flags=agg['flags'].setdefault(name,Counter());flags[row[name]]+=1
 if ds=='nso_vietnam_v2':
  assert int(row['bronze_read_snapshot_id'])==int(CFG['data']['silver'][ds][ds]['bronze_read_snapshot_id'])
  assert row['geography_code'] is None and row['boundary_version'] is None
  ns=agg['nso'];ns['numeric']+=value is not None;ns['missing']+=value is None;ns['zeros']+=(value is not None and Decimal(value)==0);ns['provisional']+=row['release_status']=='provisional';ns['diagnostic_observations']+=row['diagnostic_evidence']!='[]';ns['national']+=row['source_table']=='V06.12.csv'
  if value is not None:assert Decimal(value)==Decimal(row['value_source'])*Decimal(row['conversion_factor'])
  else:assert row['value_source'] is None and row['quality_status']=='MISSING' and row['missing_reason']=='source_marker_unspecified' and row['value_raw'].strip()=='..'
  if row['statistic_kind']=='development_index':assert row['unit']=='percent' and Decimal(row['conversion_factor'])==1
 if len(agg['representative'])<3 or (m is not None and not any(x.get('missing_reason') is not None for x in agg['representative'])):
  keep=[f['name'] for f in fields if f['name'] in ['year','date','value_raw','value_source','value','price','price_value','unit_raw','unit','missing_reason','source_table','geography_raw','season','statistic_kind','bronze_read_snapshot_id']]
  agg['representative'].append({k:str(row[k]) if isinstance(row[k],(Decimal,datetime,date)) else row[k] for k in keep})

AUDIT=['_ingestion_run_id','_ingestion_batch_id','_ingestion_chunk_id','_ingestion_timestamp','_source_id','_source_file','_source_checksum','_source_snapshot_id']

cat=BronzeIcebergWriter().get_iceberg_catalog();result={'status':'RUNNING','tables':{}}
for spec in CFG['tables']:
 ds=spec['dataset'];identity=spec['identity'];t=cat.load_table(spec['target'].removeprefix('iceberg.'));fields=identity['schema']['fields'];keyfields=[next(f for f in fields if f['name']==k) for k in spec['keys']]
 assert str(t.current_snapshot().snapshot_id)==identity['snapshot_id'] and t.metadata_location==identity['metadata_location']
 schema=small('DESCRIBE '+ident(spec['target']));assert {r['Column']:r['Type'].replace(' ','') for r in schema}=={f['name']:schema_type(f) for f in fields}
 db=sqlite3.connect(str(ROOT/(ds+'.sqlite')));db.execute('PRAGMA cache_size=-4096');db.execute('PRAGMA temp_store=FILE');db.execute('CREATE TABLE expected(k BLOB PRIMARY KEY,h BLOB NOT NULL,seen INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID')
 count=0
 for rows in arrow_rows(t.scan(snapshot_id=int(identity['snapshot_id']))):
  db.executemany('INSERT INTO expected(k,h) VALUES (?,?)',[(packed(r,keyfields),hashlib.sha256(packed(r,fields)).digest()) for r in rows]);count+=len(rows)
 db.commit();assert count==spec['rows']
 sql='SELECT '+','.join(ident(f['name']) for f in fields)+' FROM '+ident(spec['target'])+' FOR VERSION AS OF '+identity['snapshot_id'];read=0
 for columns,rows in stream(sql):
  assert [c['name'] for c in columns]==[f['name'] for f in fields];updates=[]
  for values in rows:
   row=dict(zip([f['name'] for f in fields],values));updates.append((packed(row,keyfields),hashlib.sha256(packed(row,fields)).digest()))
  cursor=db.executemany('UPDATE expected SET seen=seen+1 WHERE k=? AND h=?',updates);assert cursor.rowcount==len(rows),'TRINO_NATIVE_FULL_ROW_MISMATCH';read+=len(rows)
 db.commit();assert read==count and db.execute('SELECT count(*) FROM expected WHERE seen<>1').fetchone()[0]==0
 h=hashlib.sha256()
 for key,rowhash in db.execute('SELECT k,h FROM expected ORDER BY k'):h.update(len(key).to_bytes(8,'big')+key+rowhash)
 rec={'status':'PASS','rows':read,'unique_keys':read,'full_content':'ALL_ROWS_ALL_COLUMNS_NATIVE_TRINO_MATCH','canonical_sha256':h.hexdigest(),'identity':identity,'schema':schema};result['tables'][ds]=rec;event('dataset_pass',dataset=ds,record=rec);db.close()
result['status']='PASS';event('validation_pass',result=result)
