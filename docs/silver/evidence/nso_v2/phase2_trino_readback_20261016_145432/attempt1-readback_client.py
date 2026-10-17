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
HEADERS={'X-Trino-User':'phase2-readonly','Content-Type':'text/plain','X-Trino-Session':','.join(k+'='+v for k,v in SESSION.items())}
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
cat=BronzeIcebergWriter().get_iceberg_catalog();assert cat is not None
result={'status':'RUNNING','datasets':{},'method':'SHA256 canonical typed full rows, exact business-key matching on disk, independent ordered native/Trino SHA256 per partition','batch_rows':BATCH,'session':SESSION}
for ds,c in [*CFG['data']['contracts'].items(),('dead_letters',{'target':'iceberg.silver_local.dead_letters','keys':['source_dataset','quarantine_record_id'],'columns':[]})]:
 check();n=CFG['native']['dead_letters'] if ds=='dead_letters' else CFG['native']['datasets'][ds];identity=n['identity'] if ds=='dead_letters' else n['silver'];snapshot=int(identity['snapshot_id']);table=cat.load_table(c['target'].removeprefix('iceberg.'))
 assert str(table.metadata.table_uuid)==identity['uuid'] and str(table.current_snapshot().snapshot_id)==identity['snapshot_id'] and table.metadata_location==identity['metadata_location']
 fields=identity['schema']['fields'];keys=[next(f for f in fields if f['name']==k) for k in c['keys']];required=set(c['keys'])|{f['name'] for f in c['columns'] if not f.get('nullable',True)}
 schema=small('DESCRIBE '+ident(c['target']));assert {f['Column']:f['Type'].replace(' ','') for f in schema}=={f['name']:schema_type(f) for f in fields}
 cols=','.join(ident(f['name']) for f in fields);source=ident(c['target'])+' FOR VERSION AS OF '+str(snapshot)
 lineages=set()
 if ds!='dead_letters':
  br=cat.load_table(c['bronze'].removeprefix('iceberg.'));pin=int(CFG['data']['silver'][ds][ds]['bronze_read_snapshot_id']);assert str(br.current_snapshot().snapshot_id)==str(pin)
  auditfields=[f for f in fields if f['name'] in AUDIT]
  with br.scan(snapshot_id=pin).select(*AUDIT).to_arrow_batch_reader() as reader:
   for batch in reader:
    check();distinct=pa.Table.from_batches([batch]).group_by(AUDIT).aggregate([]).to_pylist()
    for row in distinct:lineages.add(packed(row,auditfields))
  assert lineages
 if ds=='faostat_trade':
  partition_counts=small('SELECT year,count(*) AS rows FROM '+source+' GROUP BY year');assert all(p['year'] is not None for p in partition_counts)
  partition_counts.sort(key=lambda p:(p['rows'],p['year']))
 else:partition_counts=[{'year':None,'rows':n['rows']}]
 assert sum(p['rows'] for p in partition_counts)==n['rows']
 na=aggregate();ta=aggregate();parts=[]
 for partition in partition_counts:
  check();year=partition['year'];predicate=' WHERE year='+str(int(year)) if year is not None else '';sql='SELECT '+cols+' FROM '+source+predicate
  if not parts:
   explain=small('EXPLAIN (TYPE DISTRIBUTED) '+sql);event('explain',dataset=ds,year=year,plan=explain)
  db=sqlite3.connect(str(ROOT/(ds+'-'+str(year)+'.sqlite')));db.execute('PRAGMA cache_size=-8192');db.execute('PRAGMA temp_store=FILE');db.execute('CREATE TABLE expected(k BLOB PRIMARY KEY,h BLOB NOT NULL,t BLOB,seen INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID')
  native_count=0;trino_count=0
  for rows in arrow_rows(table.scan(snapshot_id=snapshot,row_filter=EqualTo('year',int(year)) if year is not None else AlwaysTrue())):
   inserts=[]
   for row in rows:
    observe(row,na,ds,fields,required,lineages);inserts.append((packed(row,keys),hashlib.sha256(packed(row,fields)).digest()))
   try:db.executemany('INSERT INTO expected(k,h) VALUES (?,?)',inserts)
   except sqlite3.IntegrityError:raise ValueError('DUPLICATE_NATIVE_BUSINESS_KEY '+ds)
   native_count+=len(rows)
  db.commit();assert native_count==partition['rows'],(ds,year,'native_partition_count',native_count,partition['rows'])
  for columns,rows in stream(sql):
   assert [x['name'] for x in columns]==[f['name'] for f in fields]
   updates=[]
   for values in rows:
    row=dict(zip([f['name'] for f in fields],values));observe(row,ta,ds,fields,required,lineages);h=hashlib.sha256(packed(row,fields)).digest();updates.append((h,packed(row,keys),h))
   cursor=db.executemany('UPDATE expected SET t=?,seen=seen+1 WHERE k=? AND h=?',updates);assert cursor.rowcount==len(rows),(ds,year,'FULL_CONTENT_HASH_MISMATCH')
   trino_count+=len(rows)
  db.commit();assert native_count==trino_count and db.execute('SELECT count(*) FROM expected WHERE seen<>1 OR t IS NULL').fetchone()[0]==0,(ds,year,'MISSING_OR_DUPLICATE_TRINO_KEY')
  prefix=json.dumps({'fields':fields,'partition':partition},ensure_ascii=False,sort_keys=True,separators=(',',':')).encode();nh=hashlib.sha256(prefix);th=hashlib.sha256(prefix)
  cursor=db.execute('SELECT k,h,t FROM expected ORDER BY k')
  while True:
   rows=cursor.fetchmany(BATCH)
   if not rows:break
   for key,native_hash,trino_hash in rows:
    header=len(key).to_bytes(8,'big')+key;nh.update(header+native_hash);th.update(header+trino_hash)
  assert nh.digest()==th.digest()
  rec={'year':year,'rows':native_count,'unique_keys':native_count,'native_sha256':nh.hexdigest(),'trino_sha256':th.hexdigest(),'status':'PASS','sql':sql};parts.append(rec);event('partition_pass',dataset=ds,**rec);db.close()
 assert na['rows']==ta['rows']==n['rows']
 for k in ['units','flags','warnings','missing','nso']:assert na[k]==ta[k],(ds,k,'NATIVE_TRINO_SEMANTICS')
 if ds!='dead_letters':
  m=CFG['data']['silver'][ds][ds];assert na['warnings']==m['warning_count']
  for u in n['unit_distribution']:
   got=na['units'][u['unit']];assert got['rows']==u['rows'] and got['present']==u['present'] and (not got['present'] or got['sum_value']==Decimal(u['sum_value']))
  if ds=='nso_vietnam_v2':
   prior=CFG['prior']
   for k in ['numeric','missing','zeros','provisional','diagnostic_observations']:assert na['nso'][k]==prior[k],k
   assert na['nso']['national']==prior['actual_counts']['national']
   for col,key in [('unit','units'),('season','seasons'),('geography_status','geography_status')]:
    actual={u:v['rows'] for u,v in na['units'].items()} if col=='unit' else dict(na['flags'][col]);assert actual==prior[key],key
 rec={'status':'PASS','table':c['target'],'uuid':identity['uuid'],'snapshot_id':identity['snapshot_id'],'rows':n['rows'],'unique_keys':sum(p['unique_keys'] for p in parts),'partitions':parts,'schema':schema,'aggregate':{'warnings':ta['warnings'],'missing':ta['missing'],'nso':dict(ta['nso']),'representative':ta['representative']},'lineage_unique_tuples':len(lineages),'full_content':'PASS_ALL_ROWS_ALL_COLUMNS','required_fields':'PASS','business_keys':'PASS','schema_compatibility':'PASS','units_flags_values':'PASS'}
 result['datasets'][ds]=rec;event('dataset_pass',dataset=ds,record=rec)
 (ROOT/'result.json').write_text(json.dumps(result,ensure_ascii=False,default=str),encoding='utf-8')
result['status']='PASS';event('validation_pass',result=result)
