"""Static PBIR/field-reference checks using cached official Microsoft JSON schemas.

This proves metadata shape, not Desktop rendering or DAX execution.
"""
import argparse
import json
from pathlib import Path
import re
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))


def validate_project(project,schemas,fetch=False):
    from jsonschema import Draft7Validator
    from referencing import Registry,Resource
    from analytics.marts.report import TABLES,SCHEMAS,MEASURES
    schemas=Path(schemas)
    def retrieve(uri):
        prefix='https://developer.microsoft.com/json-schemas/'
        if not uri.startswith(prefix) or '..' in uri[len(prefix):].split('/'):
            raise ValueError('Only official Microsoft report schemas are admitted')
        target=schemas/uri[len(prefix):]
        if not target.exists():
            if not fetch:raise ValueError('Schema not cached; rerun with --fetch-schemas: '+uri)
            import urllib.request
            import urllib.error
            try:
                with urllib.request.urlopen(uri,timeout=20) as response:raw=response.read(2*1024*1024+1)
            except urllib.error.HTTPError as error:
                if error.code!=404:raise
                # Same official schema repository; never substitute another
                # schema version when Desktop emits an unpublished version.
                url='https://raw.githubusercontent.com/microsoft/json-schemas/main/'+uri[len(prefix):]
                with urllib.request.urlopen(url,timeout=20) as response:raw=response.read(2*1024*1024+1)
            if len(raw)>2*1024*1024:raise ValueError('Schema response too large')
            target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
        return Resource.from_contents(json.loads(target.read_text(encoding='utf-8')))
    registry=Registry(retrieve=retrieve)
    for path in schemas.rglob('*.json'):
        schema=json.loads(path.read_text(encoding='utf-8'))
        if isinstance(schema,dict) and schema.get('$id'):
            resource=Resource.from_contents(schema)
            registry=registry.with_resource(schema['$id'],resource)
            registry=registry.with_resource('https://developer.microsoft.com/json-schemas/'+path.relative_to(schemas).as_posix(),resource)
    fields={TABLES[n]:{c for c,_ in s} for n,s in SCHEMAS.items()}
    fields.update(CalendarYear={'Year'},BenchmarkMonth={'Date'},QuoteDate={'Date'})
    checked=0;visuals=0
    def refs(value):
        if isinstance(value,dict):
            for kind in ('Column','Measure'):
                if kind in value and isinstance(value[kind],dict) and 'Property' in value[kind]:
                    node=value[kind];table=node['Expression']['SourceRef']['Entity'];name=node['Property']
                    if kind=='Column' and name not in fields.get(table,set()):raise ValueError(f'Broken column reference {table}.{name}')
                    if kind=='Measure' and (table!='Overview' or name not in MEASURES):raise ValueError(f'Broken measure reference {table}.{name}')
            for child in value.values():refs(child)
        elif isinstance(value,list):
            for child in value:refs(child)
    project=Path(project)
    for path in project.rglob('*'):
        if '.pbi' in path.parts:continue
        if path.suffix not in ('.json','.pbip','.pbir','.pbism'):continue
        data=json.loads(path.read_text(encoding='utf-8'))
        if '$schema' in data:
            schema=registry.get_or_retrieve(data['$schema']).value.contents
            errors=list(Draft7Validator(schema,registry=registry).iter_errors(data))
            if errors:raise ValueError(str(path)+': '+str(errors[0].message))
            checked+=1
        if path.name=='visual.json':
            visuals+=1;refs(data)
            pos=data['position']
            if pos['x']<0 or pos['y']<0 or pos['x']+pos['width']>1280 or pos['y']+pos['height']>720:raise ValueError('Visual outside 16:9 canvas')
        if len(str(path.resolve()))>=260:raise ValueError('Windows path too long')
    pages=project/'R/definition/pages'
    order=json.loads((pages/'pages.json').read_text())['pageOrder']
    if len(order)!=6 or len(set(order))!=6:raise ValueError('Exactly six unique report pages required')
    ids=[]
    for pid in order:
        page=pages/pid
        ids.extend((pid,json.loads(p.read_text(encoding='utf-8'))['name']) for p in page.glob('visuals/*/visual.json'))
    if len(ids)!=len(set(ids)):raise ValueError('Duplicate visual ID within page')
    # Cross-table references in DAX: syntax/type/engine validation is a separate gate.
    for name,record in MEASURES.items():
        for table,col in re.findall(r'([A-Za-z][A-Za-z0-9]*)\[([^\]]+)\]',record['dax']):
            if col not in fields.get(table,set()):raise ValueError(f'DAX field reference {name}: {table}[{col}]')
    return {'status':'STATIC_PASS','validated_source_documents':checked,'visuals':visuals,'pages':len(order),'measures':len(MEASURES),'desktop_rendering':'NOT_PROVEN','dax_engine_execution':'NOT_PROVEN'}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--schemas',type=Path,default=ROOT/'.test-tmp/pbi-schemas');p.add_argument('--fetch-schemas',action='store_true');a=p.parse_args()
    print(json.dumps(validate_project(ROOT/'powerbi',a.schemas,a.fetch_schemas)))
