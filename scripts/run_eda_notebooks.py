"""Execute owned notebooks sequentially, then render measured Markdown findings.

No allow_errors, no stale cache fallback. A failed notebook is saved with its
actual error and reported PARTIAL; no numerical findings invented for that run.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import threading
import time
import asyncio

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import nbformat
from nbclient import NotebookClient

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from analytics.client import windows_available_bytes


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--notebook',help='Exact owned notebook filename, or all six by default')
    args=parser.parse_args()
    paths=sorted((ROOT/'notebooks/analytics').glob('*.ipynb'))
    if args.notebook:
        paths=[p for p in paths if p.name==args.notebook]
        if not paths:raise ValueError('Unknown notebook name')
    folder=ROOT/'docs/analytics/evidence';folder.mkdir(exist_ok=True)
    journal=[]
    for path in paths:
        nb=nbformat.read(path,as_version=4)
        if nb.metadata.get('eda_generated_by')!='TLCN-EDA-01':raise ValueError('Unowned notebook')
        evidence_path=folder/(path.stem+'.json')
        # Remove only this run's derived evidence, never historical Gold evidence.
        if evidence_path.exists():evidence_path.unlink()
        started=time.monotonic();print('START',path.name,flush=True)
        samples=[];done=threading.Event()
        def monitor():
            import psutil
            process=psutil.Process()
            while not done.wait(.5):
                try:
                    rss=sum(p.memory_info().rss for p in process.children(recursive=True) if p.is_running())
                    samples.append({'windows_available_bytes':windows_available_bytes(),'child_rss_bytes':rss})
                except (OSError,psutil.Error):pass
        thread=threading.Thread(target=monitor,daemon=True);thread.start()
        status,error='PASS',None
        try:
            NotebookClient(nb,timeout=120,kernel_name='python3',resources={'metadata':{'path':str(ROOT)}}).execute()
            if not evidence_path.exists():raise RuntimeError('Missing final notebook evidence')
            evidence=json.loads(evidence_path.read_text(encoding='utf-8'))
            rendered=[]
            for finding in evidence['findings']:
                rendered.append('### '+finding['id']+' — '+finding['title']+'\n\n**OBSERVATION:** '+finding['observation']+
                    '\n\n**Scope:** '+finding['scope']+'\n\n**INTERPRETATION / LIMITATION:** '+finding['interpretation']+
                    '\n\n**Reproduce:** query `'+finding['query']+'`, '+finding['cell']+'.')
            for cell in nb.cells:
                if cell.metadata.get('eda_findings'):
                    cell.source='## Key Findings\n\nMeasured batch `'+evidence['batch']+'`.\n\n'+'\n\n'.join(rendered)
        except Exception as exc:
            status,error='PARTIAL',str(exc)[-3000:]
        finally:
            done.set();thread.join(timeout=1)
        nb.metadata['eda_execution']={'status':status,'executed_at':datetime.now(timezone.utc).isoformat()}
        nbformat.validate(nb);nbformat.write(nb,path)
        entry={'notebook':path.name,'status':status,'seconds':time.monotonic()-started,'error':error,
               'windows_available_min_bytes':min((s['windows_available_bytes'] for s in samples),default=None),
               'child_rss_peak_bytes':max((s['child_rss_bytes'] for s in samples),default=None),
               'code_cells':sum(c.cell_type=='code' for c in nb.cells),
               'charts':sum('image/png' in o.get('data',{}) for c in nb.cells if c.cell_type=='code' for o in c.get('outputs',[]))}
        journal.append(entry)
        (folder/('execution_'+path.stem+'.json')).write_text(json.dumps(entry,indent=2),encoding='utf-8')
        print('DONE',path.name,status,round(entry['seconds'],1),'s',str(error or '')[:500],flush=True)
        if status!='PASS':break
    if any(j['status']!='PASS' for j in journal):raise SystemExit(1)


if __name__=='__main__':main()
