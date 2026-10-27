"""Read-only comparison with the EDA baseline; no SQL workload or mutations."""
import argparse
import json
from pathlib import Path
import subprocess
from datetime import datetime, timezone

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--phase',required=True)
    phase=parser.parse_args().phase
    if not phase.replace('-','').replace('_','').isalnum():raise ValueError('Invalid phase')
    before=json.loads((ROOT/'docs/analytics/eda_baseline_before.json').read_text(encoding='utf-8-sig'))
    result=subprocess.run(['docker','exec','-e','PYTHONPATH=/workspace/src:/workspace','tlcn-local-runner-1',
        'python3','/workspace/scripts/validate_foundation_readonly.py'],capture_output=True,text=True,encoding='utf-8',timeout=30,check=True)
    after=json.loads(result.stdout)
    checks={k:before[k]==after[k] for k in ('tables','context','pointer','manifest_hash','gold_dagruns')}
    evidence={'phase':phase,'status':'PASS' if all(checks.values()) else 'FAIL','checks':checks,
              'existing_tables':len(after['tables']),'manifest_hash':after['manifest_hash'],
              'captured_at':datetime.now(timezone.utc).isoformat()}
    folder=ROOT/'docs/analytics/evidence';folder.mkdir(exist_ok=True)
    (folder/('preservation_'+phase+'.json')).write_text(json.dumps(evidence,indent=2),encoding='utf-8')
    print(json.dumps(evidence))
    if not all(checks.values()):raise RuntimeError('Gold/source baseline changed')


if __name__=='__main__':main()
