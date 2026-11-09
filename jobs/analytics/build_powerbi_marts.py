"""Build all eight Analytics marts as one immutable verified Power BI CSV bundle."""
import json
import argparse
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))

if __name__=='__main__':
    argparse.ArgumentParser(description=__doc__).parse_args()
    from analytics.powerbi import export_bundle
    folder,manifest=export_bundle(ROOT/'exports/powerbi',report=True)
    print(json.dumps({'status':'DATA_MARTS_EXPORTED','bundle':str(folder),'published_batch':manifest['published_batch'],
        'rows':{n:v['rows'] for n,v in manifest['outputs'].items()}},ensure_ascii=False))
