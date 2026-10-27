"""Export four verified published-Gold CSVs for local Power BI Desktop Import."""
import argparse
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify',type=Path,help='Offline CSV checksums/schema/row counts; does not verify current publication')
    parser.add_argument('--_hold-publication',action='store_true',help=argparse.SUPPRESS)
    args=parser.parse_args()
    if args._hold_publication:
        if args.verify:parser.error('Incompatible guard/verify modes')
        from analytics.powerbi_fence import serve
        serve();return
    from analytics.powerbi import export_bundle,verify_bundle
    if args.verify:
        manifest=verify_bundle(args.verify)
        print(json.dumps(dict(status='VERIFIED_OFFLINE',bundle=str(args.verify.resolve()),published_batch=manifest['published_batch'])))
    else:
        folder,manifest=export_bundle(ROOT/'exports/powerbi')
        print(json.dumps(dict(status='POWERBI_EXPORT_COMPLETE',bundle=str(folder),published_batch=manifest['published_batch'],
                              rows={n:v['rows'] for n,v in manifest['outputs'].items()})))


if __name__=='__main__':main()
