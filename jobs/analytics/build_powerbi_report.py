"""Generate editable Power BI project source from one verified mart bundle."""
import argparse
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))

if __name__=='__main__':
    from analytics.marts.report import build_project
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle',type=Path,required=True)
    parser.add_argument('--allow-historical-sql',action='store_true',help='Skip SQL template hash re-check when rebuilding from an older verified bundle after queries/ changed')
    args=parser.parse_args()
    print(build_project(ROOT/'powerbi',args.bundle,verify_sql_provenance=not args.allow_historical_sql))
