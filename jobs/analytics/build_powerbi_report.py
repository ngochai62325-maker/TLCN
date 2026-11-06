"""Generate editable Power BI project source from one verified mart bundle."""
import argparse
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))

if __name__=='__main__':
    from analytics.marts.report import build_project
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--bundle',type=Path,required=True)
    args=parser.parse_args();print(build_project(ROOT/'powerbi',args.bundle))
