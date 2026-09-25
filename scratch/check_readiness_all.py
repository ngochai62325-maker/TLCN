import sys
import os

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, 'src')

from ingestion.config.registry import SourceRegistry
from ingestion.readiness.source_readiness import ReadinessChecker
from unittest.mock import patch

reg = SourceRegistry('src/ingestion/config/source_registry.yaml')
reg.load()
checker = ReadinessChecker()

print(f"{'SOURCE ID':<28} | {'TYPE':<10} | {'READY':<5} | {'LATENCY':<8} | {'DETAILS'}")
print("-" * 105)
with patch('requests.head', side_effect=RuntimeError('HTTP called!')), patch('requests.get', side_effect=RuntimeError('HTTP called!')):
    for s in reg.get_all_sources().values():
        res = checker.check(s)
        meta = res.source_metadata or {}
        p = meta.get('path', '')
        size = meta.get('size', 0)
        fc = meta.get('file_count')
        extra = f"files={fc}, total={size:,}B" if fc and fc > 1 else f"size={size:,}B"
        print(f"{s.source_id:<28} | {s.source_type.value:<10} | {str(res.ready):<5} | {res.reason:<35} | {extra} ({p})")
