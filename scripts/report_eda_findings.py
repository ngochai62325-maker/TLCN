"""Render findings only from successfully executed, publication-bound notebooks."""
import json
from pathlib import Path
import xml.etree.ElementTree as ET
import hashlib
import subprocess

ROOT=Path(__file__).resolve().parents[1]
E=ROOT/'docs/analytics/evidence'


def main():
    runs=[];queries=[];findings=[];batch=None
    for path in sorted((ROOT/'notebooks/analytics').glob('*.ipynb')):
        nb=json.loads(path.read_text(encoding='utf-8'))
        run=json.loads((E/('execution_'+path.stem+'.json')).read_text(encoding='utf-8'))
        evidence=json.loads((E/(path.stem+'.json')).read_text(encoding='utf-8'))
        assert run['status']=='PASS' and nb['metadata']['eda_execution']['status']=='PASS'
        assert all(c.get('execution_count') is not None and not any(o['output_type']=='error' for o in c.get('outputs',[]))
                   for c in nb['cells'] if c['cell_type']=='code')
        assert run['charts']>0
        if batch is None:batch=evidence['batch']
        assert batch==evidence['batch'] and all(q['batch']==batch for q in evidence['queries'])
        qrefs={}
        for cell in nb['cells']:
            for query in set(cell.get('metadata',{}).get('eda_queries',[])):qrefs.setdefault(query,[]).append(cell['id'])
        for finding in evidence['findings']:
            finding={**finding,'notebook':path.name,'cell_ids':qrefs.get(finding['query'],[])}
            assert finding['cell_ids'],finding['id']
            findings.append(finding)
        queries += evidence['queries'];runs.append({**run,'queries':len(evidence['queries']),
            'returned_rows':[q['row_count'] for q in evidence['queries']], 'query_names':sorted(set(q['query'] for q in evidence['queries']))})
    assert len(runs)==6
    priority=['VN01','VN05','VN02','FAO01','PR01','TR03','TR04','PSD02','PR02','VN03','VN04','FAO02','FAO03','SUA01','SUA02','PSD01','PR03','TR01','TR02','OV01','OV02','OV03']
    findings.sort(key=lambda f:priority.index(f['id']))
    parts=['# EDA Findings — 27/10/2026',
      '**EDA_PHASE_01_COMPLETE.** Sáu notebooks đã execute độc lập theo từng cell; không có unexecuted/error code cells. '
      'Không Dashboard/ML, không lấy KPI từ consumer 20 rows. Xếp thứ tự findings dưới đây theo business relevance và evidence clarity, không phải statistical causality.',
      f'Published batch: `{batch}`. Gold model `gold-20261025.1`. '
      'Scope/typed filters, SQL hashes, query IDs và timings ở từng [evidence JSON](evidence/). '
      'Tất cả findings là descriptive observations trên immutable snapshots, không xác nhận official geography conformance, causality hoặc publication vintage.',
      '## VERIFIED FINDINGS']
    for f in findings:
        notebook='../../notebooks/analytics/'+f['notebook']
        parts += ['### '+f['id']+' — '+f['title'],
                  '**Business question / scope:** '+f['scope'],
                  '**OBSERVATION — exact measured result:** '+f['observation'],
                  '**INTERPRETATION / business implication / limitation:** '+f['interpretation'],
                  '**Evidence:** ['+f['notebook']+']('+notebook+'), '+f['cell']+
                  '; query `'+f['query']+'`; retrieval cell IDs '+', '.join('`'+c+'`' for c in f['cell_ids'])+'. '+
                  '[Query parameters/calculated values](evidence/'+Path(f['notebook']).stem+'.json). Confidence: **VERIFIED within stated scope**, no causal claim.',
                  '**Dashboard:** '+('coverage/missingness table' if f['id'].startswith('OV') else 'source-filtered trend, paired-measure or comparison chart; see MVP table below')]
        if f['id']=='OV02':
            parts+=['| Fact | Represented periods | Calendar span | Absent periods |','|---|---:|---:|---:|']
            parts += [f"| {r['fact']} | {r['represented']} | {r['calendar_span']} | {r['absent_periods']} |" for r in f['numbers']]
            parts+=['Daily absences can include non-reporting days. These are whole-fact domains, not proof of continuity for every individual series.']
    parts += ['## Data quality và scope discoveries',
      '- Present-NULL, genuine zero và absent calendar rows được đếm riêng. Structural exclusions không thể suy ra từ retained Gold rows. Source flags/release statuses được giữ; unconfirmed không thành FINAL.',
      '- V06.12 national annual quantity và V06.14 reported yield là publications riêng. Không resolve bằng MAX/FIRST/latest ingestion; không SUM national/regional/provincial hoặc annual với seasons.',
      '- Global dimension membership không chứng minh fact availability. SUA Viet Nam: `F0030` trả empty; final analysis chọn actual CPC `23161.02` (Rice, milled). Có 8 metric codes/112 observations trong 2010–2023, không phải đủ 12 supported codes toàn model. Empty-scope attempt được giữ tại [attempt evidence](evidence/attempt_03_empty_F0030.json). Không thay absent cells bằng zero.',
      '- Trade whole-fact max year 2024 không có nghĩa Viet Nam reporter có data năm đó. [2024 smoke](evidence/trade_smoke_2024.json) trả 0 groups; actual selected product coverage kết thúc 2023. [2023 smoke](evidence/trade_smoke_2023.json) trả 85 groups. Notebook 05 dùng 2019–2023 và trả 397 bilateral-year groups, không collect 1,226,470 fact rows.',
      '- NSO area, FAO harvested area, SUA Rice milled và USDA Rice, Milled không tự trở thành các measures tương đương giữa providers. World Bank table chứa cả non-rice commodity benchmarks; notebook chọn exact `rice_thai_5`.',
      '## EXPLORATORY HYPOTHESES — chưa được kiểm định',
      '1. Production growth có association với thay đổi area và computed yield. Derived yield là accounting ratio, không phải independent causal explanation.',
      '2. WB benchmark volatility và trade unit-value changes có thể liên quan composition/contracts/reporting; không dùng coincident movements để suy cause.',
      '3. Monthly seasonality hoặc khác biệt giữa NSO publications cần investigation riêng, coverage checks và statistical tests; chưa có kết luận seasonality.',
      '## UNSUPPORTED / DEFERRED',
      'Official provincial maps/63→34 transitions; universal product/geography crosswalks; mirrored global trade totals; partner concentration/shares khi denominator chưa verified; FX conversions; paddy↔milled conversions; closing stocks suy ra cho SUA; cross-provider balance reconciliation; vintage/PIT forecasts; unrestricted 39-year Trade pivot.',
      '## Notebook execution và performance',
      '| Notebook | Queries | Returned rows per request | Charts | Wall seconds |','|---|---:|---|---:|---:|']
    for r in runs:
        parts.append(f"| {r['notebook']} | {r['queries']} | {', '.join(map(str,r['returned_rows']))} | {r['charts']} | {r['seconds']:.2f} |")
    peak=max(q['query_seconds'] for q in queries);size=max(q['response_bytes'] for q in queries)
    minimum=min(r['windows_available_min_bytes'] for r in runs);rss=max(r['child_rss_peak_bytes'] for r in runs)
    parts += [f"{len(queries)} requests gồm publication metadata calls; SQL HTTP elapsed tối đa **{peak:.3f} s**; response tối đa **{size:,} bytes**. "
       f"Sampled Windows available minimum **{minimum/1024**3:.2f} GiB**; notebook child-process RSS peak **{rss/1024**2:.2f} MiB**. "
       'Đây không phải toàn bộ WSL/Trino heap peak. Trino per-query peak memory/spill không được statement API trả trong profile này (NULL), không giả định 0. '
       'Giữ session memory cap 64 MB và query timeout 15 s; không đổi Trino 448/512 MiB heap. No result cache. No query exceeded its row/byte/time budget.',
       '## Gold preservation và tests',
       'Xem [final preservation](evidence/preservation_EDA-05.json): 36 existing tables giữ UUID/snapshot/metadata/data files; publication/context/manifest và Gold DagRun states không đổi. Không trigger pipeline hoặc ghi Iceberg. '
       'Unit/Gold regression và 4 Trino VALUES fixtures được phân biệt với actual notebook analytical execution. Tests không tạo catalog objects.',
       '## Dashboard MVP — đề xuất dựa trên findings',
       '| Priority | KPI/chart | Query / required filters | Cost & limits |','|---|---|---|---|',
       '| 1 | Numeric coverage và present-NULL/absent-period diagnostics | fact_overview + period_coverage, exact fact | Small aggregates; không source-quality ranking |',
       '| 2 | NSO national production/area trends | nso_observations; V06.12, national, quantity, annual, 1990–2024 | Giữ publication/measure/unit riêng |',
       '| 3 | NSO seasonal production | same query; winter_spring/summer_autumn/main_season | Không cộng annual vào seasons; no official-map claim |',
       '| 4 | FAO production và paired yield comparisons | fao_production; CPC 0113, 4 exact M49 keys, common year | Selected-universe ranking, no global total/AVG yields |',
       '| 5 | WB monthly rice benchmark + 12M volatility | price_observations; exact rice_thai_5 series | USD/tonne, calendar windows, no filling |',
       '| 6 | Domestic quote series trend + gaps | price_coverage then price_observations; exact selected quote identity | VND/kg; do not blend markets/sources |',
       '| 7 | Reported bilateral quantity / unit-value comparison | trade; reporter 704, CPC 23161.02, export, 2019–2023, selected partners | 397 groups in tested scope; not market price or global share |',
       'SUA/PSD stocks/flows là trang mở rộng, tách source/time semantics. Chưa code Streamlit/Dash. '
       'Next smallest task: một local read-only Dashboard với publication/coverage + NSO annual page, dùng chính allowlisted CLI và source-filter contracts đã kiểm chứng; sau đó thêm FAO/price pages. '
       'Không đổi Gold semantic hashes/publication; serialize requests và benchmark lại sau khi thêm UI.']
    # Adjacent Markdown table rows must not be separated by blank paragraphs.
    body=''
    previous_table=False
    for part in parts:
        is_table=part.startswith('|')
        body+=('\n' if previous_table and is_table else '\n\n')+part
        previous_table=is_table
    (ROOT/'docs/analytics/EDA_FINDINGS.md').write_text(body.lstrip()+'\n',encoding='utf-8')
    summary={'status':'COMPLETE','batch':batch,'notebooks':runs,'findings':len(findings),
       'requests':len(queries),'max_sql_seconds':peak,'max_response_bytes':size,'minimum_windows_available_bytes':minimum,
       'peak_notebook_child_rss_bytes':rss,'trino_values_fixtures':4,'dashboard_implemented':False}
    summary['git_baseline']={'branch':subprocess.check_output(['git','branch','--show-current'],cwd=ROOT).decode().strip(),
                             'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT).decode().strip()}
    summary['analytics_source_sha256']={p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest()
        for folder,pattern in [('src/analytics','*.py'),('queries/analytics','*.sql'),('jobs/analytics','*.py')]
        for p in sorted((ROOT/folder).glob(pattern))}
    changed=subprocess.check_output(['git','diff','--name-only','-z'],cwd=ROOT).decode().split('\0')
    untracked=subprocess.check_output(['git','ls-files','--others','--exclude-standard','-z'],cwd=ROOT).decode().split('\0')
    summary['created_or_updated_files']=sorted(set(p for p in changed+untracked if p))
    xml=E/'all_unit_tests.xml'
    if xml.exists():
        tree=ET.parse(xml);summary['unit_tests']=len(list(tree.iter('testcase')))
        assert not list(tree.iter('failure')) and not list(tree.iter('error')) and not list(tree.iter('skipped'))
    (ROOT/'docs/analytics/EDA_ACCEPTANCE.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    print('Rendered',len(findings),'verified findings; six notebooks PASS')


if __name__=='__main__':main()
