"""Create the six owned EDA notebooks; never overwrites pre-existing unrelated work."""
from pathlib import Path
import argparse
import textwrap
import nbformat as nbf

ROOT=Path(__file__).resolve().parents[1]
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--refresh-generated',action='store_true')
parser.add_argument('--notebook',help='Generate only this exact owned filename')
args=parser.parse_args()


def md(text,**metadata):return nbf.v4.new_markdown_cell(textwrap.dedent(text).strip(),metadata=metadata)
def code(text):return nbf.v4.new_code_cell(textwrap.dedent(text).strip())


SETUP='''
from pathlib import Path
import sys, json
ROOT = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p/'src/gold/consumer.py').exists())
sys.path.insert(0, str(ROOT/'src'))  # repository bootstrap, no credentials
import pandas as pd
import numpy as np
get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt
from analytics.client import EDAClient
from analytics.methods import calendar_yoy, endpoint_change, monthly_calendar
from analytics.notebook import inspect_frame, label, resolve, annual_plot, save_evidence
pd.set_option('display.max_columns', 20)
plt.rcParams.update({'axes.spines.top':False,'axes.spines.right':False,'font.size':10})
client = EDAClient()
findings = []
inventory = client.query('publication')
EXPECTED_BATCH = 'ec421297c194804dc7a3a63aacfda37a65dacfac07245d118e34b34506ca2235'
assert client.batch == EXPECTED_BATCH, 'Publication changed: review scope before reproducing EDA'
display(inventory)
print('Published batch:', client.batch, '| No query cache | Windows/WSL/writer gates checked per request')
'''


def base(title,scope):
    return [md(f'''# {title}

{scope}

Environment: Windows host, Python 3.10–3.13; `pip install -r requirements/analytics.txt`.
Docker `tlcn-local` đã healthy, Windows available >=2 GiB, WSL >=3 GiB, không active writer/DagRun.
Không start Spark. Mỗi notebook chạy độc lập, theo thứ tự cells. Query names/typed filters hiển thị
trong retrieval cells; kết quả là full scoped results/aggregates, **không phải 20-row sample consumer**.
Source-scoped identities không phải official crosswalk. Không forward-fill/interpolate.

Chạy `python scripts/run_eda_notebooks.py --notebook <filename>` từ repo root để execute và cập nhật
Markdown diễn giải bằng đúng measured evidence. Kết quả Markdown ghi batch cố định bên trên;
publication đổi sẽ fail closed. Không dùng outputs cũ như live query cache.'''),code(SETUP)]


def ending(cells,slug,limits):
    cells += [md('## Key Findings\n\nKết luận được điền từ các phép tính đã thực thi, kèm scope và cell/query reference.',eda_findings=True),
              md('## Limitations\n\n'+limits),code(f"print(save_evidence(ROOT, '{slug}', client, findings))\nprint('Query audit:')\ndisplay(pd.DataFrame(client.evidence)[['query','row_count','query_seconds','response_bytes','batch']])")]
    # Keep validation and analytical computation in separate executable cells.
    split_markers=("overview['missing_rate_pct']=", "annual_stats=", "wb_calendar=", "daily_line=",
                   "flow=sua[", "display(df.groupby('source_area')", "display(trade.groupby('year_key')")
    expanded=[]
    for cell in cells:
        parts=[cell]
        if cell.cell_type=='code':
            positions=[cell.source.find('\n'+marker) for marker in split_markers if '\n'+marker in cell.source]
            if positions:
                cut=min(positions)
                parts=[code(cell.source[:cut]),code(cell.source[cut+1:])]
        for part in parts:
            expanded.append(part)
            if part.cell_type=='code' and ('annual_plot(' in part.source or 'plt.show()' in part.source):
                expanded.append(md('**Nguồn biểu đồ:** published TLCN Gold, batch `ec421297…`. '
                    'Exact source table/metric/series, units và time filters ở retrieval/validation cells ngay trong section này. '
                    'Không có interpolation hoặc tự chuyển source identities.'))
    cells=expanded
    import re
    for i,c in enumerate(cells):
        c['id']=f'cell-{i:03d}'
        if c.cell_type=='code':c.metadata['eda_queries']=re.findall(r"client.query\(['\"](\w+)",c.source)
    nb=nbf.v4.new_notebook(cells=cells,metadata={'kernelspec':{'display_name':'Python 3','language':'python','name':'python3'},
         'language_info':{'name':'python'},'eda_generated_by':'TLCN-EDA-01'})
    path=ROOT/'notebooks/analytics'/(slug+'.ipynb')
    if args.notebook and path.name!=args.notebook:return
    if path.exists():
        existing=nbf.read(path,as_version=4)
        if not args.refresh_generated or existing.metadata.get('eda_generated_by')!='TLCN-EDA-01':
            raise FileExistsError(f'Refuse overwrite: {path}')
    nbf.validate(nb);nbf.write(nb,path)


# 00: inventory and actual coverage, not just min/max.
cells=base('00 — Tổng quan và chất lượng dữ liệu Gold','Kiểm tra inventory, numeric coverage và các khoảng trống trước khi phân tích nghiệp vụ.')
cells += [md('## Câu hỏi 1 — Mỗi fact có bao nhiêu quan sát, NULL, zero và cảnh báo?'),code('''
from analytics.queries import PERIODS
overview = pd.DataFrame([{'fact':fact, **client.query('fact_overview', fact=fact).iloc[0].to_dict()} for fact in PERIODS])
'''),code("display(overview.shape, inspect_frame(overview), overview)"),code('''
assert len(inventory)==17 and sum(inventory.table.str.startswith('dim_'))==8
assert not overview.fact.duplicated().any()
assert (overview.rows == overview.numeric_observations + overview.missing_observations).all()
expected=inventory.set_index('table')['rows']
assert all(row.rows==expected[row.fact] for row in overview.itertuples())
overview['missing_rate_pct']=100*overview.missing_observations/overview.rows
display(overview[['fact','rows','numeric_observations','missing_rate_pct','genuine_zero','warned_observations']])
'''),code('''
fig,axes=plt.subplots(1,2,figsize=(12,5))
overview.set_index('fact').rows.sort_values().plot.barh(ax=axes[0],logx=True,color='#477998',title='Fact observations (log scale)')
overview.set_index('fact').missing_rate_pct.sort_values().plot.barh(ax=axes[1],color='#9b6a6c',title='Present rows with NULL value (%)')
for ax in axes: ax.set_ylabel('');ax.grid(axis='x',alpha=.2)
fig.tight_layout();plt.show();plt.close(fig)
'''),md('**Diễn giải:** Hai chart đo quy mô và present-NULL, không xếp hạng độ tin cậy chỉ bằng số dòng.'),
code('''
total=int(overview.rows.sum())
most=overview.sort_values('missing_rate_pct',ascending=False).iloc[0]
findings.append(dict(id='OV01',title='Numeric coverage khác nhau giữa các subject areas',
 observation=f"9 facts có {total:,} observations. {most.fact} có {int(most.missing_observations):,}/{int(most.rows):,} present-NULL ({most.missing_rate_pct:.2f}%).",
 scope='All nine published facts; count(value)/count(*)', interpretation='NULL cần được giữ trong diagnostics; không thay bằng zero hoặc so missing rates như source quality scores.',
 query='fact_overview',cell='Câu hỏi 1',numbers=overview.to_dict(orient='records')))
'''),md('## Câu hỏi 2 — Min/max có che khoảng trống thời gian không?'),code('''
coverage={fact:client.query('period_coverage',fact=fact) for fact in PERIODS}
display({k:v.shape for k,v in coverage.items()})
'''),code('''
gap_rows=[];year_rows=[]
for fact,df in coverage.items():
    assert not df.period.duplicated().any()
    observed=pd.Index(df.period.astype(int))
    kind=PERIODS[fact]
    if kind=='month_key':
        periods=pd.to_datetime(observed.astype(str),format='%Y%m').to_period('M')
        full=pd.period_range(periods.min(),periods.max(),freq='M')
        years=observed//100
    elif kind=='date_key':
        periods=pd.to_datetime(observed.astype(str),format='%Y%m%d')
        full=pd.date_range(periods.min(),periods.max(),freq='D')
        years=observed//10000
    else:
        periods=observed;full=pd.Index(range(observed.min(),observed.max()+1));years=observed
    gap_rows.append(dict(fact=fact,represented=len(periods),calendar_span=len(full),absent_periods=len(full.difference(periods))))
    year_rows += [dict(fact=fact,year=int(y)) for y in sorted(set(years))]
gaps=pd.DataFrame(gap_rows)
display(gaps)
'''),code('''
grid=pd.crosstab(pd.DataFrame(year_rows).fact,pd.DataFrame(year_rows).year).clip(upper=1)
fig,ax=plt.subplots(figsize=(12,4));ax.imshow(grid.values,aspect='auto',cmap='Blues',vmin=0,vmax=1)
ax.set_yticks(range(len(grid)),grid.index,fontsize=8)
ax.set_xticks(range(0,len(grid.columns),5),grid.columns[::5],rotation=45)
ax.set(title='Represented source periods by year (presence, not continuity of each series)',xlabel='Source period year')
fig.tight_layout();plt.show();plt.close(fig)
'''),md('**Diễn giải:** Daily gaps có thể là ngày không báo giá; không tự coi đó là ingestion failure. Coverage toàn fact không chứng minh từng series liên tục.'),
code('''
findings.append(dict(id='OV02',title='Coverage được đếm bằng represented periods',
 observation='Các khoảng trống calendar đã được tính trực tiếp từ distinct periods; xem bảng số dưới đây.',
 scope='Global fact period domain, not individual series', interpretation='Trend metrics cần kiểm tra calendar gaps ở đúng series; không suy continuity từ min/max.',
 query='period_coverage',cell='Câu hỏi 2',numbers=gaps.to_dict(orient='records')))
'''),md('## Câu hỏi 3 — Source identities, metric units và quality warnings nói gì?'),code('''
geographies=client.query('catalogue',kind='geographies')
products=client.query('catalogue',kind='products')
metrics=client.query('catalogue',kind='metrics')
series=client.query('catalogue',kind='series')
nso_domain=client.query('nso_catalogue')
flags=client.query('context_profile',fact='fact_fao_production_observation',field='flag')
releases=client.query('context_profile',fact='fact_nso_observation',field='release_status')
'''),code('''
display(geographies.groupby(['identity_namespace','source_level']).size().rename('members').to_frame())
display(products.groupby(['identity_namespace','identity_kind']).size().rename('members').to_frame())
display(metrics[['source_metric_id','gold_unit','aggregation_rule']])
display(series.groupby(['price_kind','unit','currency'],dropna=False).size().rename('series').to_frame())
display(nso_domain,flags,releases)
'''),code('''
fig,ax=plt.subplots(figsize=(8,4));metrics.groupby('gold_unit').size().sort_values().plot.barh(ax=ax,color='#477998')
ax.set(title='Supported observation types by Gold unit',xlabel='Metric definitions',ylabel='Gold unit');fig.tight_layout();plt.show();plt.close(fig)
'''),md('**Diễn giải:** Source flags và NSO release status là bằng chứng mô tả, không tự chuyển unconfirmed thành FINAL. Structural exclusions không thể suy ra từ retained Gold rows.'),
code('''
findings.append(dict(id='OV03',title='Units và identities chưa universally conformed',
 observation=f"Có {len(metrics)} metric definitions, {len(series)} price series và {geographies.identity_namespace.nunique()} geography namespaces trong inventory.",
 scope='Published dimensions',interpretation='NSO/FAO/USDA/price benchmarks cần filters riêng; ưu tiên production và từng price series có đủ numeric/calendar coverage.',
 query='catalogue',cell='Câu hỏi 3'))
''')]
ending(cells,'00_gold_data_overview','Row counts không chứng minh source quality. Không có official crosswalk, release vintages hay denominator của structural exclusions trong Gold retained facts.')


# 01: preserve NSO table identity, separate publications.
cells=base('01 — Sản xuất lúa Việt Nam theo các publication NSO','National quantity trong V06.12; reported yield V06.14 và regional observations V06.15 được giữ riêng.')
cells += [md('## Câu hỏi 1 — Sản lượng và diện tích annual trong cùng V06.12 thay đổi ra sao?'),code('''
nso=client.query('nso_observations',source_table='V06.12.csv',start_year=1990,end_year=2024,level='national')
yield_reported=client.query('nso_observations',source_table='V06.14.csv',start_year=1995,end_year=2024,level='national')
'''),code("display(nso.shape,inspect_frame(nso),nso.head(),yield_reported.shape)"),code('''
bk=['source_table','geography_key','year_key','measure','season','statistic_kind']
assert not nso.duplicated(bk).any()
quantity=nso[nso.statistic_kind.eq('quantity')].copy()
quantity['value']=pd.to_numeric(quantity.value)
annual=quantity[quantity.season.eq('annual')].copy()
assert annual.geography_key.nunique()==1
assert set(annual.unit)=={'tonne','hectare'}
production=annual[annual.measure.eq('production')].copy()
area=annual[annual.measure.eq('area')].copy()
annual_stats=pd.DataFrame([dict(measure=m,**endpoint_change(df)) for m,df in [('production_t',production),('area_ha',area)]])
display(annual_stats)
'''),code('''
annual_plot(production,'value','NSO V06.12 — Annual production','tonne')
annual_plot(area,'value','NSO V06.12 — Annual area','hectare')
'''),md('**Diễn giải:** Hai chuỗi cùng publication, national scope và annual quantity. Không cộng với regional/provincial hoặc seasonal rows.'),code('''
for row in annual_stats.to_dict(orient='records'):
    findings.append(dict(id='VN01' if row['measure']=='production_t' else 'VN02',title='Thay đổi annual '+row['measure'],
      observation=f"V06.12: {row['first_value']:,.2f} năm {row['first_year']} → {row['last_value']:,.2f} năm {row['last_year']} ({row['change_pct']:+.2f}%).",
      scope='NSO V06.12.csv, national, annual, quantity; unit in metric',interpretation='So sánh endpoints trong đúng publication; không suy causal attribution hoặc release vintage.',query='nso_observations',cell='Câu hỏi 1',numbers=row))
peak_year=int(production.loc[production.value.idxmax(),'year_key'])
post_production=endpoint_change(production[production.year_key>=peak_year])
post_area=endpoint_change(area[area.year_key>=peak_year])
assert post_production['first_year']==post_area['first_year']==peak_year
display(pd.DataFrame([dict(metric='production_t',**post_production),dict(metric='area_ha',**post_area)]))
findings.append(dict(id='VN05',title='Xu hướng dài hạn che giai đoạn sau observed peak',
 observation=f"Annual production đạt observed peak {post_production['first_value']:,.2f} tonnes năm {peak_year}; tới {post_production['last_year']} còn {post_production['last_value']:,.2f} tonnes ({post_production['change_pct']:+.2f}%). Diện tích cùng interval thay đổi {post_area['change_pct']:+.2f}%.",
 scope='NSO V06.12.csv, national annual quantity; peak selected from observed 1990–2024 series',
 interpretation='Descriptive peak-to-latest comparison; không phải formal structural-break test hoặc chứng minh area gây thay đổi production.',
 query='nso_observations',cell='Câu hỏi 1',numbers=dict(production=post_production,area=post_area)))
'''),md('## Câu hỏi 2 — YoY và association giữa sản lượng/diện tích?'),code('''
growth=calendar_yoy(production,[])
paired=production[['year_key','value']].rename(columns={'value':'production_t'}).merge(area[['year_key','value']].rename(columns={'value':'area_ha'}),on='year_key',validate='one_to_one')
paired['computed_yield_kg_ha']=1000*paired.production_t/paired.area_ha.where(paired.area_ha>0)
indexed=paired.set_index('year_key')[['production_t','area_ha','computed_yield_kg_ha']]
indexed=indexed/indexed.iloc[0]*100
display(growth[['year_key','value','previous_value','yoy_pct']],paired.head())
'''),code('''
annual_plot(growth,'yoy_pct','NSO V06.12 — YoY, exact adjacent calendar years','%')
ax=indexed.plot(figsize=(9,4),title='V06.12 within-publication indices (first common year=100)',ylabel='Index',xlabel='Year');ax.grid(alpha=.2);plt.tight_layout();plt.show();plt.close()
'''),md('**Diễn giải:** Computed yield là ratio trong cùng V06.12, không đổi thành reported yield. Index co-movement chỉ gợi association, không causal mechanism.'),
md('## Câu hỏi 3 — Reported yield và seasonal patterns?'),code('''
yr=yield_reported[(yield_reported.measure=='yield')&(yield_reported.season=='annual')&(yield_reported.statistic_kind=='quantity')].copy()
yr['value']=pd.to_numeric(yr.value);assert yr.unit.eq('kg/hectare').all()
seasons=quantity[(quantity.measure=='production')&(quantity.season!='annual')].copy()
season_stats=pd.DataFrame([dict(season=s,**endpoint_change(df)) for s,df in seasons.groupby('season')])
display(inspect_frame(yr),season_stats,nso.groupby('release_status').size())
'''),code('''
annual_plot(yr,'value','NSO V06.14 — Reported annual yield','kg/hectare')
annual_plot(seasons,'value','NSO V06.12 — Seasonal production, separate from annual','tonne',group='season')
'''),md('**Diễn giải:** Không cộng annual vào seasonal. V06.14 là publication riêng; không chọn MAX/FIRST để giải quyết khác biệt giữa publications.'),code('''
s=endpoint_change(yr)
findings.append(dict(id='VN03',title='Reported yield trong V06.14',observation=f"{s['first_value']:,.2f} → {s['last_value']:,.2f} kg/ha, {s['first_year']}–{s['last_year']} ({s['change_pct']:+.2f}%).",scope='V06.14.csv; national annual quantity',interpretation='Yield tăng/giảm là descriptive source observation; không đồng nhất với derived yield của V06.12.',query='nso_observations',cell='Câu hỏi 3',numbers=s))
latest=seasons[seasons.year_key==seasons.year_key.max()].sort_values('value',ascending=False)
winner=latest.iloc[0]
findings.append(dict(id='VN04',title='Season có reported production lớn nhất trong selected table',observation=f"Năm {int(winner.year_key)}, {winner.season}: {winner.value:,.2f} tonnes, trong {len(latest)} seasonal observations của V06.12.",scope='V06.12.csv; national seasonal quantity, no annual summation',interpretation='Đây là so sánh các season do nguồn định nghĩa, không xác nhận coverage của mọi lịch mùa vụ.',query='nso_observations',cell='Câu hỏi 3'))
'''),md('## Câu hỏi 4 — Source-defined regional categories'),code('''
regions=client.query('nso_observations',source_table='V06.15.csv',start_year=1995,end_year=2024,level='region')
regions=regions[(regions.measure=='production')&(regions.season=='annual')&(regions.statistic_kind=='quantity')].copy()
regions['value']=pd.to_numeric(regions.value);regions['source_label']=regions.source_labels_json.map(label)
latest_regions=regions[regions.year_key==regions.year_key.max()].copy()
display(regions.shape,latest_regions[['geography_key','source_label','value','unit','release_status']])
'''),code('''
fig,ax=plt.subplots(figsize=(10,4));latest_regions.set_index('source_label').value.sort_values().plot.barh(ax=ax,color='#477998')
ax.set(title='NSO V06.15 — Source-defined regional observations, latest year',xlabel='tonne',ylabel='Source label (unverified historical conformance)');fig.tight_layout();plt.show();plt.close(fig)
'''),md('**Diễn giải:** Chỉ so sánh các observations được nguồn gọi là region, không SUM thành national total và không coi candidate labels là official historical geography.')]
ending(cells,'01_vietnam_rice_production','Provisional/unconfirmed không phải FINAL. V06.12/V06.14/V06.15 không được merge publication identity. Development index % tách khỏi quantity. Không official province/region mappings hoặc causal proof.')


# 02: fixed source-comparator universe, no global total.
cells=base('02 — So sánh sản xuất gạo theo FAOSTAT source areas','Comparator universe: India, Indonesia, Viet Nam, Thailand theo exact source M49 identities; không world ranking/total.')
cells += [md('## Câu hỏi 1 — Production, area và paired yield của bốn source areas?'),code('''
geos=client.query('catalogue',kind='geographies');products=client.query('catalogue',kind='products')
identities=['356','360','704','764']  # observed catalogue codes, not a cross-provider conversion
keys=[resolve(geos,'faostat_m49',i,'geography_key') for i in identities]
product=resolve(products,'faostat_cpc','0113','product_key')
df=client.query('fao_production',geographies=keys,product=product,start_year=1961,end_year=2024)
'''),code("display(df.shape,inspect_frame(df),df.head())"),code('''
for col in ['production_t','area_ha','reported_yield_kg_ha','computed_yield_kg_ha']:df[col]=pd.to_numeric(df[col])
assert not df.duplicated(['geography_key','product_key','year_key']).any()
assert all((df[c]<=1).all() for c in ['present_5312','present_5510','present_5412'])
df['source_area']=df.geography_labels.map(label)
paired=df.production_t.notna()&df.area_ha.gt(0)
np.testing.assert_allclose(df.loc[paired,'computed_yield_kg_ha'],1000*df.loc[paired,'production_t']/df.loc[paired,'area_ha'],rtol=1e-4)
display(df.groupby('source_area')[['production_t','area_ha','computed_yield_kg_ha']].describe())
'''),code('''
annual_plot(df,'production_t','FAOSTAT CPC 0113 — Selected source-area production','tonne',group='source_area')
annual_plot(df,'computed_yield_kg_ha','FAOSTAT — Computed paired yield','kg/hectare',group='source_area')
'''),md('**Diễn giải:** Chỉ cùng source product 0113 và metric 5510/5312. Không gộp paddy với milled hoặc nguồn khác.'),code('''
stats=pd.DataFrame([dict(source_area=a,**endpoint_change(d,value='production_t')) for a,d in df.groupby('source_area')])
display(stats)
vn=df[df.geography_identity=='704']
production_change=endpoint_change(vn,value='production_t');area_change=endpoint_change(vn,value='area_ha');yield_change=endpoint_change(vn,value='computed_yield_kg_ha')
findings.append(dict(id='FAO01',title='Production, area và computed yield của Viet Nam',observation=f"{production_change['first_year']}–{production_change['last_year']}: production {production_change['change_pct']:+.2f}%, harvested area {area_change['change_pct']:+.2f}%, computed paired yield {yield_change['change_pct']:+.2f}%.",scope='FAOSTAT M49 704 / CPC 0113; source quantity metrics',interpretation='Accounting association của production/area; derived yield không là independent causal explanation.',query='fao_production',cell='Câu hỏi 1',numbers=dict(production=production_change,area=area_change,yield_change=yield_change)))
yield_changes=pd.DataFrame([dict(source_area=a,**endpoint_change(d,value='computed_yield_kg_ha')) for a,d in df.groupby('source_area')])
display(yield_changes)
best=yield_changes.sort_values('change_pct',ascending=False).iloc[0]
findings.append(dict(id='FAO02',title='Thay đổi computed yield trong selected universe',observation=f"{best.source_area}: {best.first_value:,.2f} → {best.last_value:,.2f} kg/ha ({best.change_pct:+.2f}%, {int(best.first_year)}–{int(best.last_year)}).",scope='Four explicitly selected FAOSTAT source areas; CPC 0113, paired measures',interpretation='Largest proportional change among selected endpoints, not global best performance.',query='fao_production',cell='Câu hỏi 1',numbers=yield_changes.to_dict(orient='records')))
'''),md('## Câu hỏi 2 — Common-year ranking và area/yield association'),code('''
common=set.intersection(*[set(d.loc[d.production_t.notna()&d.area_ha.gt(0),'year_key']) for _,d in df.groupby('source_area')])
assert common,'No common paired comparison year'
year=max(common);rank=df[df.year_key==year].sort_values('production_t',ascending=False)
growth=calendar_yoy(df,['geography_key','product_key'],value='production_t')
display(rank[['source_area','year_key','production_t','area_ha','computed_yield_kg_ha']],growth[['source_area','year_key','yoy_pct']].tail())
leader=rank.iloc[0]
findings.append(dict(id='FAO03',title='Common-year ranking trong bốn source areas',observation=f"Năm {year}, {leader.source_area} có {leader.production_t:,.0f} tonnes, cao nhất trong bốn source areas được chọn.",scope='Exact four-area comparator universe, common numeric year, CPC 0113',interpretation='Không phải world ranking và không chứng minh verified global total.',query='fao_production',cell='Câu hỏi 2',numbers=rank[['source_area','year_key','production_t','area_ha','computed_yield_kg_ha']].to_dict(orient='records')))
'''),code('''
fig,axes=plt.subplots(1,2,figsize=(11,4));rank.set_index('source_area').production_t.plot.bar(ax=axes[0],color='#477998',title=f'Production in {year}: selected four-area universe',ylabel='tonne')
for row in rank.itertuples():axes[1].scatter(row.area_ha,row.computed_yield_kg_ha,label=row.source_area)
axes[1].set(xlabel='Harvested area (ha)',ylabel='Computed yield (kg/ha)',title=f'Paired measures in {year}');axes[1].legend(fontsize=8)
fig.tight_layout();plt.show();plt.close(fig)
'''),md('**Diễn giải:** Ranking chỉ trong bốn source areas đã chọn. Không tính global total khi membership aggregate/country chưa có crosswalk chứng minh. Weighted yield, nếu dùng, phải ratio-of-sums trên paired non-overlapping population; notebook không tự chứng nhận global population.')]
ending(cells,'02_global_rice_production','FAOSTAT source_level chỉ là source_area; chưa có universal atomic/aggregate classifier. Không dùng nhãn hiển thị để join USDA/NSO; không world total, country share toàn cầu hoặc causal attribution.')


# 03: separate providers and time semantics.
cells=base('03 — Cung cầu và tồn kho','FAOSTAT SUA calendar years và USDA PSD marketing years được phân tích tách biệt.')
cells += [md('## Câu hỏi 1 — Supported SUA components của Viet Nam'),code('''
geos=client.query('catalogue',kind='geographies');products=client.query('catalogue',kind='products')
vn=resolve(geos,'faostat_m49','704','geography_key')
sua_options=client.query('subject_products',fact='fact_fao_sua_observation',geography=vn)
display(sua_options)  # verify actual fact-specific product/metric availability
sua_product=resolve(products,'faostat_cpc','23161.02','product_key')
assert sua_product in set(sua_options.product_key)
sua=client.query('sua',geography=vn,product=sua_product,start_year=2010,end_year=2023)
'''),code("display(sua.shape,inspect_frame(sua),sua.head())"),code('''
value_columns=['food_supply_quantity_t','production_t','import_quantity_t','export_quantity_t','opening_stocks_t','stock_variation_t','residuals_t','feed_t','seed_t','loss_t','processed_t','other_uses_t']
for col in value_columns:sua[col]=pd.to_numeric(sua[col])
assert not sua.year_key.duplicated().any()
display(sua[['year_key']+value_columns])
flow=sua[['year_key','food_supply_quantity_t','production_t','import_quantity_t','export_quantity_t']].melt('year_key',var_name='metric',value_name='value')
stock=sua[['year_key','opening_stocks_t','stock_variation_t','residuals_t']].melt('year_key',var_name='metric',value_name='value')
'''),code('''
annual_plot(flow,'value','FAOSTAT SUA — Selected flows, 23161.02','tonne',group='metric')
annual_plot(stock,'value','FAOSTAT SUA — Stock levels and signed changes (no sum across time)','tonne',group='metric')
'''),md('**Diễn giải:** 5071/5166 giữ signed; opening stocks không cộng qua năm. 12 supported codes không chứng minh một accounting identity đầy đủ.'),code('''
for identifier,col in [('SUA01','food_supply_quantity_t'),('SUA02','opening_stocks_t')]:
    stat=endpoint_change(sua,value=col)
    findings.append(dict(id=identifier,title='SUA '+col,observation=f"{stat['first_year']}–{stat['last_year']}: {stat['first_value']:,.2f} → {stat['last_value']:,.2f} tonnes ({stat['change_pct']:+.2f}%).",scope='FAOSTAT Viet Nam, 23161.02; calendar years',interpretation='Reported component trend; opening stock is not annual flow and no closing stock is invented.',query='sua',cell='Câu hỏi 1',numbers=stat))
'''),md('## Câu hỏi 2 — USDA production, consumption và ending stocks'),code('''
psd_geo=resolve(geos,'usda_psd_country','Vietnam','geography_key')
psd_product=resolve(products,'usda_psd','Rice, Milled','product_key')
psd=client.query('psd',geography=psd_geo,product=psd_product,start_year=1960,end_year=2025)
'''),code("display(psd.shape,inspect_frame(psd),psd[['metric','unit']].drop_duplicates())"),code('''
psd['value']=pd.to_numeric(psd.value)
assert not psd.duplicated(['market_year','metric']).any()
core=psd[psd.metric.isin(['Production','Domestic Consumption','Ending Stocks'])].copy()
assert core.unit.eq('tonne').all()
milling=psd[psd.metric=='Milling Rate (.9999)']
assert milling.unit.eq('fraction').all() and milling.value.between(0,1).all()
display(psd.pivot(index='market_year',columns='metric',values='value').tail(),milling.tail())
'''),code('''
annual_plot(core[core.metric!='Ending Stocks'],'value','USDA PSD — MY Production / Domestic Consumption','tonne',year='market_year',group='metric')
annual_plot(core[core.metric=='Ending Stocks'],'value','USDA PSD — MY Ending Stocks','tonne',year='market_year')
'''),md('**Diễn giải:** MY không phải calendar year; TY imports/exports và Rough Production vẫn là attributes riêng, không được cộng vào MY hoặc milled totals.'),code('''
recent=core[core.market_year>=2010]
for identifier,metric in [('PSD01','Production'),('PSD02','Ending Stocks')]:
    stat=endpoint_change(recent[recent.metric==metric],year='market_year')
    findings.append(dict(id=identifier,title='USDA MY '+metric,observation=f"MY {stat['first_year']}–{stat['last_year']}: {stat['first_value']:,.2f} → {stat['last_value']:,.2f} tonnes ({stat['change_pct']:+.2f}%).",scope='USDA PSD Vietnam / Rice, Milled; exact attribute',interpretation='Descriptive marketing-year trend; không reconcile với FAOSTAT calendar observations.',query='psd',cell='Câu hỏi 2',numbers=stat))
''')]
ending(cells,'03_rice_supply_demand','Không cross-provider balance reconciliation; không suy closing stocks SUA, không cộng stocks qua thời gian, không cộng totals với components hoặc MY với TY.')


# 04: exact series, separate grains, explicit monthly windows.
cells=base('04 — Phân tích giá gạo','Từng exact benchmark/quote series được phân tích riêng; không FX conversion, không trộn monetary prices với indices.')
cells += [md('## Câu hỏi 1 — World Bank rice_thai_5: mức giá và monthly volatility'),code('''
series=client.query('catalogue',kind='series')
selected=series[series.series_identity_json.eq('["rice_thai_5"]')]
assert len(selected)==1
wb_key=selected.iloc[0].price_series_key
wb=client.query('price_observations',fact='fact_price_monthly',series=wb_key,start_period=200001,end_period=202512)
'''),code("display(wb.shape,inspect_frame(wb),selected,wb.head())"),code('''
assert wb.price_series_key.nunique()==1 and wb.unit.eq('USD/tonne').all() and wb.currency.eq('USD').all()
wb_calendar=monthly_calendar(wb)
display(wb_calendar[['value','observed_row','change_pct','rolling_mean_12m','volatility_12m']].describe())
'''),code('''
fig,axes=plt.subplots(2,1,figsize=(10,7),sharex=True)
wb_calendar[['value','rolling_mean_12m']].plot(ax=axes[0]);axes[0].set(title='WB rice_thai_5: exact benchmark',ylabel='USD/tonne')
wb_calendar.volatility_12m.plot(ax=axes[1],color='#9b6a6c');axes[1].set(title='12 calendar-month sample std of monthly log returns; requires 13 numeric levels',ylabel='% log-return std',xlabel='Month')
fig.tight_layout();plt.show();plt.close(fig)
'''),md('**Diễn giải:** Rolling mean cần 12 numeric calendar months; volatility cần 12 returns. Gaps được giữ NaN; không forward-fill/interpolate.'),code('''
wb_numeric=wb_calendar.value.dropna();hi=wb_numeric.idxmax();lo=wb_numeric.idxmin()
findings.append(dict(id='PR01',title='High/low của WB benchmark',observation=f"rice_thai_5, 2000–2025: high {wb_numeric.max():,.2f} USD/t tại {hi:%Y-%m}; low {wb_numeric.min():,.2f} USD/t tại {lo:%Y-%m}.",scope='World Bank rice_thai_5, USD/tonne, monthly',interpretation='Observed benchmark extrema; không đại diện mọi giống/grade hay giá nội địa Việt Nam.',query='price_observations',cell='Câu hỏi 1',numbers=dict(high=float(wb_numeric.max()),low=float(wb_numeric.min()),missing=int(wb_calendar.value.isna().sum()))))
vol=wb_calendar.volatility_12m.dropna()
findings.append(dict(id='PR02',title='Window volatility cao nhất',observation=f"Highest trailing 12-month log-return sample std: {vol.max():.2f}% tại {vol.idxmax():%Y-%m}.",scope='Same WB benchmark; complete-calendar 12 returns; ddof=1',interpretation='Historical variability, không là forecast hoặc causal event attribution.',query='price_observations',cell='Câu hỏi 1'))
'''),md('## Câu hỏi 2 — Một domestic quote identity, không AVG các markets'),code('''
vn_coverage=client.query('price_coverage',fact='fact_vn_market_quote_daily')
vn_candidates=vn_coverage[vn_coverage.currency.eq('VND')&vn_coverage.unit.eq('VND/kg')].sort_values(['numeric_observations','price_series_key'],ascending=[False,True])
# Choose strongest numeric coverage, never blend quotes or hide selected identity.
vn_series=vn_candidates.iloc[0]
daily=client.query('price_observations',fact='fact_vn_market_quote_daily',series=vn_series.price_series_key,start_period=20210101,end_period=20260803)
'''),code("display(daily.shape,inspect_frame(daily),vn_series,daily.head())"),code('''
assert daily.price_series_key.nunique()==1 and not daily.period.duplicated().any()
daily['value']=pd.to_numeric(daily.value)
daily.index=pd.to_datetime(daily.period.astype(str),format='%Y%m%d')
daily_line=daily.value.reindex(pd.date_range(daily.index.min(),daily.index.max(),freq='D'))
display(daily.value.describe(),pd.DataFrame({'observed_dates':[len(daily)],'absent_calendar_days':[int(daily_line.isna().sum())]}))
'''),code('''
fig,ax=plt.subplots(figsize=(10,4));ax.plot(daily_line.index,daily_line.values,marker='.',markersize=2)
ax.set(title='One domestic quote identity — calendar gaps remain visible',ylabel='VND/kg',xlabel='Date');fig.tight_layout();plt.show();plt.close(fig)
'''),md('**Diễn giải:** Calendar days không có quote không tự là pipeline lỗi. Series giữ nguyên market/commodity/price type/unit/currency/information source; không đổi sang USD/t.'),code('''
findings.append(dict(id='PR03',title='Range của một domestic quote',observation=f"Selected exact identity có {len(daily)} quoted dates; numeric range {daily.value.min():,.2f}–{daily.value.max():,.2f} VND/kg trong {daily.index.min():%Y-%m-%d}–{daily.index.max():%Y-%m-%d}.",scope=str(vn_series.series_identity_json),interpretation='Chỉ đại diện quote identity được in; không phải mức giá trung bình thị trường hay toàn Việt Nam.',query='price_observations',cell='Câu hỏi 2'))
'''),md('## Câu hỏi 3 — FAO annual/index và USDA reference prices có thể so trực tiếp không?'),code('''
annual_coverage=client.query('price_coverage',fact='fact_fao_price_annual')
fao_series=annual_coverage[annual_coverage.price_kind.eq('producer_price_index')].sort_values(['numeric_observations','price_series_key'],ascending=[False,True]).iloc[0]
annual_index=client.query('price_observations',fact='fact_fao_price_annual',series=fao_series.price_series_key,start_period=1991,end_period=2025)
usda_series=series[series.series_identity_json.eq('["VIETNAM","5-PERCENT BROKEN","FREIGHT ON BOARD PRICE"]')].iloc[0]
reference=client.query('price_observations',fact='fact_usda_price_reference',series=usda_series.price_series_key,start_period=1985,end_period=2025)
display(annual_index.shape,inspect_frame(annual_index),fao_series,reference.shape,reference.groupby('reference_period').size())
'''),code('''
assert len(annual_index)>1 and annual_index.price_kind.eq('producer_price_index').all()
annual_index['value']=pd.to_numeric(annual_index.value)
assert not reference.duplicated(['period','reference_period']).any()
display(annual_index[['period','value','unit','price_kind']],reference.groupby('reference_period').value.agg(['size','count']))
'''),code('''
annual_plot(annual_index,'value','One FAO annual producer-price index (not a nominal price)',str(fao_series.unit),year='period')
'''),md('**Diễn giải:** Annual indices không là nominal prices; USDA reference_period không tự suy monthly calendar. Hai datasets được kiểm tra riêng, không đưa chung vào monthly price chart. Seasonality chỉ là hypothesis cần đủ năm liên tục, chưa kết luận từ chart tổng quan.')]
ending(cells,'04_rice_price_analysis','Không FX/currency conversion. Không interpolate. Benchmark movements không chứng minh causality. Chưa kiểm định seasonality, không dùng 12-month volatility làm dự báo.')


# 05: selective published trade, per-partner results, no unverified denominator.
cells=base('05 — Thương mại gạo quốc tế có bounded scope','Viet Nam reporter, CPC 23161.02 Rice, milled, export flow, 2019–2023. Không full-history semantic validation.')
cells += [md('## Câu hỏi 1 — Partner-level quantity/value và paired unit values'),code('''
geos=client.query('catalogue',kind='geographies');products=client.query('catalogue',kind='products')
reporter=resolve(geos,'faostat_m49','704','geography_key')
product=resolve(products,'faostat_cpc','23161.02','product_key')
trade_products=client.query('subject_products',fact='fact_fao_trade_observation',geography=reporter)
display(trade_products)
assert product in set(trade_products.product_key)
trade=client.query('trade',reporter=reporter,product=product,flow='export',start_year=2019,end_year=2023)
'''),code("display(trade.shape,inspect_frame(trade),trade.head())"),code('''
for col in ['quantity_t','value_usd','reported_unit_value_usd_t']:trade[col]=pd.to_numeric(trade[col])
assert not trade.duplicated(['year_key','reporter_geography_key','partner_geography_key','product_key']).any()
assert trade.reporter_geography_key.eq(reporter).all() and trade.product_key.eq(product).all()
assert trade.present_quantity.le(1).all() and trade.present_value.le(1).all()
paired=trade.quantity_t.gt(0)&trade.value_usd.notna()
np.testing.assert_allclose(trade.loc[paired,'reported_unit_value_usd_t'],trade.loc[paired,'value_usd']/trade.loc[paired,'quantity_t'],rtol=1e-3,atol=.02)
trade['partner']=trade.partner_labels.map(label)
display(trade.groupby('year_key').agg(groups=('partner','size'),null_quantity=('quantity_t',lambda x:x.isna().sum()),null_value=('value_usd',lambda x:x.isna().sum())))
'''),md('## Câu hỏi 2 — Selected partner trends (không country-total denominator)'),code('''
# Explicit reported source identities, not automatically conformed country groups.
selected_identities=['156','608','458']  # China, Philippines, Malaysia catalogue identities checked below
selected=trade[trade.partner_identity.isin(selected_identities)].copy()
display(geos[(geos.identity_namespace=='faostat_m49')&geos.source_identity.isin(selected_identities)])
assert selected.partner_identity.nunique()>=2
display(selected[['year_key','partner_identity','partner','quantity_t','value_usd','reported_unit_value_usd_t']])
'''),code('''
annual_plot(selected,'quantity_t','Viet Nam reported exports, CPC 23161.02: selected partners','tonne',group='partner')
annual_plot(selected,'value_usd','Viet Nam reported export monetary values: selected partners','USD',group='partner')
annual_plot(selected,'reported_unit_value_usd_t','Reported paired export unit values: selected partners','USD/tonne',group='partner')
'''),md('**Diễn giải:** Không cộng reporter mirror flows hoặc partner breakdown với country totals. Unit value là ratio reported value/quantity, không observed market price; không AVG row-level unit values.'),code('''
for index,(partner,d) in enumerate(selected.groupby('partner')):
    stat=endpoint_change(d,value='quantity_t')
    findings.append(dict(id=f'TR{index+1:02d}',title='Bilateral reported quantity — '+partner,
      observation=f"{stat['first_year']}–{stat['last_year']}: {stat['first_value']:,.2f} → {stat['last_value']:,.2f} tonnes ({stat['change_pct']:+.2f}%).",
      scope='Viet Nam reporter; CPC 23161.02; export; exact selected partner identity',interpretation='Bilateral reported change; không suy global exporter rank, mirror reconciliation hoặc price causality.',query='trade',cell='Câu hỏi 2',numbers=stat))
uv_series=selected[selected.partner_identity=='608']
uv=endpoint_change(uv_series,value='reported_unit_value_usd_t')
findings.append(dict(id='TR04',title='Reported unit value sang Philippines',observation=f"{uv['first_year']}–{uv['last_year']}: {uv['first_value']:,.2f} → {uv['last_value']:,.2f} USD/tonne ({uv['change_pct']:+.2f}%).",scope='Viet Nam reporter / partner M49 608 / CPC 23161.02 / export; value-quantity pairing',interpretation='Có thể phản ánh composition/contract/reporting effects; không phải independent market price hoặc causal inflation estimate.',query='trade',cell='Câu hỏi 2',numbers=uv))
'''),md('## Câu hỏi 3 — Lượng hàng và unit value có association nào đáng khảo sát?'),code('''
scatter=selected[selected.quantity_t.gt(0)&selected.reported_unit_value_usd_t.notna()].copy()
fig,ax=plt.subplots(figsize=(9,4))
for partner,d in scatter.groupby('partner'):ax.scatter(d.quantity_t,d.reported_unit_value_usd_t,label=partner)
ax.set(xlabel='Reported quantity (tonne)',ylabel='Reported unit value (USD/tonne)',title='Selected bilateral cells, 2019–2023');ax.legend(fontsize=8)
fig.tight_layout();plt.show();plt.close(fig)
display(pd.DataFrame(client.evidence)[['query','row_count','query_seconds','response_bytes']])
'''),md('**Diễn giải:** Ít bilateral-year cells, không causal claim. Partner shares/concentration được DEFER vì Gold source_area chưa có verified non-overlapping partner universe/denominator; không tạo share bằng cách bỏ World/unspecified theo cảm tính.')]
ending(cells,'05_international_rice_trade','Không global totals/rankings, mirror double count, product mixing hoặc cross-reporter price comparisons. Partner concentration cần approved universe. Bộ query chỉ bao phủ selected reporter/product/flow/5 years, không toàn 39 năm.')

print('Notebook generation done:',args.notebook or 'all six owned notebooks')
