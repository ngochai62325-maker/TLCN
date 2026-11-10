"""Reproducible PBIP/TMDL/PBIR source authoring; no proprietary PBIX rewriting."""
import json
from pathlib import Path
import uuid

from analytics.powerbi import SCHEMAS as BASE_SCHEMAS,verify_bundle
from analytics.marts.powerbi import SCHEMAS as MART_SCHEMAS

BASE='https://developer.microsoft.com/json-schemas/fabric/'
TABLES={'fact_overview.csv':'Overview','nso_national_annual.csv':'NSOAnnual','nso_national_seasonal.csv':'NSOSeasonal',
 'nso_reported_yield.csv':'NSOYield','fao_production.csv':'FAO','sua_components.csv':'SUA','usda_psd.csv':'PSD',
 'wb_benchmark.csv':'WB','usda_price_reference.csv':'USDAPrice','vn_quote.csv':'VNQuote','trade_exports.csv':'Trade'}
SCHEMAS={**BASE_SCHEMAS,**MART_SCHEMAS}
MEASURES={}


def measure(name,dax,unit,policy):
    MEASURES[name]={'dax':dax,'format':unit,'description':policy}


measure('Total Observations','SUM(Overview[rows_total])','#,0','All published facts or selected fact; count rows, including present-NULL.')
measure('Numeric Observations','SUM(Overview[numeric_rows])','#,0','COUNT(value), aggregated from full-data fact counts.')
measure('Present NULL','SUM(Overview[present_null_rows])','#,0','Rows with NULL numeric value; not absent observations.')
measure('Numeric Coverage','DIVIDE([Numeric Observations], [Total Observations])','0.00%','Ratio of count sums; blank denominator zero, never average per-fact percentages.')
for table,column,name,fmt,scope in [
 ('NSOAnnual','production_t','Annual Production','#,0','V06.12 national annual quantity tonnes'),
 ('NSOAnnual','area_ha','Annual Area','#,0','V06.12 national annual quantity hectares'),
 ('NSOYield','yield_kg_ha','Reported Yield','#,0.00','V06.14 national annual reported kg/ha'),
 ('NSOSeasonal','production_t','Seasonal Production','#,0','V06.12 source seasons, never includes annual'),
 ('FAO','production_t','FAO Production','#,0','CPC0113 selected non-overlapping source areas, tonnes'),
 ('FAO','area_ha','FAO Area','#,0','CPC0113 selected non-overlapping source areas, hectares')]:
    measure(name,f'IF(HASONEVALUE(CalendarYear[Year]), SUM({table}[{column}]))',fmt,scope+'; one calendar year only; NULL remains blank; no SUM across years.')
measure('First Production Year','MINX(FILTER(ALLSELECTED(CalendarYear[Year]), NOT ISBLANK(CALCULATE([Annual Production]))), CalendarYear[Year])','0','First selected numeric V06.12 annual year.')
measure('Latest Production Year','MAXX(FILTER(ALLSELECTED(CalendarYear[Year]), NOT ISBLANK(CALCULATE([Annual Production]))), CalendarYear[Year])','0','Latest selected numeric V06.12 annual year.')
measure('First Production','VAR y=[First Production Year] RETURN IF(NOT ISBLANK(y), CALCULATE([Annual Production], REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))','#,0','First numeric selected annual production, V06.12 tonnes.')
measure('Latest Production','VAR y=[Latest Production Year] RETURN IF(NOT ISBLANK(y), CALCULATE([Annual Production], REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))','#,0','Latest numeric selected annual production, V06.12 tonnes.')
measure('Production Change','IF([First Production Year]<[Latest Production Year], [Latest Production]-[First Production])','+#,0;-#,0;0','Requires two distinct numeric comparable years; tonnes.')
measure('Production Change Pct','IF([First Production]>0 && [First Production Year]<[Latest Production Year], DIVIDE([Production Change],[First Production]))','+0.00%;-0.00%;0.00%','Endpoint ratio; positive first denominator; full selected scope recalculated.')
measure('Production YoY','VAR y=SELECTEDVALUE(CalendarYear[Year]) VAR prev=CALCULATE([Annual Production],REMOVEFILTERS(CalendarYear),CalendarYear[Year]=y-1) VAR currentValue=[Annual Production] RETURN IF(NOT ISBLANK(y) && NOT ISBLANK(currentValue) && prev>0,DIVIDE(currentValue-prev,prev))','+0.00%;-0.00%;0.00%','Exact previous calendar year; never previous available row. Y-1 may be outside displayed selection but must exist in imported same-source data.')
measure('Latest Production YoY','VAR y=[Latest Production Year] RETURN IF(NOT ISBLANK(y),CALCULATE([Production YoY],REMOVEFILTERS(CalendarYear),CalendarYear[Year]=y))','+0.00%;-0.00%;0.00%','YoY at latest numeric selected year, exact prior year.')
measure('Peak Production','MAXX(ALLSELECTED(CalendarYear[Year]),CALCULATE([Annual Production]))','#,0','Observed numeric peak within selected V06.12 years, not a forecast.')
measure('Peak Year','VAR peak=[Peak Production] RETURN IF(NOT ISBLANK(peak), MINX(FILTER(ALLSELECTED(CalendarYear[Year]),CALCULATE([Annual Production])=peak),CalendarYear[Year]))','0','Earliest year if selected observed peaks tie.')
measure('Latest vs Peak Pct','IF([Peak Production]>0,DIVIDE([Latest Production]-[Peak Production],[Peak Production]))','+0.00%;-0.00%;0.00%','Selected endpoint vs selected observed peak, not causal attribution.')
measure('NSO Insight','IF(ISBLANK([First Production Year]),"Không có dữ liệu trong phạm vi chọn.",IF([First Production Year]=[Latest Production Year],"V06.12 · chỉ một năm có số: " & FORMAT([Latest Production Year],"0") & ". Thay đổi đầu–cuối: Không áp dụng. Yield V06.14 chỉ có từ 1995.","V06.12 · " & FORMAT([First Production Year],"0") & "–" & FORMAT([Latest Production Year],"0") & ": " & FORMAT([Production Change Pct],"+0.00%;-0.00%") & ". Đỉnh quan sát " & FORMAT([Peak Year],"0") & "; cuối kỳ so với đỉnh " & FORMAT([Latest vs Peak Pct],"+0.00%;-0.00%") & ". Không suy quan hệ nhân quả."))','@','Dynamic descriptive scope; all numeric facts come from current filters.')
measure('FAO Paired Production','IF(HASONEVALUE(CalendarYear[Year]),SUM(FAO[paired_production_t]))','#,0','Same eligible country/year population as paired area, tonnes.')
measure('FAO Paired Area','IF(HASONEVALUE(CalendarYear[Year]),SUM(FAO[paired_area_ha]))','#,0','Only pairs with numeric production and positive numeric area, hectares.')
measure('FAO Weighted Yield','DIVIDE(1000*[FAO Paired Production],[FAO Paired Area])','#,0.00','kg/ha, single calendar year, ratio of paired sums over selected non-overlapping area universe; never AVG yields.')
measure('FAO Reported Yield','IF(HASONEVALUE(CalendarYear[Year]) && HASONEVALUE(FAO[geography_key]),MAX(FAO[reported_yield_kg_ha]))','#,0.00','Source reported kg/ha, one source area/year; not combined across areas.')
measure('FAO Common Year','MAXX(FILTER(ALLSELECTED(CalendarYear[Year]), VAR n=CALCULATE(DISTINCTCOUNT(FAO[geography_key]),ALLSELECTED(FAO[area_name]),FAO[paired_valid]=TRUE()) VAR expected=CALCULATE(DISTINCTCOUNT(FAO[geography_key]),ALLSELECTED(FAO[area_name]),REMOVEFILTERS(CalendarYear)) RETURN n=expected && n>0),CalendarYear[Year])','0','Latest selected year with valid paired measures for every selected area; selected universe only.')
measure('FAO Common Production','VAR y=[FAO Common Year] RETURN IF(NOT ISBLANK(y), CALCULATE([FAO Production],REMOVEFILTERS(CalendarYear),CalendarYear[Year]=y))','#,0','Production at selected common year, not world total.')
measure('FAO Common Rank','IF(NOT ISBLANK([FAO Common Production]),RANKX(ALLSELECTED(FAO[area_name]),[FAO Common Production],,DESC,Dense))','0','Rank only among admitted selected source areas, common paired year.')
measure('FAO Scatter Year','IF(HASONEVALUE(CalendarYear[Year]), SELECTEDVALUE(CalendarYear[Year]), [FAO Common Year])','0','Active year for FAO scatter: selected single year, or common paired year when all years are selected.')
measure('FAO Scatter Area','VAR y=[FAO Scatter Year] RETURN IF(NOT ISBLANK(y), CALCULATE(SUM(FAO[area_ha]), REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))','#,0','Area at selected year or default common year.')
measure('FAO Scatter Production','VAR y=[FAO Scatter Year] RETURN IF(NOT ISBLANK(y), CALCULATE(SUM(FAO[production_t]), REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))','#,0','Production at selected year or default common year.')
measure('FAO Scatter Yield','VAR y=[FAO Scatter Year] RETURN IF(NOT ISBLANK(y), CALCULATE([FAO Weighted Yield], REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))','#,0.00','Weighted yield at selected year or default common year.')
measure('SUA Selected Value','IF(HASONEVALUE(CalendarYear[Year]) && HASONEVALUE(SUA[element_code]),MAX(SUA[value_t]))','#,0.00','One SUA calendar year/element, M49 704/CPC23161.02; NULL stays blank; no cross-element SUM.')
for code,name in [('5141','SUA Food Supply'),('5113','SUA Opening Stocks'),('5071','SUA Stock Variation'),
 ('5023','SUA Processed'),('5520','SUA Feed'),('5525','SUA Seed'),('5016','SUA Loss'),('5910','SUA Exports'),
 ('5165','SUA Other Uses'),('5166','SUA Residuals')]:
    measure(name,f'CALCULATE([SUA Selected Value],REMOVEFILTERS(SUA[element_code]),REMOVEFILTERS(SUA[metric]),SUA[element_code]="{code}")','#,0.00','SUA element '+code+'; one calendar year, tonnes; no sum over years or invented balance.')
measure('PSD Selected Value','IF(HASONEVALUE(PSD[market_year]) && HASONEVALUE(PSD[attribute]),MAX(PSD[value]))','#,0.00','One marketing year and attribute; unit from row. No mixing MY/TY/rough/milled, rates or stocks.')
for attr,name in [('Production','PSD Production'),('Domestic Consumption','PSD Consumption'),('Ending Stocks','PSD Ending Stocks')]:
    measure(name,f'CALCULATE([PSD Selected Value],REMOVEFILTERS(PSD[attribute]),PSD[attribute]="{attr}")','#,0','USDA Vietnam/Rice, Milled '+attr+' tonnes at one market year; no stock sum across time.')
measure('USDA Price','AVERAGE(USDAPrice[price_usd_t])','#,0.00','USDA FOB export price reference USD/tonne; NEVER_SUM; average only across selected periods/classes.')
measure('USDA Price Trend','AVERAGE(USDAPrice[price_usd_t])','#,0.00','USDA FOB export price reference USD/tonne over reference year.')
measure('WB Price','IF(HASONEVALUE(BenchmarkMonth[Date]),MAX(WB[price_usd_t]))','#,0.00','World Bank benchmark monthly USD/tonne level; never sum price over dates.')
measure('WB MoM','IF(HASONEVALUE(BenchmarkMonth[Date]),MAX(WB[monthly_change_pct])/100)','+0.00%;-0.00%;0.00%','Exact calendar previous month, both numeric and prior>0; absent/NULL gives blank.')
measure('WB Volatility','IF(HASONEVALUE(BenchmarkMonth[Date]),MAX(WB[volatility_12m_pct])/100,AVERAGE(WB[volatility_12m_pct])/100)','0.00%','Trailing 12 log-return sample std, requires 13 positive consecutive monthly observations; not annualized.')
measure('WB Peak','MAXX(ALLSELECTED(BenchmarkMonth[Date]),CALCULATE([WB Price]))','#,0.00','Observed maximum benchmark in selected months, USD/tonne.')
measure('WB Minimum','MINX(FILTER(ALLSELECTED(BenchmarkMonth[Date]),NOT ISBLANK(CALCULATE([WB Price]))),CALCULATE([WB Price]))','#,0.00','Observed numeric minimum selected monthly benchmark, USD/tonne.')
measure('WB Numeric Months','COALESCE(CALCULATE(COUNTROWS(WB),WB[numeric_present]=TRUE()),0)','#,0','Numeric monthly prices in selected calendar; no imputation.')
measure('WB Absent Months','COALESCE(CALCULATE(COUNTROWS(WB),WB[observation_present]=FALSE()),0)','#,0','Count of calendar rows with no source observation; zero is an empty count, not an imputed price.')
measure('Domestic Quote','IF(HASONEVALUE(QuoteDate[Date]),MAX(VNQuote[price_vnd_kg]))','#,0','Vietnam domestic quote VND/kg on selected date or average quote.')
measure('Domestic Quoted Dates','COUNTROWS(VNQuote)','#,0','Observed rows in selected date scope, including present-NULL.')
for col,name,policy in [('quantity_t','Trade Quantity','Reported export tonnes'),('value_usd','Trade Value','Reported export USD'),('paired_quantity_t','Trade Paired Quantity','Numeric quantity paired with numeric monetary value and q>0'),('paired_value_usd','Trade Paired Value','Same eligible contributor population as paired quantity')]:
    measure(name,f'SUM(Trade[{col}])','#,0.00',policy+'; reporter704/CPC23161.02/export/2019–2023. Additive over disjoint selected partner-year groups; blanks retained.')
measure('Trade Unit Value','DIVIDE([Trade Paired Value],[Trade Paired Quantity])','#,0.00','USD/tonne = ratio of paired sums under current year/partner selection; never average row ratios; positive paired denominator.')
measure('Trade Paired Groups','COALESCE(COUNTROWS(FILTER(Trade,NOT ISBLANK(Trade[paired_quantity_t]) && NOT ISBLANK(Trade[paired_value_usd]))),0)','#,0','Eligible paired reporter-partner-product-year groups; coverage of selected partner scope.')
measure('Trade Insight','IF([Trade Paired Groups]=0,"Không có cặp đủ điều kiện trong phạm vi chọn.","Việt Nam → " & CONCATENATEX(VALUES(Trade[partner_name]),Trade[partner_name],", ") & " · " & FORMAT(MIN(Trade[year]),"0") & "–" & FORMAT(MAX(Trade[year]),"0") & " · CPC23161.02. " & FORMAT([Trade Paired Groups],"0") & " nhóm đủ cặp; đơn giá " & FORMAT([Trade Unit Value],"#,0.00") & " USD/t. Tỷ số giá trị/lượng báo cáo; không phải giá thị trường độc lập.")','@','Dynamic paired coverage and ratio; no global denominator.')

CARD_NAMES=('Total Observations','Numeric Observations','Present NULL','Numeric Coverage',
    'First Production','Latest Production','Production Change Pct','Latest Production YoY')
for name in CARD_NAMES:
    fmt=MEASURES[name]['format']
    measure(name+' Display',f'IF(ISBLANK([{name}]),"Không áp dụng",FORMAT([{name}],"{fmt}"))','@',
        'Presentation only; BLANK is labelled Không áp dụng, numeric measure remains available for calculations.')

measure('Trade Quantity Display','IF(ISBLANK([Trade Quantity]),"Không áp dụng",FORMAT([Trade Quantity]/1000000,"#,0.00") & " tr tấn")','@','Formatted export quantity in million tonnes.')
measure('Trade Value Display','IF(ISBLANK([Trade Value]),"Không áp dụng",FORMAT([Trade Value]/1000000000,"$#,0.00") & " tỷ USD")','@','Formatted export value in billion USD.')
measure('Trade Unit Value Display','IF(ISBLANK([Trade Unit Value]),"Không áp dụng",FORMAT([Trade Unit Value],"$#,0.00") & " /tấn")','@','Formatted unit value USD/tonne.')
measure('Trade Paired Groups Display','IF(ISBLANK([Trade Paired Groups]),"Không áp dụng",FORMAT([Trade Paired Groups],"#,0") & " nhóm")','@','Formatted paired groups count.')


def write_json(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')


def quoted(name):return "'"+name.replace("'","''")+"'"


def tmdl_table(name,schema,partition,source,measures=None):
    lines=[f'table {name}']
    for col,kind in schema:
        dtype='dateTime' if col in ('month_date','date','Date') else {'integer':'int64','decimal':'double','boolean':'boolean'}.get(kind.rstrip('?'),'string')
        lines.extend([f'\tcolumn {col}',f'\t\tdataType: {dtype}','\t\tsummarizeBy: none',f'\t\tsourceColumn: {col}'])
        if dtype=='dateTime':lines.append('\t\tformatString: yyyy-MM-dd')
    for label,definition in (measures or {}).items():
        lines.extend([f'\t/// {definition["description"]}',f'\tmeasure {quoted(label)} = {definition["dax"]}',f'\t\tformatString: {definition["format"]}'])
    lines.extend([f'\tpartition {name} = {partition}','\t\tmode: import','\t\tsource ='])
    lines.extend('\t\t\t'+line for line in source.splitlines())
    return '\n'.join(lines)+'\n'


def m_import(filename,schema):
    types=[]
    for name,kind in schema:
        dtype='type date' if name in ('month_date','date') else {'integer':'Int64.Type','decimal':'type number','boolean':'type logical'}.get(kind.rstrip('?'),'type text')
        types.append('{"'+name+'", '+dtype+'}')
    return ('let\n    Raw = Csv.Document(File.Contents(ExportFolder & "\\'+filename+'"), [Delimiter=",", Columns='+str(len(schema))+', Encoding=65001, QuoteStyle=QuoteStyle.Csv]),\n'
        '    Headers = Table.PromoteHeaders(Raw, [PromoteAllScalars=true]),\n'
        '    Checked = if Table.ColumnNames(Headers) = {'+', '.join(json.dumps(c) for c,_ in schema)+'} then Headers else error "CSV schema mismatch",\n'
        '    Missing = Table.ReplaceValue(Checked, "", null, Replacer.ReplaceValue, Table.ColumnNames(Checked)),\n'
        '    Typed = Table.TransformColumnTypes(Missing, {'+', '.join(types)+'}, "en-US"),\n'
        '    Manifest = Json.Document(File.Contents(ExportFolder & "\\export_manifest.json")),\n'
        '    Entry = Record.Field(Manifest[outputs], "'+filename+'"),\n'
        '    Accepted = if Manifest[status] = "COMPLETE" and Manifest[format_version] = "powerbi-csv-2" and Table.RowCount(Typed) = Entry[rows] then Typed else error "Verify the immutable mart bundle before refresh"\nin\n    Accepted')


def field(table,name,is_measure=False):return {('Measure' if is_measure else 'Column'):{'Expression':{'SourceRef':{'Entity':table}},'Property':name}}


def literal(value):
    if isinstance(value,bool):text='true' if value else 'false'
    elif isinstance(value,(int,float)):text=str(value)+'D'
    else:text="'"+value.replace("'","''")+"'"
    return {'expr':{'Literal':{'Value':text}}}


def visual(identifier,kind,title,x,y,w,h,roles=None):
    config={'visualType':kind,'drillFilterOtherVisuals':True,
        'visualContainerObjects':{'title':[{'properties':{'show':literal(True),'text':literal(title),'fontSize':literal(12),'fontColor':{'solid':{'color':literal('#17342F')}}}}],
          'background':[{'properties':{'show':literal(True),'color':{'solid':{'color':literal('#FFFFFF')}},'transparency':literal(0)}}]}}
    if roles:
        config['query']={'queryState':{role:{'projections':[{'field':field(t,n,m),'queryRef':t+'.'+n,'displayName':label} for t,n,m,label in columns]} for role,columns in roles.items()}}
    if kind=='card' and roles:
        name=next(iter(roles.values()))[0][1]
        fmt=MEASURES.get(name,{}).get('format','')
        config['objects']={'labels':[{'properties':{'labelDisplayUnits':literal(1),'labelPrecision':literal(2 if '.00' in fmt else 0),'fontSize':literal(24 if fmt!='@' or name.endswith(' Display') else 11)}}], 'categoryLabels':[{'properties':{'show':literal(False)}}]}
    if kind=='slicer':
        config['objects']={'data':[{'properties':{'mode':literal('Dropdown')}}],
                           'selection':[{'properties':{'singleSelect':literal(False),'selectAllCheckboxEnabled':literal(True)}}]}
    return {'$schema':BASE+'item/report/definition/visualContainer/2.0.0/schema.json','name':identifier,
        'position':{'x':x,'y':y,'z':int(identifier[1:]),'width':w,'height':h,'tabOrder':int(identifier[1:])},'visual':config}


def pages():
    """Native visual specifications; page-scoped filters never join incompatible time."""
    col=lambda t,n,label:(t,n,False,label)
    ms=lambda n,label:('Overview',n,True,label)
    specs=[]
    def page(title,note):
        p={'title':title,'note':note,'visuals':[]};specs.append(p);return p
    def add(p,kind,title,pos,**roles):p['visuals'].append((kind,title,pos,roles))
    def card(p,name,label,x):add(p,'card',label,(x,120,290,90),Values=[ms(name+' Display',label)])

    # Page 1: Overview
    p=page('Tổng quan dữ liệu lúa gạo','Từ Gold đã publish · Present-NULL là quan sát có giá trị thiếu. Phạm vi thời gian của cả fact không chứng minh liên tục cho từng series.')
    for i,(n,l) in enumerate([('Total Observations','Tổng quan sát'),('Numeric Observations','Quan sát có số'),('Present NULL','Quan sát NULL'),('Numeric Coverage','Tỷ lệ có số')]):card(p,n,l,32+i*304)
    add(p,'barChart','Số quan sát theo bộ dữ liệu',(32,220,590,225),Category=[col('Overview','fact_name','Bộ dữ liệu')],Y=[ms('Total Observations','Quan sát')])
    add(p,'hundredPercentStackedBarChart','Tỷ lệ có số và thiếu theo bộ dữ liệu',(638,220,610,225),Category=[col('Overview','fact_name','Bộ dữ liệu')],Y=[ms('Numeric Observations','Có số'),ms('Present NULL','NULL')])
    add(p,'tableEx','Danh mục và phạm vi thời gian được biểu diễn',(32,456,1216,192),Values=[col('Overview',n,l) for n,l in [('fact_name','Bộ dữ liệu'),('period_grain','Grain thời gian'),('period_min','Kỳ đầu'),('period_max','Kỳ cuối'),('represented_periods','Số kỳ hiện diện')]]+[ms('Numeric Coverage','Tỷ lệ có số')])

    # Page 2: Vietnam Production
    p=page('Sản xuất lúa Việt Nam','NSO V06.12 quốc gia/quantity; annual và seasonal tách riêng. Yield báo cáo V06.14, không suy ra từ V06.12. Provisional/unconfirmed không tự trở thành FINAL.')
    for i,(n,l) in enumerate([('First Production','Sản lượng kỳ đầu (tấn)'),('Latest Production','Sản lượng kỳ cuối (tấn)'),('Production Change Pct','Thay đổi đầu–cuối'),('Latest Production YoY','YoY kỳ cuối')]):card(p,n,l,32+i*304)
    add(p,'slicer','Năm báo cáo',(1040,70,208,42),Values=[col('CalendarYear','Year','Năm')])
    for pos,name,title in [((32,220,390,180),'Annual Production','Sản lượng annual (tấn)'),((438,220,390,180),'Annual Area','Diện tích annual (ha)'),((844,220,404,180),'Reported Yield','Yield V06.14 (kg/ha)')]:add(p,'lineChart',title,pos,Category=[col('CalendarYear','Year','Năm')],Y=[ms(name,title)])
    add(p,'lineChart','Sản lượng theo vụ · không gồm annual',(32,412,796,195),Category=[col('CalendarYear','Year','Năm')],Series=[col('NSOSeasonal','season_label_vi','Vụ')],Y=[ms('Seasonal Production','Tấn')])
    add(p,'card','Nhận xét theo phạm vi chọn',(844,412,404,195),Values=[ms('NSO Insight','Nhận xét')])

    # Page 3: International Production
    p=page('Sản xuất lúa gạo quốc tế','FAOSTAT CPC 0113 · các nước sản xuất lớn (Ấn Độ, Indonesia, Việt Nam, Thái Lan, Pakistan). Yield là tỷ số cặp đủ dữ liệu.')
    add(p,'slicer','Năm',(860,120,180,55),Values=[col('CalendarYear','Year','Năm')]);add(p,'slicer','Khu vực nguồn',(1056,120,192,55),Values=[col('FAO','area_name','Khu vực')])
    for pos,name,title in [((32,192,590,195),'FAO Production','Sản lượng nguồn (tấn)'),((638,192,610,195),'FAO Weighted Yield','Năng suất cặp hợp lệ (kg/ha)')]:add(p,'lineChart',title,pos,Category=[col('CalendarYear','Year','Năm')],Series=[col('FAO','area_name','Khu vực nguồn')],Y=[ms(name,title)])
    add(p,'barChart','Sản lượng tại năm chung trong nhóm được chọn',(32,404,590,215),Category=[col('FAO','area_name','Khu vực')],Y=[ms('FAO Common Production','Tấn')],Tooltips=[ms('FAO Common Year','Năm chung'),ms('FAO Common Rank','Hạng trong nhóm')])
    add(p,'scatterChart','Diện tích và sản lượng · năm chọn hoặc năm chung',(638,404,610,215),Category=[col('FAO','area_name','Khu vực')],X=[ms('FAO Scatter Area','Diện tích ha')],Y=[ms('FAO Scatter Production','Sản lượng tấn')],Size=[ms('FAO Scatter Yield','Năng suất kg/ha')],Tooltips=[ms('FAO Scatter Year','Năm')])

    # Page 4: Supply-Demand & Stocks
    p=page('Cung cầu và tồn kho lúa gạo','Trái: FAOSTAT SUA calendar-year M49 704/CPC23161.02. Phải: USDA Vietnam/Rice, Milled theo marketing year. Không cộng stocks qua năm hoặc ghép hai cơ sở thời gian.')
    add(p,'slicer','Năm FAOSTAT',(32,120,220,55),Values=[col('CalendarYear','Year','Năm calendar')]);add(p,'slicer','Marketing year USDA',(790,120,458,55),Values=[col('PSD','market_year','Marketing year USDA')])
    add(p,'lineChart','SUA: Tiêu thụ thực phẩm & chế biến (tấn)',(32,192,590,195),Category=[col('CalendarYear','Year','Năm')],Y=[ms('SUA Food Supply','Thực phẩm'),ms('SUA Processed','Chế biến'),ms('SUA Loss','Hao hụt'),ms('SUA Feed','Thức ăn'),ms('SUA Seed','Giống')])
    add(p,'lineChart','USDA sản xuất và tiêu dùng (tấn, MY)',(638,192,610,195),Category=[col('PSD','market_year','Marketing year')],Y=[ms('PSD Production','Sản xuất'),ms('PSD Consumption','Tiêu dùng')])
    add(p,'lineChart','SUA: Tồn đầu kỳ, biến động tồn & hao hụt (tấn)',(32,404,590,215),Category=[col('CalendarYear','Year','Năm')],Y=[ms('SUA Opening Stocks','Tồn đầu kỳ'),ms('SUA Stock Variation','Biến động tồn'),ms('SUA Residuals','Phần dư')])
    add(p,'lineChart','USDA tồn cuối kỳ (tấn, MY)',(638,404,610,215),Category=[col('PSD','market_year','Marketing year')],Y=[ms('PSD Ending Stocks','Tồn cuối kỳ')])

    # Page 5: Prices (USDA Export FOB, World Bank Benchmark, Domestic Quotes)
    p=page('Biến động giá lúa gạo','Giá FOB USDA 4 nước (Thái Lan, Việt Nam, Ấn Độ, Pakistan); Benchmark World Bank gạo Thái & VN; Giá nội địa ĐBSCL theo tỉnh/loại lúa.')
    add(p,'slicer','Quốc gia FOB USDA',(32,120,290,55),Values=[col('USDAPrice','location','Nước xuất khẩu')])
    add(p,'slicer','Kỳ USDA',(338,120,284,55),Values=[col('USDAPrice','reference_period','Kỳ tham chiếu')])
    add(p,'slicer','Chuỗi giá World Bank',(638,120,290,55),Values=[col('WB','series_code','Chuỗi giá WB')])
    add(p,'slicer','Thị trường nội địa VN',(944,120,304,55),Values=[col('VNQuote','market','Thị trường')])
    add(p,'lineChart','Giá xuất khẩu FOB USDA (USD/tấn)',(32,192,590,195),Category=[col('USDAPrice','reference_year','Năm tham chiếu')],Series=[col('USDAPrice','location','Quốc gia')],Y=[ms('USDA Price','USD/tấn')])
    add(p,'lineChart','Benchmark giá gạo World Bank (USD/tấn)',(638,192,610,195),Category=[col('BenchmarkMonth','Date','Tháng')],Series=[col('WB','series_code','Chuỗi giá')],Y=[ms('WB Price','USD/tấn')],Tooltips=[ms('WB MoM','MoM'),ms('WB Peak','Đỉnh phạm vi'),ms('WB Minimum','Đáy phạm vi')])
    add(p,'lineChart','Giá nội địa Việt Nam (VND/kg)',(32,404,590,215),Category=[col('QuoteDate','Date','Ngày')],Series=[col('VNQuote','market','Thị trường')],Y=[ms('Domestic Quote','VND/kg')])
    add(p,'lineChart','Thay đổi & biến động giá World Bank (%)',(638,404,610,215),Category=[col('BenchmarkMonth','Date','Tháng')],Y=[ms('WB MoM','MoM'),ms('WB Volatility','Volatility')])

    # Page 6: Trade Exports
    p=page('Thương mại lúa gạo quốc tế','FAOSTAT · Việt Nam reporter 704 xuất khẩu sang các đối tác lớn · CPC 23161.02 · 2019–2023. Đơn giá = ratio-of-sums trên cặp đủ điều kiện.')
    for i,(n,l) in enumerate([('Trade Quantity','Lượng xuất khẩu'),('Trade Value','Giá trị xuất khẩu'),('Trade Unit Value','Đơn giá bình quân'),('Trade Paired Groups','Số nhóm đối tác')]):card(p,n,l,32+i*304)
    add(p,'slicer','Năm',(860,70,180,42),Values=[col('CalendarYear','Year','Năm')]);add(p,'slicer','Đối tác',(1056,70,192,42),Values=[col('Trade','partner_name','Đối tác')])
    add(p,'lineChart','Lượng xuất khẩu theo đối tác (tấn)',(32,220,590,195),Category=[col('CalendarYear','Year','Năm')],Series=[col('Trade','partner_name','Đối tác')],Y=[ms('Trade Quantity','Tấn')])
    add(p,'lineChart','Đơn giá báo cáo theo đối tác (USD/tấn)',(638,220,610,195),Category=[col('CalendarYear','Year','Năm')],Series=[col('Trade','partner_name','Đối tác')],Y=[ms('Trade Unit Value','USD/tấn')])
    add(p,'lineChart','Giá trị xuất khẩu theo năm và đối tác (USD)',(32,430,590,185),Category=[col('CalendarYear','Year','Năm')],Series=[col('Trade','partner_name','Đối tác')],Y=[ms('Trade Value','USD')])
    add(p,'card','Phạm vi và diễn giải',(638,430,610,185),Values=[ms('Trade Insight','Nhận xét')])
    return specs


def build_project(root,bundle):
    root=Path(root).resolve();bundle=Path(bundle).resolve();manifest=verify_bundle(bundle)
    if manifest['format_version']!='powerbi-csv-2':raise ValueError('Report requires a verified mart bundle')
    model=root/'M';report=root/'R';definition=model/'definition';definition.mkdir(parents=True,exist_ok=True)
    write_json(root/'TLCN_Rice_Analytics.pbip',{'$schema':BASE+'pbip/pbipProperties/1.0.0/schema.json','version':'1.0','artifacts':[{'report':{'path':'R'}}],'settings':{'enableAutoRecovery':True}})
    write_json(model/'definition.pbism',{'$schema':BASE+'item/semanticModel/definitionProperties/1.0.0/schema.json','version':'4.0','settings':{}})
    write_json(report/'definition.pbir',{'$schema':BASE+'item/report/definitionProperties/2.0.0/schema.json','version':'4.0','datasetReference':{'byPath':{'path':'../M'}}})
    (definition/'database.tmdl').write_text('database TLCN_Rice_Analytics\n\tcompatibilityLevel: 1600\n',encoding='utf-8')
    (definition/'model.tmdl').write_text('model Model\n\tculture: vi-VN\n\tdefaultPowerBIDataSourceVersion: powerBI_V3\n\tsourceQueryCulture: en-US\n\tdiscourageImplicitMeasures\n\tmaxParallelismPerRefresh: 1\n\tannotation PBI_ProTooling = ["DevMode"]\n\tannotation __PBI_TimeIntelligenceEnabled = 0\n',encoding='utf-8')
    path=str(bundle).replace('"','""')
    (definition/'expressions.tmdl').write_text('expression ExportFolder = "'+path+'" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]\n\tkind: m\n',encoding='utf-8')
    tables=definition/'tables';tables.mkdir(exist_ok=True)
    for filename,name in TABLES.items():
        (tables/(name+'.tmdl')).write_text(tmdl_table(name,SCHEMAS[filename],'m',m_import(filename,SCHEMAS[filename]),MEASURES if name=='Overview' else None),encoding='utf-8')
    calendar='let ys=List.Combine({NSOAnnual[year],FAO[year],SUA[year],Trade[year]}), rows=Table.FromList({List.Min(ys)..List.Max(ys)},Splitter.SplitByNothing(),{"Year"}), typed=Table.TransformColumnTypes(rows,{{"Year",Int64.Type}}) in typed'
    (tables/'CalendarYear.tmdl').write_text(tmdl_table('CalendarYear',[('Year','integer')],'m',calendar),encoding='utf-8')
    for name,base,col in [('BenchmarkMonth','WB','month_date'),('QuoteDate','VNQuote','date')]:
        source='let lo=List.Min(VNQuote[date]), hi=List.Max(VNQuote[date]), rows=Table.FromList(List.Dates(lo,Duration.Days(hi-lo)+1,#duration(1,0,0,0)),Splitter.SplitByNothing(),{"Date"}) in Table.TransformColumnTypes(rows,{{"Date",type date}})' if name=='QuoteDate' else 'Table.RenameColumns(Table.Distinct(Table.SelectColumns(WB,{"month_date"})),{{"month_date","Date"}})'
        (tables/(name+'.tmdl')).write_text(tmdl_table(name,[('Date','text')],'m',source),encoding='utf-8')
    relations=[]
    for name in ('NSOAnnual','NSOSeasonal','NSOYield','FAO','SUA','Trade'):
        relations.extend([f'relationship {uuid.uuid5(uuid.NAMESPACE_URL,"tlcn/"+name)}',f'\tfromColumn: {name}.year','\ttoColumn: CalendarYear.Year','\tcrossFilteringBehavior: oneDirection',''])
    for name,src,col in [('BenchmarkMonth','WB','month_date'),('QuoteDate','VNQuote','date')]:relations.extend([f'relationship {uuid.uuid5(uuid.NAMESPACE_URL,"tlcn/"+name)}',f'\tfromColumn: {src}.{col}',f'\ttoColumn: {name}.Date','\tcrossFilteringBehavior: oneDirection',''])
    (definition/'relationships.tmdl').write_text('\n'.join(relations),encoding='utf-8')
    rd=report/'definition';rd.mkdir(parents=True,exist_ok=True)
    write_json(rd/'version.json',{'$schema':BASE+'item/report/definition/versionMetadata/1.0.0/schema.json','version':'2.0.0'})
    theme={'name':'TLCN Teal','dataColors':['#136F63','#3274A1','#D69832','#8E649A','#50A897','#C65D5B'],'background':'#F4F7F6','foreground':'#17342F','tableAccent':'#136F63'}
    write_json(root/'theme.json',theme)
    write_json(report/'StaticResources/RegisteredResources/TLCN.json',theme)
    write_json(rd/'report.json',{'$schema':BASE+'item/report/definition/report/2.0.0/schema.json','themeCollection':{'baseTheme':{'name':'CY24SU06','reportVersionAtImport':'5.55','type':'SharedResources'},'customTheme':{'name':'TLCN.json','reportVersionAtImport':'5.55','type':'RegisteredResources'}},'resourcePackages':[{'name':'RegisteredResources','type':'RegisteredResources','items':[{'name':'TLCN.json','path':'TLCN.json','type':'CustomTheme'}]}],'settings':{'defaultFilterActionIsDataFilter':True,'useStylableVisualContainerHeader':True,'exportDataMode':'AllowSummarized'}})
    page_ids=[];inventory=[]
    for i,p in enumerate(pages(),1):
        pid=f'p{i:02}';page_ids.append(pid);pd=rd/'pages'/pid
        page_data={'$schema':BASE+'item/report/definition/page/2.0.0/schema.json','name':pid,'displayName':f'{i:02} · '+p['title'],'displayOption':'FitToPage','width':1280,'height':720}
        if i in (2,4,6):
            lo,hi={2:(1990,2024),4:(2010,2023),6:(2019,2023)}[i]
            page_data['filterConfig']={'filters':[{'name':pid+'_eligible_years','field':field('CalendarYear','Year'),'type':'Categorical','filter':{'Version':2,'From':[{'Name':'y','Entity':'CalendarYear','Type':0}],'Where':[{'Condition':{'In':{'Expressions':[{'Column':{'Expression':{'SourceRef':{'Source':'y'}},'Property':'Year'}}],'Values':[[{'Literal':{'Value':str(y)+'L'}}] for y in range(lo,hi+1)]}}}]},'isLockedInViewMode':True}]}
        write_json(pd/'page.json',page_data)
        # v01: Title with increased 52px height and 20pt font to prevent clipping Vietnamese descenders and diacritics
        all_visuals=[visual('v01','textbox','',32,10,1216,52),visual('v02','pageNavigator','',32,64,770,42)]
        all_visuals[0]['visual']['objects']={'general':[{'properties':{'paragraphs':[{'textRuns':[{'value':p['title'],'textStyle':{'fontFamily':'Segoe UI','fontSize':'20pt','fontWeight':'bold','color':'#136F63'}}]}]}}]}
        for j,(kind,title,pos,roles) in enumerate(p['visuals'],3):all_visuals.append(visual(f'v{j:02}',kind,title,*pos,roles))
        for v in all_visuals:
            if v['visual']['visualType']=='slicer':
                v['position']['height']=72
                if v['position']['y']<120:v['position']['y']=28;v['position']['height']=88
        note=visual('v90','textbox','',32,654,1216,56)
        note['visual']['objects']={'general':[{'properties':{'paragraphs':[{'textRuns':[{'value':p['note']+' · Batch '+manifest['published_batch'][:12]+' · Export '+manifest['export_timestamp'][:10],'textStyle':{'fontFamily':'Segoe UI','fontSize':'10pt','color':'#435B55'}}]}]}}]}
        all_visuals.append(note)
        for v in all_visuals:write_json(pd/'visuals'/v['name']/'visual.json',v)
        inventory.append({'page':pid,'title':p['title'],'visuals':len(all_visuals),'note':p['note']})
    write_json(rd/'pages/pages.json',{'$schema':BASE+'item/report/definition/pagesMetadata/1.0.0/schema.json','pageOrder':page_ids,'activePageName':'p01'})
    write_json(root/'report_inventory.json',{'project':'TLCN_Rice_Analytics','batch':manifest['published_batch'],'bundle_format':manifest['format_version'],'pages':inventory,'measures':MEASURES,'tables':TABLES,'desktop_validation':'PENDING'})
    return root/'TLCN_Rice_Analytics.pbip'
