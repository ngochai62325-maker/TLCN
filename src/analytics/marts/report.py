"""Reproducible PBIP/TMDL/PBIR source authoring; no proprietary PBIX rewriting."""
import json
from pathlib import Path
import uuid

from analytics.powerbi import SCHEMAS as BASE_SCHEMAS, verify_bundle
from analytics.marts.powerbi import SCHEMAS as MART_SCHEMAS

BASE = 'https://developer.microsoft.com/json-schemas/fabric/'
TABLES = {
    'fact_overview.csv': 'Overview',
    'nso_national_annual.csv': 'NSOAnnual',
    'nso_national_seasonal.csv': 'NSOSeasonal',
    'nso_reported_yield.csv': 'NSOYield',
    'fao_production.csv': 'FAO',
    'sua_components.csv': 'SUA',
    'usda_psd.csv': 'PSD',
    'wb_benchmark.csv': 'WB',
    'usda_price_reference.csv': 'USDAPrice',
    'vn_quote.csv': 'VNQuote',
    'trade_exports.csv': 'Trade'
}

# Calculated / Added columns for Power Query M and TMDL schema
TABLE_EXTRA_COLUMNS = {
    'fact_overview.csv': [
        ('null_pct', 'each [present_null_rows] / [rows_total]', 'type number', 'decimal?'),
        ('fact_name_vi', 'each if [fact_name] = "fact_usda_price_reference" then "USDA · Giá xuất khẩu tham chiếu" else if [fact_name] = "fact_fao_trade_observation" then "FAOSTAT · Thương mại xuất nhập khẩu" else if [fact_name] = "fact_fao_production_observation" then "FAOSTAT · Sản lượng lúa toàn cầu" else if [fact_name] = "fact_fao_sua_observation" then "FAOSTAT SUA · Cân đối cung sử dụng" else if [fact_name] = "fact_usda_psd_observation" then "USDA PSD · Cân đối cung cầu niên vụ" else if [fact_name] = "fact_nso_observation" then "NSO · Sản xuất lúa Việt Nam" else if [fact_name] = "fact_fao_price_annual" then "FAOSTAT · Giá sản xuất hằng năm" else if [fact_name] = "fact_vn_market_quote_daily" then "ĐBSCL · Báo giá lúa nội địa" else if [fact_name] = "fact_price_monthly" then "World Bank · Benchmark giá tháng" else [fact_name]', 'type text', 'text'),
        ('period_grain_vi', 'each if [period_grain] = "calendar_year" then "Năm dương lịch" else if [period_grain] = "reference_year" then "Năm tham chiếu" else if [period_grain] = "marketing_year" then "Niên vụ thương mại" else if [period_grain] = "calendar_month" then "Tháng dương lịch" else if [period_grain] = "calendar_date" then "Ngày dương lịch" else [period_grain]', 'type text', 'text')
    ],
    'fao_production.csv': [
        ('area_name_vi', 'each if [area_name] = "India" then "Ấn Độ" else if [area_name] = "Indonesia" then "Indonesia" else if [area_name] = "Pakistan" then "Pakistan" else if [area_name] = "Thailand" then "Thái Lan" else if [area_name] = "Viet Nam" then "Việt Nam" else [area_name]', 'type text', 'text')
    ],
    'wb_benchmark.csv': [
        ('series_name_vi', 'each if [series_code] = "rice_thai_5" then "Gạo Thái 5% tấm" else if [series_code] = "rice_thai_25" then "Gạo Thái 25% tấm" else if [series_code] = "rice_thai_a_1" then "Gạo Thái A1 đặc biệt" else if [series_code] = "rice_viet_namese_5" then "Gạo Việt Nam 5% tấm" else [series_code]', 'type text', 'text')
    ],
    'trade_exports.csv': [
        ("partner_name_vi", 'each if [partner_name] = "China, mainland" then "Trung Quốc" else if [partner_name] = "Côte d\'Ivoire" then "Bờ Biển Ngà" else if [partner_name] = "Ghana" then "Ghana" else if [partner_name] = "Indonesia" then "Indonesia" else if [partner_name] = "Malaysia" then "Malaysia" else if [partner_name] = "Philippines" then "Philippines" else if [partner_name] = "Singapore" then "Singapore" else [partner_name]', "type text", "text"),
    ],
    'usda_price_reference.csv': [
        ('location_vi', 'each if [location] = "INDIA" then "Ấn Độ" else if [location] = "PAKISTAN" then "Pakistan" else if [location] = "THAILAND" then "Thái Lan" else if [location] = "VIETNAM" then "Việt Nam" else [location]', 'type text', 'text'),
        ('commodity_class_vi', 'each if [commodity_class] = "100-PERCENT GRADE B" then "Gạo 100% loại B" else if [commodity_class] = "5-PERCENT BROKEN" then "Gạo 5% tấm" else if [commodity_class] = "5-PERCENT PARBOILED" then "Gạo 5% đồ" else if [commodity_class] = "FRAGRANT WHITE RICE" then "Gạo thơm hạt dài" else [commodity_class]', 'type text', 'text')
    ],
    'vn_quote.csv': [
        ('series_label', 'each [commodity] & " · " & [price_type]', 'type text', 'text')
    ]
}

CSV_SCHEMAS = {**BASE_SCHEMAS, **MART_SCHEMAS}
SCHEMAS = {}
for fname, schema_cols in CSV_SCHEMAS.items():
    extra = [(c[0], c[3]) for c in TABLE_EXTRA_COLUMNS.get(fname, [])]
    SCHEMAS[fname] = list(schema_cols) + extra

MEASURES = {}


def measure(name, dax, unit, policy):
    MEASURES[name] = {'dax': dax, 'format': unit, 'description': policy}


# ==============================================================================
# PAGE 1: OVERVIEW MEASURES (Kiểm toán độ phủ & chất lượng 9 Fact)
# ==============================================================================
measure('Total Observations', 'SUM(Overview[rows_total])', '#,0', 'Tổng số quan sát toàn hệ thống Gold (tính từ fact_overview).')
measure('Numeric Observations', 'SUM(Overview[numeric_rows])', '#,0', 'Số quan sát có dữ liệu số thực tế (tính từ fact_overview).')
measure('Present NULL', 'SUM(Overview[present_null_rows])', '#,0', 'Số quan sát khuyết có ghi nhận (tính từ fact_overview).')
measure('Numeric Coverage', 'DIVIDE([Numeric Observations], [Total Observations])', '0.00%', 'Tỷ lệ quan sát có số trên tổng quan sát.')
measure('Null Percentage', 'DIVIDE([Present NULL], [Total Observations])', '0.00%', 'Tỷ lệ quan sát khuyết trên tổng quan sát.')
measure('Highest Null Fact Pct', 'MAXX(Overview, DIVIDE(Overview[present_null_rows], Overview[rows_total]))', '0.00%', 'Tỷ lệ quan sát khuyết cao nhất trong 9 fact.')
measure('Highest Null Fact Display', 'VAR maxFact = TOPN(1, Overview, DIVIDE(Overview[present_null_rows], Overview[rows_total]), DESC) VAR pct = MAXX(maxFact, DIVIDE(Overview[present_null_rows], Overview[rows_total])) VAR factName = MAXX(maxFact, Overview[fact_name_vi]) RETURN factName & ": " & FORMAT(pct, "0.00%")', '@', 'Tên tiếng Việt và tỷ lệ của fact có tỷ lệ khuyết cao nhất.')

# ==============================================================================
# PAGE 2: NSO VIETNAM PRODUCTION MEASURES
# ==============================================================================
for table, column, name, fmt, scope in [
    ('NSOAnnual', 'production_t', 'Annual Production', '#,0', 'V06.12 national annual quantity tonnes'),
    ('NSOAnnual', 'area_ha', 'Annual Area', '#,0', 'V06.12 national annual quantity hectares'),
    ('NSOYield', 'yield_kg_ha', 'Reported Yield', '#,0.00', 'V06.14 national annual reported kg/ha'),
    ('NSOSeasonal', 'production_t', 'Seasonal Production', '#,0', 'V06.12 source seasons, never includes annual'),
    ('FAO', 'production_t', 'FAO Production', '#,0', 'CPC0113 selected non-overlapping source areas, tonnes'),
    ('FAO', 'area_ha', 'FAO Area', '#,0', 'CPC0113 selected non-overlapping source areas, hectares')
]:
    measure(name, f'IF(HASONEVALUE(CalendarYear[Year]), SUM({table}[{column}]))', fmt, scope + '; one calendar year only; NULL remains blank; no SUM across years.')

measure('First Production Year', 'MINX(FILTER(ALLSELECTED(CalendarYear[Year]), NOT ISBLANK(CALCULATE([Annual Production]))), CalendarYear[Year])', '0', 'First selected numeric V06.12 annual year.')
measure('Latest Production Year', 'MAXX(FILTER(ALLSELECTED(CalendarYear[Year]), NOT ISBLANK(CALCULATE([Annual Production]))), CalendarYear[Year])', '0', 'Latest selected numeric V06.12 annual year.')
measure('First Production', 'VAR y=[First Production Year] RETURN IF(NOT ISBLANK(y), CALCULATE([Annual Production], REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))', '#,0', 'First numeric selected annual production, V06.12 tonnes.')
measure('Latest Production', 'VAR y=[Latest Production Year] RETURN IF(NOT ISBLANK(y), CALCULATE([Annual Production], REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))', '#,0', 'Latest numeric selected annual production, V06.12 tonnes.')
measure('Production Change', 'IF([First Production Year]<[Latest Production Year], [Latest Production]-[First Production])', '+#,0;-#,0;0', 'Requires two distinct numeric comparable years; tonnes.')
measure('Production Change Pct', 'IF([First Production]>0 && [First Production Year]<[Latest Production Year], DIVIDE([Production Change],[First Production]))', '+0.00%;-0.00%;0.00%', 'Endpoint ratio; positive first denominator; full selected scope recalculated.')
measure('Production YoY', 'VAR y=SELECTEDVALUE(CalendarYear[Year]) VAR prev=CALCULATE([Annual Production],REMOVEFILTERS(CalendarYear),CalendarYear[Year]=y-1) VAR currentValue=[Annual Production] RETURN IF(NOT ISBLANK(y) && NOT ISBLANK(currentValue) && prev>0,DIVIDE(currentValue-prev,prev))', '+0.00%;-0.00%;0.00%', 'Exact previous calendar year; never previous available row.')
measure('Latest Production YoY', 'VAR y=[Latest Production Year] RETURN IF(NOT ISBLANK(y),CALCULATE([Production YoY],REMOVEFILTERS(CalendarYear),CalendarYear[Year]=y))', '+0.00%;-0.00%;0.00%', 'YoY at latest numeric selected year, exact prior year.')
measure('Peak Production', 'MAXX(ALLSELECTED(CalendarYear[Year]),CALCULATE([Annual Production]))', '#,0', 'Observed numeric peak within selected V06.12 years, not a forecast.')
measure('Peak Year', 'VAR peak=[Peak Production] RETURN IF(NOT ISBLANK(peak), MINX(FILTER(ALLSELECTED(CalendarYear[Year]),CALCULATE([Annual Production])=peak),CalendarYear[Year]))', '0', 'Earliest year if selected observed peaks tie.')
measure('Latest vs Peak Pct', 'IF([Peak Production]>0,DIVIDE([Latest Production]-[Peak Production],[Peak Production]))', '+0.00%;-0.00%;0.00%', 'Selected endpoint vs selected observed peak.')

# Area endpoints
measure('First Area Year', 'MINX(FILTER(ALLSELECTED(CalendarYear[Year]), NOT ISBLANK(CALCULATE([Annual Area]))), CalendarYear[Year])', '0', 'First selected numeric V06.12 annual area year.')
measure('Latest Area Year', 'MAXX(FILTER(ALLSELECTED(CalendarYear[Year]), NOT ISBLANK(CALCULATE([Annual Area]))), CalendarYear[Year])', '0', 'Latest selected numeric V06.12 annual area year.')
measure('First Area', 'VAR y=[First Area Year] RETURN IF(NOT ISBLANK(y), CALCULATE([Annual Area], REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))', '#,0', 'First numeric selected annual area, V06.12 hectares.')
measure('Latest Area', 'VAR y=[Latest Area Year] RETURN IF(NOT ISBLANK(y), CALCULATE([Annual Area], REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))', '#,0', 'Latest numeric selected annual area, V06.12 hectares.')
measure('Area Change Pct', 'IF([First Area]>0 && [First Area Year]<[Latest Area Year], DIVIDE([Latest Area]-[First Area], [First Area]))', '+0.00%;-0.00%;0.00%', 'Tăng trưởng diện tích gieo trồng cả giai đoạn.')

# Yield endpoints
measure('First Yield Year', 'MINX(FILTER(ALLSELECTED(CalendarYear[Year]), NOT ISBLANK(CALCULATE([Reported Yield]))), CalendarYear[Year])', '0', 'First selected numeric V06.14 reported yield year.')
measure('Latest Yield Year', 'MAXX(FILTER(ALLSELECTED(CalendarYear[Year]), NOT ISBLANK(CALCULATE([Reported Yield]))), CalendarYear[Year])', '0', 'Latest selected numeric V06.14 reported yield year.')
measure('First Yield', 'VAR y=[First Yield Year] RETURN IF(NOT ISBLANK(y), CALCULATE([Reported Yield], REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))', '#,0.00', 'First numeric selected reported yield, V06.14 kg/ha.')
measure('Latest Yield', 'VAR y=[Latest Yield Year] RETURN IF(NOT ISBLANK(y), CALCULATE([Reported Yield], REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))', '#,0.00', 'Latest numeric selected reported yield, V06.14 kg/ha.')
measure('Yield Change Pct', 'IF([First Yield]>0 && [First Yield Year]<[Latest Yield Year], DIVIDE([Latest Yield]-[First Yield], [First Yield]))', '+0.00%;-0.00%;0.00%', 'Tăng trưởng năng suất lúa công bố cả giai đoạn.')

measure('NSO Insight', 'IF(ISBLANK([First Production Year]),"Không có dữ liệu trong phạm vi chọn.",IF([First Production Year]=[Latest Production Year],"V06.12 · chỉ một năm có số: " & FORMAT([Latest Production Year],"0") & ". Thay đổi đầu–cuối: Không áp dụng. Yield V06.14 chỉ có từ 1995.","V06.12 · " & FORMAT([First Production Year],"0") & "–" & FORMAT([Latest Production Year],"0") & ": " & FORMAT([Production Change Pct],"+0.00%;-0.00%") & ". Đỉnh quan sát " & FORMAT([Peak Year],"0") & " (" & FORMAT([Peak Production]/1000000,"#,0.00") & " tr tấn); cuối kỳ so với đỉnh " & FORMAT([Latest vs Peak Pct],"+0.00%;-0.00%") & ". Diện tích: " & FORMAT([Area Change Pct],"+0.00%;-0.00%") & ", Năng suất: " & FORMAT([Yield Change Pct],"+0.00%;-0.00%") & ". Không suy quan hệ nhân quả."))', '@', 'Dynamic descriptive scope; all numeric facts come from current filters.')

# ==============================================================================
# PAGE 3: FAO INTERNATIONAL PRODUCTION MEASURES
# ==============================================================================
measure('FAO Paired Production', 'IF(HASONEVALUE(CalendarYear[Year]),SUM(FAO[paired_production_t]))', '#,0', 'Same eligible country/year population as paired area, tonnes.')
measure('FAO Paired Area', 'IF(HASONEVALUE(CalendarYear[Year]),SUM(FAO[paired_area_ha]))', '#,0', 'Only pairs with numeric production and positive numeric area, hectares.')
measure('FAO Weighted Yield', 'DIVIDE(1000*[FAO Paired Production],[FAO Paired Area])', '#,0.00', 'kg/ha, single calendar year, ratio of paired sums over selected non-overlapping area universe; never AVG yields.')
measure('FAO Reported Yield', 'IF(HASONEVALUE(CalendarYear[Year]) && HASONEVALUE(FAO[geography_key]),MAX(FAO[reported_yield_kg_ha]))', '#,0.00', 'Source reported kg/ha, one source area/year; not combined across areas.')
measure('FAO Common Year', 'MAXX(FILTER(ALLSELECTED(CalendarYear[Year]), VAR n=CALCULATE(DISTINCTCOUNT(FAO[geography_key]),ALLSELECTED(FAO[area_name]),FAO[paired_valid]=TRUE()) VAR expected=CALCULATE(DISTINCTCOUNT(FAO[geography_key]),ALLSELECTED(FAO[area_name]),REMOVEFILTERS(CalendarYear)) RETURN n=expected && n>0),CalendarYear[Year])', '0', 'Latest selected year with valid paired measures for every selected area; selected universe only.')
measure('FAO Common Production', 'VAR y=[FAO Common Year] RETURN IF(NOT ISBLANK(y), CALCULATE([FAO Production],REMOVEFILTERS(CalendarYear),CalendarYear[Year]=y))', '#,0', 'Production at selected common year, not world total.')
measure('FAO Common Rank', 'IF(NOT ISBLANK([FAO Common Production]),RANKX(ALLSELECTED(FAO[area_name]),[FAO Common Production],,DESC,Dense))', '0', 'Rank only among admitted selected source areas, common paired year.')
measure('FAO Scatter Year', 'IF(HASONEVALUE(CalendarYear[Year]), SELECTEDVALUE(CalendarYear[Year]), [FAO Common Year])', '0', 'Active year for FAO scatter: selected single year, or common paired year when all years are selected.')
measure('FAO Scatter Area', 'VAR y=[FAO Scatter Year] RETURN IF(NOT ISBLANK(y), CALCULATE(SUM(FAO[area_ha]), REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))', '#,0', 'Area at selected year or default common year.')
measure('FAO Scatter Production', 'VAR y=[FAO Scatter Year] RETURN IF(NOT ISBLANK(y), CALCULATE(SUM(FAO[production_t]), REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))', '#,0', 'Production at selected year or default common year.')
measure('FAO Scatter Yield', 'VAR y=[FAO Scatter Year] RETURN IF(NOT ISBLANK(y), CALCULATE([FAO Weighted Yield], REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))', '#,0.00', 'Weighted yield at selected year or default common year.')
measure('FAO Production Index', 'VAR cur=[FAO Production] VAR base=CALCULATE([FAO Production],REMOVEFILTERS(CalendarYear),CalendarYear[Year]=1961) RETURN IF(NOT ISBLANK(cur) && base>0,DIVIDE(cur,base)*100)', '#,0.00', 'Chỉ số sản lượng lúa so với năm gốc 1961 (1961=100); loại trừ chênh lệch quy mô tuyệt đối.')
measure('FAO Common Production Title', '"[Lúa] Sản lượng lúa tại năm chung (" & FORMAT([FAO Common Year],"0") & ")"', '@', 'Tiêu đề động biểu đồ sản lượng lúa theo năm chung.')

# ==============================================================================
# PAGE 4: SUA & USDA PSD BALANCE MEASURES
# ==============================================================================
measure('SUA Selected Value', 'IF(HASONEVALUE(CalendarYear[Year]) && HASONEVALUE(SUA[element_code]),MAX(SUA[value_t]))', '#,0.00', 'One SUA calendar year/element, M49 704/CPC23161.02; NULL stays blank; no cross-element SUM.')
for code, name in [
    ('5141', 'SUA Food Supply'), ('5113', 'SUA Opening Stocks'), ('5071', 'SUA Stock Variation'),
    ('5023', 'SUA Processed'), ('5520', 'SUA Feed'), ('5525', 'SUA Seed'), ('5016', 'SUA Loss'),
    ('5910', 'SUA Exports'), ('5165', 'SUA Other Uses'), ('5166', 'SUA Residuals')
]:
    measure(name, f'CALCULATE([SUA Selected Value],REMOVEFILTERS(SUA[element_code]),REMOVEFILTERS(SUA[metric]),SUA[element_code]="{code}")', '#,0.00', 'SUA element ' + code + '; one calendar year, tonnes; no sum over years or invented balance.')

measure('PSD Selected Value', 'IF(HASONEVALUE(PSD[market_year]) && HASONEVALUE(PSD[attribute]),MAX(PSD[value]))', '#,0.00', 'One marketing year and attribute; unit from row. No mixing MY/TY/rough/milled, rates or stocks.')
for attr, name in [('Production', 'PSD Production'), ('Domestic Consumption', 'PSD Consumption'), ('Ending Stocks', 'PSD Ending Stocks')]:
    measure(name, f'CALCULATE([PSD Selected Value],REMOVEFILTERS(PSD[attribute]),PSD[attribute]="{attr}")', '#,0', 'USDA Vietnam/Rice, Milled ' + attr + ' tonnes at one market year; no stock sum across time.')

# ==============================================================================
# DISPLAY MEASURES FOR CHARTS (S1: Divided by 1e6 and 1e9 to match axis labels)
# ==============================================================================
# Page 2: Vietnam Production Chart Display Measures
measure('Annual Production Mil', '[Annual Production] / 1000000', '#,0.00', 'Sản lượng lúa cả năm triệu tấn.')
measure('Annual Area Mil', '[Annual Area] / 1000000', '#,0.00', 'Diện tích lúa cả năm triệu ha.')
measure('Seasonal Production Mil', '[Seasonal Production] / 1000000', '#,0.00', 'Sản lượng lúa theo vụ triệu tấn.')

# Page 3: International Production Chart Display Measures
measure('FAO Common Production Mil', '[FAO Common Production] / 1000000', '#,0.0', 'Sản lượng lúa tại năm chung triệu tấn.')
measure('FAO Scatter Area Mil', '[FAO Scatter Area] / 1000000', '#,0.0', 'Diện tích scatter triệu ha.')
measure('FAO Scatter Production Mil', '[FAO Scatter Production] / 1000000', '#,0.0', 'Sản lượng scatter triệu tấn.')

# Page 4: SUA & PSD Chart Display Measures
measure('SUA Food Supply Mil', '[SUA Food Supply] / 1000000', '#,0.00', 'Tiêu thụ thực phẩm triệu tấn.')
measure('SUA Processed Mil', '[SUA Processed] / 1000000', '#,0.00', 'Chế biến triệu tấn.')
measure('SUA Opening Stocks Mil', '[SUA Opening Stocks] / 1000000', '#,0.00', 'Tồn kho đầu kỳ triệu tấn.')
measure('SUA Stock Variation Mil', '[SUA Stock Variation] / 1000000', '#,0.00', 'Biến động tồn kho triệu tấn.')
measure('SUA Residuals Mil', '[SUA Residuals] / 1000000', '#,0.00', 'Sai số thống kê triệu tấn.')
measure('PSD Production Mil', '[PSD Production] / 1000000', '#,0.00', 'Sản xuất USDA PSD triệu tấn.')
measure('PSD Consumption Mil', '[PSD Consumption] / 1000000', '#,0.00', 'Tiêu dùng USDA PSD triệu tấn.')
measure('PSD Ending Stocks Mil', '[PSD Ending Stocks] / 1000000', '#,0.00', 'Tồn kho cuối kỳ USDA PSD triệu tấn.')

# Page 6: Trade Chart Display Measures
measure('Trade Quantity Mil', '[Trade Quantity] / 1000000', '#,0.00', 'Lượng xuất khẩu triệu tấn.')
measure('Trade Value Bil', '[Trade Value] / 1000000000', '#,0.00', 'Giá trị xuất khẩu tỷ USD.')

# ==============================================================================
# PAGE 5: PRICES MEASURES (World Bank, USDA Price Reference, VN Domestic Quotes)
# ==============================================================================
# USDA Price: guard both location and commodity_class
measure('USDA Price', 'IF(HASONEVALUE(USDAPrice[location]) && HASONEVALUE(USDAPrice[commodity_class]), IF(HASONEVALUE(USDAPrice[reference_year]), AVERAGE(USDAPrice[price_usd_t]), AVERAGE(USDAPrice[price_usd_t])))', '#,0.00', 'USDA FOB export price reference USD/tonne; guardrail: yêu cầu chọn đúng 1 quốc gia và 1 phẩm cấp.')
measure('USDA FOB Chart Price', 'VAR chosen_class = SELECTEDVALUE(USDAPrice[commodity_class], "5-PERCENT BROKEN") RETURN CALCULATE(AVERAGE(USDAPrice[price_usd_t]), KEEPFILTERS(USDAPrice[commodity_class] = chosen_class))', '#,0.00', 'Giá FOB USDA đối sánh 4 nước (mặc định Gạo 5% tấm nếu chưa chọn).')
measure('USDA FOB Chart Title', 'VAR chosen_class_vi = SELECTEDVALUE(USDAPrice[commodity_class_vi]) RETURN IF(ISBLANK(chosen_class_vi), "[Gạo xay xát] Giá FOB USDA – Gạo 5% tấm (đối sánh 4 nước) (USD/tấn)", "[Gạo xay xát] Giá FOB USDA – " & chosen_class_vi & " (USD/tấn)")', '@', 'Tiêu đề động biểu đồ giá FOB USDA.')

# WB Price, MoM, Volatility: guard single series and single month
measure('WB Price', 'IF(HASONEVALUE(WB[series_code]) && HASONEVALUE(BenchmarkMonth[Date]), MAX(WB[price_usd_t]))', '#,0.00', 'World Bank benchmark monthly USD/tonne level; guardrail: yêu cầu đúng 1 chuỗi giá và 1 tháng.')
measure('WB MoM', 'IF(HASONEVALUE(WB[series_code]) && HASONEVALUE(BenchmarkMonth[Date]), MAX(WB[monthly_change_pct])/100)', '+0.00%;-0.00%;0.00%', 'Exact calendar previous month for single selected WB series; guardrail cấm trộn chuỗi.')
measure('WB Volatility', 'IF(HASONEVALUE(WB[series_code]) && HASONEVALUE(BenchmarkMonth[Date]), MAX(WB[volatility_12m_pct])/100)', '0.00%', 'Trailing 12 log-return sample std for single selected WB series; guardrail cấm trộn chuỗi.')
measure('WB Peak', 'MAXX(ALLSELECTED(BenchmarkMonth[Date]),CALCULATE([WB Price]))', '#,0.00', 'Observed maximum benchmark in selected months, USD/tonne.')
measure('WB Minimum', 'MINX(FILTER(ALLSELECTED(BenchmarkMonth[Date]),NOT ISBLANK(CALCULATE([WB Price]))),CALCULATE([WB Price]))', '#,0.00', 'Observed numeric minimum selected monthly benchmark, USD/tonne.')
measure('WB Numeric Months', 'COALESCE(CALCULATE(COUNTROWS(WB),WB[numeric_present]=TRUE()),0)', '#,0', 'Numeric monthly prices in selected calendar; no imputation.')
measure('WB Absent Months', 'COALESCE(CALCULATE(COUNTROWS(WB),WB[observation_present]=FALSE()),0)', '#,0', 'Count of calendar rows with no source observation.')

measure('Latest WB Month', 'MAXX(FILTER(ALLSELECTED(BenchmarkMonth[Date]), NOT ISBLANK(CALCULATE(MAX(WB[price_usd_t])))), BenchmarkMonth[Date])', '@', 'Tháng mới nhất có giá World Bank trong phạm vi chọn.')
measure('Latest WB Price', 'VAR m=[Latest WB Month] RETURN IF(NOT ISBLANK(m) && HASONEVALUE(WB[series_code]), CALCULATE(MAX(WB[price_usd_t]), REMOVEFILTERS(BenchmarkMonth), BenchmarkMonth[Date]=m))', '#,0.00', 'Mức giá World Bank tại tháng mới nhất của chuỗi đang chọn.')
measure('Latest WB MoM', 'VAR m=[Latest WB Month] RETURN IF(NOT ISBLANK(m) && HASONEVALUE(WB[series_code]), CALCULATE(MAX(WB[monthly_change_pct])/100, REMOVEFILTERS(BenchmarkMonth), BenchmarkMonth[Date]=m))', '+0.00%;-0.00%;0.00%', 'Thay đổi MoM tại tháng mới nhất của chuỗi đang chọn.')
measure('Latest WB Price Display', 'IF(ISBLANK([Latest WB Price]), "Chọn 1 chuỗi", FORMAT([Latest WB Price], "$#,0.00") & " /tấn (" & FORMAT([Latest WB MoM], "+0.00%;-0.00%") & ")")', '@', 'Thẻ giá WB mới nhất kèm MoM.')
measure('WB Peak Display', 'IF(ISBLANK([WB Peak]), "Chọn 1 chuỗi", FORMAT([WB Peak], "$#,0.00") & " /tấn")', '@', 'Thẻ đỉnh giá WB.')
measure('WB Minimum Display', 'IF(ISBLANK([WB Minimum]), "Chọn 1 chuỗi", FORMAT([WB Minimum], "$#,0.00") & " /tấn")', '@', 'Thẻ đáy giá WB.')
measure('WB Numeric Months Display', 'FORMAT([WB Numeric Months], "#,0") & " tháng"', '@', 'Thẻ số tháng có giá WB.')

# S2: Fixed WB Cards (never display "Chọn 1 chuỗi" at "All")
measure('Latest WB Thai 5 Price', 'VAR m = MAXX(FILTER(ALL(WB), WB[series_code] = "rice_thai_5" && NOT ISBLANK(WB[price_usd_t])), WB[month_date]) RETURN CALCULATE(MAX(WB[price_usd_t]), REMOVEFILTERS(WB), WB[series_code] = "rice_thai_5", WB[month_date] = m)', '#,0.00', 'Giá Gạo Thái 5% mới nhất.')
measure('Latest WB Thai 5 MoM', 'VAR m = MAXX(FILTER(ALL(WB), WB[series_code] = "rice_thai_5" && NOT ISBLANK(WB[price_usd_t])), WB[month_date]) RETURN CALCULATE(MAX(WB[monthly_change_pct]) / 100, REMOVEFILTERS(WB), WB[series_code] = "rice_thai_5", WB[month_date] = m)', '+0.00%;-0.00%;0.00%', 'Biến động MoM Gạo Thái 5% mới nhất.')
measure('Latest WB Thai 5 Display', 'FORMAT([Latest WB Thai 5 Price], "$#,0.00") & " /tấn (" & FORMAT([Latest WB Thai 5 MoM], "+0.00%;-0.00%") & ")"', '@', 'Thẻ giá Thái 5% mới nhất kèm MoM.')

measure('Latest WB VN 5 Price', 'VAR m = MAXX(FILTER(ALL(WB), WB[series_code] = "rice_viet_namese_5" && NOT ISBLANK(WB[price_usd_t])), WB[month_date]) RETURN CALCULATE(MAX(WB[price_usd_t]), REMOVEFILTERS(WB), WB[series_code] = "rice_viet_namese_5", WB[month_date] = m)', '#,0.00', 'Giá Gạo Việt Nam 5% mới nhất.')
measure('Latest WB VN 5 MoM', 'VAR m = MAXX(FILTER(ALL(WB), WB[series_code] = "rice_viet_namese_5" && NOT ISBLANK(WB[price_usd_t])), WB[month_date]) RETURN CALCULATE(MAX(WB[monthly_change_pct]) / 100, REMOVEFILTERS(WB), WB[series_code] = "rice_viet_namese_5", WB[month_date] = m)', '+0.00%;-0.00%;0.00%', 'Biến động MoM Gạo Việt Nam 5% mới nhất.')
measure('Latest WB VN 5 Display', 'FORMAT([Latest WB VN 5 Price], "$#,0.00") & " /tấn (" & FORMAT([Latest WB VN 5 MoM], "+0.00%;-0.00%") & ")"', '@', 'Thẻ giá Việt Nam 5% mới nhất kèm MoM.')

measure('Latest Common WB Month', 'MAXX(FILTER(ALL(WB[month_date]), VAR m = WB[month_date] RETURN CALCULATE(COUNTROWS(WB), REMOVEFILTERS(WB), WB[month_date] = m, WB[series_code] = "rice_thai_5", NOT ISBLANK(WB[price_usd_t])) > 0 && CALCULATE(COUNTROWS(WB), REMOVEFILTERS(WB), WB[month_date] = m, WB[series_code] = "rice_viet_namese_5", NOT ISBLANK(WB[price_usd_t])) > 0), WB[month_date])', 'yyyy-MM-dd', 'Tháng mới nhất chung có giá của cả Thái 5% và VN 5%.')
measure('WB Spread VN5 vs Thai5', 'VAR m = [Latest Common WB Month] VAR vn = CALCULATE(MAX(WB[price_usd_t]), REMOVEFILTERS(WB), WB[series_code] = "rice_viet_namese_5", WB[month_date] = m) VAR thai = CALCULATE(MAX(WB[price_usd_t]), REMOVEFILTERS(WB), WB[series_code] = "rice_thai_5", WB[month_date] = m) RETURN IF(NOT ISBLANK(vn) && NOT ISBLANK(thai), vn - thai)', '#,0.00', 'Chênh lệch giá World Bank: Việt Nam 5% − Thái 5% (USD/tấn).')
measure('WB Spread VN5 vs Thai5 Display', 'IF(ISBLANK([WB Spread VN5 vs Thai5]), "Không áp dụng", FORMAT([WB Spread VN5 vs Thai5], "+$#,0.00;-$#,0.00;$0.00") & " /tấn")', '@', 'Thẻ chênh lệch giá VN 5% − Thái 5%.')
measure('FAO Scatter Title', 'IF(ISBLANK([FAO Scatter Year]), "[Lúa] Diện tích và sản lượng · Không áp dụng", "[Lúa] Diện tích và sản lượng (năm " & FORMAT([FAO Scatter Year], "0") & ")")', '@', 'Tiêu đề động biểu đồ phân tán diện tích – sản lượng.')

for cmp_name, base_measure, fmt, desc in [
    ('WB MoM Comparison', 'WB MoM', '+0.00%;-0.00%;0.00%', 'Biến động MoM: 2 chuỗi 5% đối sánh mặc định khi chưa lọc, hoặc chuỗi được chọn trên slicer.'),
    ('WB Volatility Comparison', 'WB Volatility', '0.00%', 'Độ biến động 12M: 2 chuỗi 5% đối sánh mặc định khi chưa lọc, hoặc chuỗi được chọn trên slicer.'),
]:
    measure(
        cmp_name,
        f'VAR selected_cnt = CALCULATE(DISTINCTCOUNT(WB[series_code]), ALLSELECTED(WB), REMOVEFILTERS(BenchmarkMonth)) VAR total_cnt = CALCULATE(DISTINCTCOUNT(WB[series_code]), ALL(WB)) VAR cur_code = SELECTEDVALUE(WB[series_code]) RETURN IF(NOT ISBLANK(cur_code) && (selected_cnt < total_cnt || cur_code IN {{"rice_thai_5", "rice_viet_namese_5"}}), [{base_measure}])',
        fmt,
        desc,
    )
measure('WB MoM Title', 'IF(HASONEVALUE(WB[series_code]), "[Gạo xay xát] Biến động MoM – " & SELECTEDVALUE(WB[series_name_vi]) & " (%)", "[Gạo xay xát] Biến động MoM – Gạo Thái 5% & VN 5% (%)")', '@', 'Tiêu đề động biểu đồ MoM.')
measure('WB Volatility Title', 'IF(HASONEVALUE(WB[series_code]), "[Gạo xay xát] Biến động 12M – " & SELECTEDVALUE(WB[series_name_vi]) & IF(SELECTEDVALUE(WB[series_code]) = "rice_viet_namese_5", " (VN khuyết 12/2005–03/2010)", " (%)"), "[Gạo xay xát] Biến động 12M – Thái 5% & VN 5% (VN khuyết 12/2005–03/2010)")', '@', 'Tiêu đề động biểu đồ biến động 12M kèm ghi chú khoảng khuyết dữ liệu khi hiển thị chuỗi VN.')

# Domestic Quote: guard commodity AND price_type; default v13 to standard series when v12 unfiltered, follow v12 selection, and exclude < 1000 unit-error / invalid quotes
_DEFAULT_QUOTE_SERIES = 'Lúa IR 50404 (lúa tươi) · Thương lái thu mua'
_CHOSEN_QUOTE_SERIES_DAX = f'VAR chosen_series = IF(ISFILTERED(VNQuote[series_label]) && COUNTROWS(FILTERS(VNQuote[series_label])) = 1, MAXX(FILTERS(VNQuote[series_label]), VNQuote[series_label]), "{_DEFAULT_QUOTE_SERIES}")'
measure('Domestic Quote', 'IF(HASONEVALUE(VNQuote[commodity]) && HASONEVALUE(VNQuote[price_type]), IF(HASONEVALUE(QuoteDate[Date]), MAX(VNQuote[price_vnd_kg]), AVERAGE(VNQuote[price_vnd_kg])))', '#,0', 'Vietnam domestic quote VND/kg; guardrail: yêu cầu chọn đúng 1 giống lúa và 1 loại giá xác định.')
measure('Domestic Quoted Dates', 'DISTINCTCOUNT(VNQuote[date])', '#,0', 'Số ngày có báo giá duy nhất trong phạm vi chọn.')
measure('Domestic Quote Chart Price', f'{_CHOSEN_QUOTE_SERIES_DAX} VAR valid_rows = FILTER(VNQuote, VNQuote[series_label] = chosen_series && NOT ISBLANK(VNQuote[price_vnd_kg]) && VNQuote[price_vnd_kg] >= 1000) RETURN IF(COUNTROWS(valid_rows) > 0, AVERAGEX(valid_rows, VNQuote[price_vnd_kg]), BLANK())', '#,0', 'Giá nội địa theo chuỗi chọn trên slicer (mặc định IR 50404 tươi · Thương lái thu mua); lọc bỏ giá < 1000 VND/kg và trả BLANK() khi thiếu quan sát.')
measure('Domestic Quote Chart Title', f'{_CHOSEN_QUOTE_SERIES_DAX} VAR valid_markets = FILTER(VALUES(VNQuote[market]), CALCULATE(COUNTROWS(VNQuote), VNQuote[series_label] = chosen_series, NOT ISBLANK(VNQuote[price_vnd_kg]), VNQuote[price_vnd_kg] >= 1000) > 0) VAR market_list = CONCATENATEX(valid_markets, VNQuote[market], ", ", VNQuote[market], ASC) RETURN IF(ISBLANK(market_list) || market_list = "", "[Lúa] Giá nội địa – " & chosen_series & " (không có dữ liệu)", "[Lúa] Giá nội địa (" & market_list & ") – " & chosen_series & " (VND/kg)")', '@', 'Tiêu đề động biểu đồ giá nội địa liệt kê đúng chuỗi đang chọn và tập thị trường hợp lệ.')

# ==============================================================================
# PAGE 6: TRADE EXPORTS MEASURES
# ==============================================================================
for col, name, policy in [
    ('quantity_t', 'Trade Quantity', 'Reported export tonnes'),
    ('value_usd', 'Trade Value', 'Reported export USD'),
    ('paired_quantity_t', 'Trade Paired Quantity', 'Numeric quantity paired with numeric monetary value and q>0'),
    ('paired_value_usd', 'Trade Paired Value', 'Same eligible contributor population as paired quantity')
]:
    measure(name, f'SUM(Trade[{col}])', '#,0.00', policy + '; reporter704/CPC23161.02/export/2019–2023. Additive over disjoint selected partner-year groups; blanks retained.')

measure('Trade Unit Value', 'DIVIDE([Trade Paired Value],[Trade Paired Quantity])', '#,0.00', 'USD/tonne = ratio of paired sums under current year/partner selection; never average row ratios; positive paired denominator.')
measure('Trade Partners', 'DISTINCTCOUNT(Trade[partner_name])', '#,0', 'Số lượng quốc gia đối tác xuất khẩu thực tế trong phạm vi lọc (7 đối tác).')
measure('Latest Trade Year', 'MAXX(FILTER(ALLSELECTED(CalendarYear[Year]), NOT ISBLANK(CALCULATE([Trade Quantity]))), CalendarYear[Year])', '0', 'Năm xuất khẩu mới nhất có dữ liệu.')
measure('Latest Trade Quantity', 'VAR y=[Latest Trade Year] RETURN IF(NOT ISBLANK(y), CALCULATE([Trade Quantity], REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))', '#,0.00', 'Reported export tonnes in latest selected year.')
measure('Trade Paired Groups', 'COALESCE(COUNTROWS(FILTER(Trade, NOT ISBLANK(Trade[paired_quantity_t]) && NOT ISBLANK(Trade[paired_value_usd]))), 0)', '#,0', 'Eligible paired reporter-partner-product-year groups; coverage of selected partner scope (35 cặp).')
measure('Trade Insight', 'IF([Trade Paired Groups]=0,"Không có cặp đủ điều kiện trong phạm vi chọn.","Việt Nam → " & CONCATENATEX(VALUES(Trade[partner_name_vi]),Trade[partner_name_vi],", ") & " · " & FORMAT(MIN(Trade[year]),"0") & "–" & FORMAT(MAX(Trade[year]),"0") & " · CPC23161.02. " & FORMAT([Trade Partners],"0") & " đối tác (" & FORMAT([Trade Paired Groups],"0") & " cặp năm–đối tác); đơn giá bình quân " & FORMAT([Trade Unit Value],"#,0.00") & " USD/t. Tỷ số giá trị/lượng báo cáo; không phải giá thị trường độc lập.")', '@', 'Dynamic paired coverage and ratio; no global denominator.')

# ==============================================================================
# CARD DISPLAY MEASURES (Legacy strings kept intact; Hero + 3-tier Ref measures added)
# ==============================================================================
measure('Total Observations Display', 'FORMAT([Total Observations], "#,0")', '@', 'Tổng quan sát format vi-VN.')
measure('Numeric Coverage Display', 'FORMAT([Numeric Coverage], "0.00%")', '@', 'Tỷ lệ có số format vi-VN.')
measure('Null Percentage Display', 'FORMAT([Null Percentage], "0.00%")', '@', 'Tỷ lệ khuyết format vi-VN.')
measure('Highest Null Fact Pct Display', 'FORMAT([Highest Null Fact Pct], "0.00%")', '@', 'Tỷ lệ khuyết cao nhất format vi-VN.')

# Page 2 Card Displays
measure('First Production Display', 'IF(ISBLANK([First Production]),"Không áp dụng",FORMAT([First Production]/1000000,"#,0.00") & " tr tấn")', '@', 'Sản lượng kỳ đầu triệu tấn.')
measure('Latest Production Display', 'IF(ISBLANK([Latest Production]),"Không áp dụng",FORMAT([Latest Production]/1000000,"#,0.00") & " tr tấn")', '@', 'Sản lượng kỳ cuối triệu tấn.')
measure('Production Change Pct Display', 'IF(ISBLANK([Production Change Pct]),"Không áp dụng",FORMAT([Production Change Pct],"+0.00%;-0.00%"))', '@', 'Thay đổi sản lượng đầu–cuối.')
measure('Latest Production YoY Display', 'IF(ISBLANK([Latest Production YoY]),"Không áp dụng",FORMAT([Latest Production YoY],"+0.00%;-0.00%"))', '@', 'YoY kỳ cuối.')
measure('Peak Production Display', 'IF(ISBLANK([Peak Production]),"Không áp dụng",FORMAT([Peak Production]/1000000,"#,0.00") & " tr tấn (" & FORMAT([Peak Year],"0") & ")")', '@', 'Đỉnh sản lượng kèm năm.')
measure('Latest vs Peak Pct Display', 'IF(ISBLANK([Latest vs Peak Pct]),"Không áp dụng",FORMAT([Latest vs Peak Pct],"+0.00%;-0.00%"))', '@', 'Cuối kỳ so với đỉnh.')
measure('Area Change Pct Display', 'IF(ISBLANK([Area Change Pct]),"Không áp dụng",FORMAT([Area Change Pct],"+0.00%;-0.00%"))', '@', 'Thay đổi diện tích đầu–cuối.')
measure('Yield Change Pct Display', 'IF(ISBLANK([Yield Change Pct]),"Không áp dụng",FORMAT([Yield Change Pct],"+0.00%;-0.00%"))', '@', 'Thay đổi năng suất đầu–cuối.')

# Page 6 Card Displays
measure('Trade Quantity Display', 'IF(ISBLANK([Trade Quantity]),"Không áp dụng",FORMAT([Trade Quantity]/1000000,"#,0.00") & " tr tấn")', '@', 'Formatted export quantity in million tonnes.')
measure('Latest Trade Quantity Display', 'VAR y=[Latest Trade Year] RETURN IF(ISBLANK([Latest Trade Quantity]),"Không áp dụng",FORMAT([Latest Trade Quantity]/1000000,"#,0.00") & " tr tấn · " & FORMAT(y,"0"))', '@', 'Formatted export quantity in million tonnes for latest year.')
measure('Trade Value Display', 'IF(ISBLANK([Trade Value]),"Không áp dụng",FORMAT([Trade Value]/1000000000,"$#,0.00") & " tỷ USD")', '@', 'Formatted export value in billion USD.')
measure('Trade Unit Value Display', 'IF(ISBLANK([Trade Unit Value]),"Không áp dụng",FORMAT([Trade Unit Value],"$#,0.00") & " /tấn")', '@', 'Formatted unit value USD/tonne.')
measure('Trade Partners Display', 'IF(ISBLANK([Trade Partners]),"Không áp dụng",FORMAT([Trade Partners],"#,0") & " đối tác")', '@', 'Formatted distinct partner count.')
measure('Trade Paired Groups Display', 'IF(ISBLANK([Trade Paired Groups]),"Không áp dụng",FORMAT([Trade Paired Groups],"#,0") & " cặp đối tác–năm")', '@', 'Formatted paired groups count as partner-year pairs.')

# Clean 3-tier template measures (Line 2 Hero + Line 3 Reference Triangle & Color)
_GREEN = '#16A34A'
_RED = '#DC2626'
_SLATE = '#64748B'
_TEAL = '#0F6B5E'


def _trend_ref(name, color_name, expr, desc, suffix=''):
    tail = f' & "{suffix}"' if suffix else ''
    measure(
        name,
        f'VAR v = {expr} RETURN IF(ISBLANK(v), "Không áp dụng", IF(v > 0, "▲ " & FORMAT(v, "0.00%"){tail}, IF(v < 0, "▼ " & FORMAT(v, "0.00%"){tail}, "• 0,00%"{tail})))',
        '@',
        desc,
    )
    measure(
        color_name,
        f'VAR v = {expr} RETURN IF(ISBLANK(v), "{_SLATE}", IF(v > 0, "{_GREEN}", IF(v < 0, "{_RED}", "{_SLATE}")))',
        '@',
        f'Màu ngữ nghĩa cho {name}.',
    )


def _static_ref(name, color_name, text_dax, color_hex, desc):
    measure(name, text_dax, '@', desc)
    measure(color_name, f'"{color_hex}"', '@', f'Màu chú thích cho {name}.')


# Page 1 3-tier refs
_static_ref('Overview Scope Ref', 'Overview Scope Ref Color', '"9 bảng Fact · Gold layer"', _SLATE, 'Phạm vi 9 bảng Fact.')
_static_ref('Overview Numeric Ref', 'Overview Numeric Ref Color', 'FORMAT([Numeric Observations], "#,0") & " có số thực"', _GREEN, 'Số quan sát có giá trị số.')
_static_ref('Overview Null Ref', 'Overview Null Ref Color', 'FORMAT([Present NULL], "#,0") & " giá trị NULL"', _SLATE, 'Số quan sát khuyết có ghi nhận.')
_static_ref('Highest Null Fact Ref', 'Highest Null Fact Ref Color', 'VAR maxFact = TOPN(1, Overview, DIVIDE(Overview[present_null_rows], Overview[rows_total]), DESC) RETURN "Nguồn: " & MAXX(maxFact, Overview[fact_name_vi])', _SLATE, 'Tên nguồn có tỷ lệ khuyết cao nhất.')

# Page 2 Hero + 3-tier refs
measure('Peak Production Hero', 'IF(ISBLANK([Peak Production]), "Không áp dụng", FORMAT([Peak Production]/1000000, "#,0.00") & " tr tấn")', '@', 'Đỉnh sản lượng triệu tấn.')
_static_ref('First Production Year Ref', 'First Production Year Ref Color', 'IF(ISBLANK([First Production Year]), "Không áp dụng", "Năm " & FORMAT([First Production Year], "0"))', _SLATE, 'Năm bắt đầu giai đoạn chọn.')
_trend_ref('Latest Production YoY Ref', 'Latest Production YoY Ref Color', '[Latest Production YoY]', 'Tăng trưởng YoY kỳ cuối.', suffix=' YoY')
measure('Production Change Abs Ref', 'IF(ISBLANK([Production Change]), "Không áp dụng", IF([Production Change] > 0, "▲ +" & FORMAT([Production Change]/1000000, "#,0.00") & " tr tấn", IF([Production Change] < 0, "▼ " & FORMAT([Production Change]/1000000, "#,0.00") & " tr tấn", "• 0,00 tr tấn")))', '@', 'Mức tăng/giảm tuyệt đối đầu–cuối.')
measure('Production Change Abs Ref Color', f'IF(ISBLANK([Production Change]), "{_SLATE}", IF([Production Change] > 0, "{_GREEN}", IF([Production Change] < 0, "{_RED}", "{_SLATE}")))', '@', 'Màu ngữ nghĩa cho Production Change Abs Ref.')
measure('Latest Production Period Ref', 'IF(ISBLANK([Latest Production Year]), "Không áp dụng", IF([Latest Production YoY] >= 0, "▲ ", "▼ ") & FORMAT([Latest Production Year], "0") & " so " & FORMAT([Latest Production Year] - 1, "0"))', '@', 'Kỳ so sánh YoY mới nhất.')
_static_ref('Peak Year Ref', 'Peak Year Ref Color', 'IF(ISBLANK([Peak Year]), "Không áp dụng", "Kỷ lục năm " & FORMAT([Peak Year], "0"))', _TEAL, 'Năm đạt đỉnh sản lượng.')
measure('Latest vs Peak Ref', 'IF(ISBLANK([Latest vs Peak Pct]), "Không áp dụng", IF([Latest vs Peak Pct] < 0, "▼ Dưới đỉnh " & FORMAT([Peak Year], "0"), "▲ Đạt đỉnh " & FORMAT([Peak Year], "0")))', '@', 'So sánh kỳ cuối với đỉnh.')
measure('Latest vs Peak Ref Color', f'IF(ISBLANK([Latest vs Peak Pct]), "{_SLATE}", IF([Latest vs Peak Pct] < 0, "{_RED}", "{_GREEN}"))', '@', 'Màu ngữ nghĩa cho Latest vs Peak Ref.')
measure('Area Span Ref', 'IF(ISBLANK([Area Change Pct]), "Không áp dụng", IF([Area Change Pct] >= 0, "▲ ", "▼ ") & FORMAT([First Area Year], "0") & "–" & FORMAT([Latest Area Year], "0"))', '@', 'Khoảng thời gian diện tích.')
measure('Area Span Ref Color', f'IF(ISBLANK([Area Change Pct]), "{_SLATE}", IF([Area Change Pct] >= 0, "{_GREEN}", "{_RED}"))', '@', 'Màu ngữ nghĩa thay đổi diện tích.')
measure('Yield Span Ref', 'IF(ISBLANK([Yield Change Pct]), "Không áp dụng", IF([Yield Change Pct] >= 0, "▲ ", "▼ ") & FORMAT([First Yield Year], "0") & "–" & FORMAT([Latest Yield Year], "0"))', '@', 'Khoảng thời gian năng suất.')
measure('Yield Span Ref Color', f'IF(ISBLANK([Yield Change Pct]), "{_SLATE}", IF([Yield Change Pct] >= 0, "{_GREEN}", "{_RED}"))', '@', 'Màu ngữ nghĩa thay đổi năng suất.')

# Page 5 Hero + 3-tier refs
measure('Latest WB Thai 5 Hero', 'FORMAT([Latest WB Thai 5 Price], "$#,0.00") & " /tấn"', '@', 'Giá gạo Thái 5% mới nhất.')
measure('Latest WB VN 5 Hero', 'FORMAT([Latest WB VN 5 Price], "$#,0.00") & " /tấn"', '@', 'Giá gạo Việt Nam 5% mới nhất.')
_trend_ref('Latest WB Thai 5 MoM Ref', 'Latest WB Thai 5 MoM Ref Color', '[Latest WB Thai 5 MoM]', 'Biến động MoM gạo Thái 5%.', suffix=' MoM')
_trend_ref('Latest WB VN 5 MoM Ref', 'Latest WB VN 5 MoM Ref Color', '[Latest WB VN 5 MoM]', 'Biến động MoM gạo VN 5%.', suffix=' MoM')
_static_ref('Latest Common WB Ref', 'Latest Common WB Ref Color', 'IF(ISBLANK([Latest Common WB Month]), "Không áp dụng", "Kỳ chung: " & FORMAT([Latest Common WB Month], "MM/yyyy"))', _SLATE, 'Tháng chung mới nhất.')

# Page 6 Hero + 3-tier refs
measure('Trade Paired Groups Hero', 'IF(ISBLANK([Trade Paired Groups]), "Không áp dụng", FORMAT([Trade Paired Groups], "#,0") & " cặp")', '@', 'Số cặp đối tác–năm.')
measure('Latest Trade Quantity Hero', 'IF(ISBLANK([Latest Trade Quantity]), "Không áp dụng", FORMAT([Latest Trade Quantity]/1000000, "#,0.00") & " tr tấn")', '@', 'Sản lượng xuất khẩu năm gần nhất.')
_static_ref('Trade Partners Ref', 'Trade Partners Ref Color', '"Thị trường trọng điểm"', _SLATE, 'Phụ đề số đối tác.')
_static_ref('Trade Paired Groups Ref', 'Trade Paired Groups Ref Color', '"Cặp đối tác–năm"', _SLATE, 'Phụ đề cặp đối tác–năm.')
_static_ref('Latest Trade Year Ref', 'Latest Trade Year Ref Color', 'IF(ISBLANK([Latest Trade Year]), "Không áp dụng", "Năm " & FORMAT([Latest Trade Year], "0"))', _TEAL, 'Năm xuất khẩu gần nhất.')
_static_ref('Trade Span Ref', 'Trade Span Ref Color', 'IF(ISBLANK(MIN(Trade[year])), "Không áp dụng", "Giai đoạn " & FORMAT(MIN(Trade[year]), "0") & "–" & FORMAT(MAX(Trade[year]), "0"))', _SLATE, 'Khoảng năm thương mại.')
_static_ref('Trade Value Ref', 'Trade Value Ref Color', '"Tổng kim ngạch 7 nước"', _SLATE, 'Phụ đề kim ngạch xuất khẩu.')
_static_ref('Trade Unit Value Ref', 'Trade Unit Value Ref Color', '"Tỷ số Giá trị / Lượng"', _SLATE, 'Phụ đề đơn giá bình quân.')


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def quoted(name):
    return "'" + name.replace("'", "''") + "'"


def tmdl_table(name, schema, partition, source, measures=None):
    lines = [f'table {name}']
    for col, kind in schema:
        dtype = 'dateTime' if col in ('month_date', 'date', 'Date') else {'integer': 'int64', 'decimal': 'double', 'boolean': 'boolean'}.get(kind.rstrip('?'), 'string')
        lines.extend([f'\tcolumn {col}', f'\t\tdataType: {dtype}', '\t\tsummarizeBy: none', f'\t\tsourceColumn: {col}'])
        if dtype == 'dateTime':
            lines.append('\t\tformatString: yyyy-MM-dd')
        elif col in ('rows_total', 'numeric_rows', 'present_null_rows', 'represented_periods'):
            lines.append('\t\tformatString: #,0')
        elif col == 'null_pct':
            lines.append('\t\tformatString: 0.00%')
    for label, definition in (measures or {}).items():
        lines.extend([f'\t/// {definition["description"]}', f'\tmeasure {quoted(label)} = {definition["dax"]}', f'\t\tformatString: {definition["format"]}'])
    lines.extend([f'\tpartition {name} = {partition}', '\t\tmode: import', '\t\tsource ='])
    lines.extend('\t\t\t' + line for line in source.splitlines())
    return '\n'.join(lines) + '\n'


def m_import(filename, csv_schema, added_columns=None):
    types = []
    for name, kind in csv_schema:
        dtype = 'type date' if name in ('month_date', 'date') else {'integer': 'Int64.Type', 'decimal': 'type number', 'boolean': 'type logical'}.get(kind.rstrip('?'), 'type text')
        types.append('{"' + name + '", ' + dtype + '}')
    lines = [
        'let',
        f'    Raw = Csv.Document(File.Contents(ExportFolder & "\\{filename}"), [Delimiter=",", Columns={len(csv_schema)}, Encoding=65001, QuoteStyle=QuoteStyle.Csv]),',
        '    Headers = Table.PromoteHeaders(Raw, [PromoteAllScalars=true]),',
        '    Checked = if Table.ColumnNames(Headers) = {' + ', '.join(json.dumps(c) for c, _ in csv_schema) + '} then Headers else error "CSV schema mismatch",',
        '    Missing = Table.ReplaceValue(Checked, "", null, Replacer.ReplaceValue, Table.ColumnNames(Checked)),',
        '    Typed = Table.TransformColumnTypes(Missing, {' + ', '.join(types) + '}, "en-US"),',
        '    Manifest = Json.Document(File.Contents(ExportFolder & "\\export_manifest.json")),',
        f'    Entry = Record.Field(Manifest[outputs], "{filename}"),',
        '    Accepted = if Manifest[status] = "COMPLETE" and Manifest[format_version] = "powerbi-csv-2" and Table.RowCount(Typed) = Entry[rows] then Typed else error "Verify the immutable mart bundle before refresh"'
    ]
    current = 'Accepted'
    if added_columns:
        lines[-1] += ','
        for col_name, expr, col_type, _ in added_columns:
            next_step = f'With_{col_name}'
            lines.append(f'    {next_step} = Table.AddColumn({current}, "{col_name}", {expr}, {col_type}),')
            current = next_step
        # Remove trailing comma on last step
        lines[-1] = lines[-1].rstrip(',')
    lines.extend(['in', f'    {current}'])
    return '\n'.join(lines)


def field(table, name, is_measure=False):
    return {('Measure' if is_measure else 'Column'): {'Expression': {'SourceRef': {'Entity': table}}, 'Property': name}}


def literal(value):
    if isinstance(value, bool):
        text = 'true' if value else 'false'
    elif isinstance(value, (int, float)):
        text = str(value) + 'D'
    else:
        text = "'" + str(value).replace("'", "''") + "'"
    return {'expr': {'Literal': {'Value': text}}}


def visual(identifier, kind, title, x, y, w, h, roles=None, single_select=False, sort=None,
           x_axis=None, y_axis=None, hide_slicer_header=False, filter_config=None, dynamic_title_measure=None,
           card_meta=None):
    is_nav = kind == 'pageNavigator'
    is_card_visual = kind == 'cardVisual'
    is_card = kind in ('card', 'cardVisual')
    is_slicer = kind == 'slicer'
    title_size = 9.5 if (is_card or is_slicer) else 10.5
    title_color = '#475569' if (is_card or is_slicer) else '#0F172A'
    title_obj = {
        'show': literal((bool(title) or bool(dynamic_title_measure)) and not is_card_visual),
        'fontSize': literal(title_size),
        'fontColor': {'solid': {'color': literal(title_color)}}
    }
    if dynamic_title_measure:
        title_obj['text'] = {'expr': {'Measure': {'Expression': {'SourceRef': {'Entity': 'Overview'}}, 'Property': dynamic_title_measure}}}
    else:
        title_obj['text'] = literal(title)

    container_objs = {'title': [{'properties': title_obj}]}
    if is_nav:
        container_objs.update({
            'background': [{'properties': {'show': literal(False)}}],
            'border': [{'properties': {'show': literal(False)}}],
            'dropShadow': [{'properties': {'show': literal(False)}}],
            'visualHeader': [{'properties': {'show': literal(False)}}]
        })
    elif is_card_visual:
        pad_v = 3
        pad_h = 14 if h >= 84 else (12 if w >= 240 else 11)
        container_objs.update({
            'background': [{'properties': {
                'show': literal(True),
                'color': {'solid': {'color': literal('#FFFFFF')}},
                'transparency': literal(0)
            }}],
            'border': [{'properties': {
                'show': literal(True),
                'color': {'solid': {'color': literal('#DCE4EC')}},
                'radius': literal(10),
                'width': literal(1)
            }}],
            'dropShadow': [{'properties': {
                'show': literal(True),
                'color': {'solid': {'color': literal('#0F172A')}},
                'preset': literal('Custom'),
                'angle': literal(90),
                'shadowDistance': literal(2),
                'shadowBlur': literal(8),
                'shadowSpread': literal(0),
                'transparency': literal(93)
            }}],
            'visualHeader': [{'properties': {'show': literal(False)}}],
            'padding': [{'properties': {
                'top': literal(pad_v),
                'bottom': literal(pad_v),
                'left': literal(pad_h),
                'right': literal(pad_h)
            }}]
        })
    elif kind == 'card':
        container_objs.update({
            'background': [{'properties': {'show': literal(True), 'color': {'solid': {'color': literal('#FFFFFF')}}, 'transparency': literal(0)}}],
            'divider': [{'properties': {'show': literal(False)}}],
            'spacing': [{'properties': {
                'customizeSpacing': literal(True),
                'verticalSpacing': literal(2),
                'spaceBelowTitle': literal(2),
                'spaceBelowTitleArea': literal(2)
            }}],
            'padding': [{'properties': {'top': literal(6), 'bottom': literal(6), 'left': literal(6), 'right': literal(6)}}],
            'visualHeader': [{'properties': {'show': literal(False)}}]
        })
    elif is_slicer:
        container_objs.update({
            'background': [{'properties': {'show': literal(True), 'color': {'solid': {'color': literal('#FFFFFF')}}, 'transparency': literal(0)}}],
            'spacing': [{'properties': {
                'customizeSpacing': literal(True),
                'verticalSpacing': literal(2),
                'spaceBelowTitle': literal(2),
                'spaceBelowTitleArea': literal(2)
            }}],
            'padding': [{'properties': {'top': literal(4), 'bottom': literal(6), 'left': literal(10), 'right': literal(10)}}],
            'visualHeader': [{'properties': {'show': literal(False)}}]
        })
    else:
        container_objs['background'] = [{'properties': {'show': literal(True), 'color': {'solid': {'color': literal('#FFFFFF')}}, 'transparency': literal(0)}}]

    config = {
        'visualType': kind,
        'drillFilterOtherVisuals': True,
        'visualContainerObjects': container_objs
    }
    if roles:
        config['query'] = {
            'queryState': {
                role: {'projections': [{'field': field(t, n, m), 'queryRef': t + '.' + n, 'displayName': label} for t, n, m, label in columns]}
                for role, columns in roles.items()
            }
        }
        if sort:
            s_t, s_n, s_m, s_dir = sort
            config['query']['sortDefinition'] = {
                'sort': [{'field': field(s_t, s_n, s_m), 'direction': s_dir}],
                'isDefaultSort': False
            }
    objs = {}
    if kind == 'cardVisual' and roles:
        main_m = next(iter(roles.values()))[0][1]
        main_qref = f'Overview.{main_m}'
        meta = card_meta or {}
        font_size = meta.get('font_size') or (16.5 if h >= 84 else (15 if w >= 240 else 14.5))
        callout_size = 71 if h >= 84 else (70 if w >= 240 else 69)
        v_spacing = 5 if (h < 84 and w >= 240) else 6
        objs['layout'] = [{
            'properties': {
                'calloutSize': literal(callout_size),
                'alignment': literal('middle'),
                'cellPadding': literal(0),
                'backgroundShow': literal(False),
                'paddingIndividual': literal(False),
                'paddingUniform': literal(0)
            }
        }]
        objs['fillCustom'] = [{'selector': {'id': 'default'}, 'properties': {'show': literal(False)}}]
        objs['outline'] = [{'selector': {'id': 'default'}, 'properties': {'show': literal(False)}}]
        objs['shadowCustom'] = [{'selector': {'id': 'default'}, 'properties': {'show': literal(False)}}]
        objs['padding'] = [{
            'selector': {'id': 'default'},
            'properties': {
                'paddingSelection': literal('Custom'),
                'paddingIndividual': literal(True),
                'topMargin': literal(0),
                'bottomMargin': literal(0),
                'leftMargin': literal(0),
                'rightMargin': literal(0)
            }
        }]
        objs['spacing'] = [{
            'selector': {'id': 'default'},
            'properties': {
                'verticalSpacing': literal(v_spacing)
            }
        }]
        objs['value'] = [{
            'selector': {'id': 'default'},
            'properties': {
                'fontSize': literal(font_size),
                'labelDisplayUnits': literal(1),
                'horizontalAlignment': literal('left'),
                'bold': literal(True),
                'fontColor': {'solid': {'color': literal('#0F172A')}}
            }
        }]
        objs['label'] = [{
            'selector': {'id': 'default'},
            'properties': {
                'show': literal(True),
                'position': literal('aboveValue'),
                'horizontalAlignment': literal('left'),
                'alignBaselines': literal(False),
                'textWrap': literal(False),
                'fontSize': literal(9),
                'fontColor': {'solid': {'color': literal('#475569')}}
            }
        }]
        objs['divider'] = [{'selector': {'id': 'default'}, 'properties': {'show': literal(False)}}]
        ref_m = meta.get('ref_measure')
        ref_color_m = meta.get('ref_color_measure')
        if ref_m:
            objs['referenceLabel'] = [{
                'selector': {
                    'metadata': main_qref,
                    'id': 'ref1',
                    'data': [{'dataViewWildcard': {'matchingOption': 0}}]
                },
                'properties': {
                    'value': {'expr': {'Measure': {'Expression': {'SourceRef': {'Entity': 'Overview'}}, 'Property': ref_m}}},
                    'backgroundShow': literal(False)
                }
            }]
            objs['referenceLabelTitle'] = [
                {'properties': {'show': literal(False)}},
                {'selector': {'metadata': main_qref, 'id': 'ref1'}, 'properties': {'show': literal(False)}}
            ]
            objs['referenceLabelLayout'] = [{
                'selector': {'id': 'default'},
                'properties': {
                    'arrangement': literal('rows'),
                    'horizontalAlignment': literal('left'),
                    'verticalAlignment': literal('middle'),
                    'labelSpace': literal(0),
                    'outerPadding': literal(0)
                }
            }]
            ref_val_props = {
                'show': literal(True),
                'valueFontSize': literal(9),
                'valueBold': literal(True),
                'valueDisplayUnits': literal(1)
            }
            if ref_color_m:
                ref_val_props['valueFontColor'] = {
                    'solid': {'color': {'expr': {'Measure': {'Expression': {'SourceRef': {'Entity': 'Overview'}}, 'Property': ref_color_m}}}}
                }
            objs['referenceLabelValue'] = [{'selector': {'metadata': main_qref, 'id': 'ref1'}, 'properties': ref_val_props}]
    elif kind == 'card' and roles:
        name = next(iter(roles.values()))[0][1]
        fmt = MEASURES.get(name, {}).get('format', '')
        font_size = 18 if (fmt != '@' or name.endswith(' Display')) else 11
        objs['labels'] = [{'properties': {'labelDisplayUnits': literal(1), 'labelPrecision': literal(2 if '.00' in fmt else 0), 'fontSize': literal(font_size)}}]
        objs['categoryLabels'] = [{'properties': {'show': literal(False)}}]
    if kind == 'barChart':
        objs['labels'] = [{'properties': {'show': literal(True), 'fontSize': literal(9), 'color': {'solid': {'color': literal('#132E2A')}}}}]
    if kind == 'slicer':
        objs['data'] = [{'properties': {'mode': literal('Dropdown')}}]
        objs['selection'] = [{'properties': {'singleSelect': literal(single_select), 'selectAllCheckboxEnabled': literal(not single_select)}}]
        if hide_slicer_header:
            objs['header'] = [{'properties': {'show': literal(False)}}]
    if x_axis:
        cat_props = {}
        if x_axis[0] is not None:
            cat_props['start'] = literal(x_axis[0])
        if x_axis[1] is not None:
            cat_props['end'] = literal(x_axis[1])
        if len(x_axis) > 2 and x_axis[2]:
            cat_props['titleText'] = literal(x_axis[2])
        if cat_props:
            objs['categoryAxis'] = [{'properties': cat_props}]
    if y_axis:
        val_props = {}
        if y_axis[0] is not None:
            val_props['titleText'] = literal(y_axis[0])
        if len(y_axis) > 1 and y_axis[1] is not None:
            val_props['labelDisplayUnits'] = literal(y_axis[1])
        if len(y_axis) > 2 and y_axis[2] is not None:
            val_props['start'] = literal(y_axis[2])
        if len(y_axis) > 3 and y_axis[3] is not None:
            val_props['end'] = literal(y_axis[3])
        if val_props:
            objs['valueAxis'] = [{'properties': val_props}]
    if objs:
        config['objects'] = objs

    result = {
        '$schema': BASE + 'item/report/definition/visualContainer/2.0.0/schema.json',
        'name': identifier,
        'position': {'x': x, 'y': y, 'z': int(identifier[1:]), 'width': w, 'height': h, 'tabOrder': int(identifier[1:])},
        'visual': config
    }
    if filter_config:
        result['filterConfig'] = filter_config
    return result


def pages(batch_short='UNBOUND', export_date='UNKNOWN'):
    """Native visual specifications; page-scoped filters never join incompatible time."""
    col = lambda t, n, label: (t, n, False, label)
    ms = lambda n, label: ('Overview', n, True, label)
    batch_tag = f'Batch: {batch_short} · Export: {export_date}'
    specs = []

    def page(title, note, footer_freshness, interactions=None, pills=None, nav_label=None):
        p = {
            'title': title,
            'nav_label': nav_label or title,
            'note': note,
            'footer': footer_freshness,
            'interactions': interactions or [],
            'pills': pills or [],
            'visuals': []
        }
        specs.append(p)
        return p

    def add(p, kind, title, pos, single_select=False, sort=None, x_axis=None, y_axis=None,
            hide_slicer_header=False, filter_config=None, dynamic_title_measure=None, card_meta=None, **roles):
        p['visuals'].append((kind, title, pos, roles, single_select, sort, x_axis, y_axis, hide_slicer_header, filter_config, dynamic_title_measure, card_meta))

    def card(p, name, label, x, y=96, w=299, h=78, ref=None, ref_color=None, font_size=None):
        if ref_color is None and ref and f'{ref} Color' in MEASURES:
            ref_color = f'{ref} Color'
        m_name = name if (name in MEASURES and (name.endswith(' Display') or name.endswith(' Hero'))) else (name + ' Display')
        card_meta = {'ref_measure': ref, 'ref_color_measure': ref_color, 'font_size': font_size}
        add(p, 'cardVisual', label, (x, y, w, h), False, None, None, None, False, None, None, card_meta, Data=[ms(m_name, label)])

    # ==============================================================================
    # Page 1: Overview
    # ==============================================================================
    p = page(
        'Tổng quan dữ liệu lúa gạo',
        'Thống kê 9 bảng Fact lớp Gold (GSO, FAOSTAT, USDA, World Bank, ĐBSCL), sắp xếp theo tỷ lệ dữ liệu khuyết giảm dần.',
        f'Nguồn dữ liệu: Tổng hợp 9 bảng Fact lớp Gold · Mốc dữ liệu mới nhất: 03/08/2026 · {batch_tag}',
        nav_label='01 · Tổng quan'
    )
    card(p, 'Total Observations', 'Tổng quan sát', 24, y=96, w=299, h=84, ref='Overview Scope Ref', font_size=16.5)
    card(p, 'Numeric Coverage', 'Tỷ lệ có số', 335, y=96, w=299, h=84, ref='Overview Numeric Ref', font_size=16.5)
    card(p, 'Null Percentage', 'Tỷ lệ khuyết', 646, y=96, w=299, h=84, ref='Overview Null Ref', font_size=16.5)
    card(p, 'Highest Null Fact Pct Display', 'Tỷ lệ NULL cao nhất', 957, y=96, w=299, h=84, ref='Highest Null Fact Ref', font_size=16.5)
    add(p, 'tableEx', 'Danh mục 9 bảng Fact lớp Gold & Tỷ lệ khuyết (Present-NULL)', (24, 190, 844, 484),
        sort=('Overview', 'Null Percentage', True, 'Descending'),
        Values=[
            col('Overview', 'fact_name_vi', 'Bộ dữ liệu'),
            ms('Total Observations', 'Quan sát'),
            ms('Numeric Observations', 'Có số'),
            ms('Present NULL', 'NULL'),
            ms('Null Percentage', '%NULL'),
            col('Overview', 'period_min', 'Kỳ đầu'),
            col('Overview', 'period_max', 'Kỳ cuối'),
            col('Overview', 'represented_periods', 'Số kỳ'),
            col('Overview', 'period_grain_vi', 'Đơn vị')
        ])
    add(p, 'barChart', 'Tỷ lệ dữ liệu khuyết theo bảng Fact (%)', (880, 190, 376, 484),
        sort=('Overview', 'Null Percentage', True, 'Descending'),
        y_axis=('%NULL', 1),
        Category=[col('Overview', 'fact_name_vi', 'Bộ dữ liệu')], Y=[ms('Null Percentage', '%NULL')])

    # ==============================================================================
    # Page 2: Vietnam Production
    # ==============================================================================
    p = page(
        'Sản xuất lúa Việt Nam',
        'Số liệu chính thức theo Niên giám Thống kê Việt Nam (GSO), tách biệt số cả năm và số theo mùa vụ.',
        f'Nguồn dữ liệu: Niên giám Thống kê GSO · Mốc mới nhất: 2024 (Sản lượng, diện tích 1990–2024; Năng suất 1995–2024) · {batch_tag}',
        nav_label='02 · SX Việt Nam',
        pills=[(
            'Phạm vi GSO quốc gia',
            'SL & DT: 1990–2024 · NS: 1995–2024',
            (1080, 186, 176, 80)
        )]
    )
    # Row 1 KPI cards + Year slicer (uniform 4-col KPI grid + right column)
    card(p, 'First Production', '[Lúa] Sản lượng kỳ đầu', 24, y=98, w=254, h=80, ref='First Production Year Ref', font_size=15)
    card(p, 'Latest Production', '[Lúa] Sản lượng kỳ cuối', 288, y=98, w=254, h=80, ref='Latest Production YoY Ref', font_size=15)
    card(p, 'Production Change Pct', '[Lúa] Thay đổi đầu–cuối', 552, y=98, w=254, h=80, ref='Production Change Abs Ref', font_size=15)
    card(p, 'Latest Production YoY', '[Lúa] YoY kỳ cuối', 816, y=98, w=254, h=80, ref='Latest Production Period Ref', ref_color='Latest Production YoY Ref Color', font_size=15)
    add(p, 'slicer', 'Năm', (1080, 98, 176, 80), False, None, hide_slicer_header=True, Values=[col('CalendarYear', 'Year', 'Năm')])

    # Row 2 KPI cards (4 equal cards aligned with Row 1, h=80)
    card(p, 'Peak Production Hero', '[Lúa] Đỉnh sản lượng', 24, y=186, w=254, h=80, ref='Peak Year Ref', font_size=15)
    card(p, 'Latest vs Peak Pct', '[Lúa] So với đỉnh', 288, y=186, w=254, h=80, ref='Latest vs Peak Ref', font_size=15)
    card(p, 'Area Change Pct', '[Lúa] Thay đổi diện tích', 552, y=186, w=254, h=80, ref='Area Span Ref', font_size=15)
    card(p, 'Yield Change Pct', '[Lúa] Thay đổi năng suất', 816, y=186, w=254, h=80, ref='Yield Span Ref', font_size=15)

    # Row 3 & Row 4 Charts
    add(p, 'lineChart', '[Lúa] Sản lượng cả năm (triệu tấn)', (24, 274, 402, 182),
        x_axis=(1990, 2024, 'Năm'), y_axis=('Triệu tấn', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Y=[ms('Annual Production Mil', 'Sản lượng')])
    add(p, 'lineChart', '[Lúa] Diện tích cả năm (triệu ha)', (438, 274, 402, 182),
        x_axis=(1990, 2024, 'Năm'), y_axis=('Triệu ha', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Y=[ms('Annual Area Mil', 'Diện tích')])
    add(p, 'lineChart', '[Lúa] Năng suất lúa cả năm (kg/ha)', (852, 274, 404, 182),
        x_axis=(1995, 2024, 'Năm'), y_axis=('Năng suất (kg/ha)', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Y=[ms('Reported Yield', 'Năng suất')])

    add(p, 'lineChart', '[Lúa] Sản lượng theo vụ · không gồm cả năm (triệu tấn)', (24, 464, 1232, 210),
        x_axis=(1990, 2024, 'Năm'), y_axis=('Triệu tấn', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Series=[col('NSOSeasonal', 'season_label_vi', 'Vụ')], Y=[ms('Seasonal Production Mil', 'Triệu tấn')])

    # ==============================================================================
    # Page 3: International Production
    # ==============================================================================
    p = page(
        'Sản xuất lúa gạo quốc tế',
        'Đối chiếu 5 quốc gia sản xuất & xuất khẩu chủ lực: Ấn Độ, Indonesia, Pakistan, Thái Lan và Việt Nam (1961–2024).',
        f'Nguồn dữ liệu: FAOSTAT Sản xuất · Mốc mới nhất: 2024 (1961–2024; Nhóm 5 nước đã chọn: Ấn Độ, Indonesia, Pakistan, Thái Lan, Việt Nam) · {batch_tag}',
        nav_label='03 · SX Quốc tế',
        pills=[(
            'Phạm vi đối chiếu FAOSTAT · 5 nước chủ lực (1961–2024)',
            'Chọn Năm để cắt lát từng năm hoặc để Tất cả để tự động đối chiếu năm chung mới nhất.',
            (24, 96, 796, 64)
        )]
    )
    add(p, 'slicer', 'Năm', (832, 96, 200, 64), False, None, hide_slicer_header=True, Values=[col('CalendarYear', 'Year', 'Năm')])
    add(p, 'slicer', 'Quốc gia', (1044, 96, 212, 64), False, None, hide_slicer_header=True, Values=[col('FAO', 'area_name_vi', 'Quốc gia')])

    add(p, 'lineChart', '[Lúa] Chỉ số sản lượng lúa so với năm 1961 (1961=100)', (24, 170, 610, 246),
        x_axis=(1961, 2024, 'Năm'), y_axis=('Chỉ số (1961=100)', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Series=[col('FAO', 'area_name_vi', 'Quốc gia')], Y=[ms('FAO Production Index', 'Chỉ số 1961=100')])
    add(p, 'lineChart', '[Lúa] Năng suất cặp hợp lệ (kg/ha)', (646, 170, 610, 246),
        x_axis=(1961, 2024, 'Năm'), y_axis=('Năng suất (kg/ha)', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Series=[col('FAO', 'area_name_vi', 'Quốc gia')], Y=[ms('FAO Weighted Yield', 'Năng suất (kg/ha)')])

    add(p, 'barChart', '[Lúa] Sản lượng lúa tại năm chung (triệu tấn)', (24, 426, 610, 248),
        sort=('Overview', 'FAO Common Production Mil', True, 'Descending'),
        y_axis=('Sản lượng (triệu tấn)', 1), dynamic_title_measure='FAO Common Production Title',
        Category=[col('FAO', 'area_name_vi', 'Quốc gia')], Y=[ms('FAO Common Production Mil', 'Triệu tấn')],
        Tooltips=[ms('FAO Common Year', 'Năm chung'), ms('FAO Common Rank', 'Hạng trong nhóm')])
    add(p, 'scatterChart', '[Lúa] Diện tích và sản lượng · năm chọn hoặc năm chung', (646, 426, 610, 248),
        x_axis=(None, None, 'Diện tích (triệu ha)'), y_axis=('Sản lượng (triệu tấn)', 1),
        dynamic_title_measure='FAO Scatter Title',
        Category=[col('FAO', 'area_name_vi', 'Quốc gia')],
        Series=[col('FAO', 'area_name_vi', 'Quốc gia')],
        X=[ms('FAO Scatter Area Mil', 'Diện tích triệu ha')], Y=[ms('FAO Scatter Production Mil', 'Sản lượng triệu tấn')],
        Size=[ms('FAO Scatter Yield', 'Năng suất kg/ha')], Tooltips=[ms('FAO Scatter Year', 'Năm')])

    # ==============================================================================
    # Page 4: Supply-Demand & Stocks
    # ==============================================================================
    p = page(
        'Cung cầu và tồn kho lúa gạo',
        'Tách biệt hai nguồn theo đúng đơn vị thời gian: Trái — FAOSTAT SUA (năm dương lịch); Phải — USDA PSD (niên vụ).',
        f'Nguồn dữ liệu: FAOSTAT Cân đối cung sử dụng (2010–2023; chỉ công bố Thực phẩm & Chế biến) & Cân đối gạo USDA PSD (1960–2025) · {batch_tag}',
        nav_label='04 · Cung & Cầu',
        pills=[
            ('FAOSTAT SUA · Năm dương lịch (2010–2023)', 'Thực phẩm, chế biến, tồn kho đầu kỳ và sai số thống kê.', (24, 96, 396, 64)),
            ('USDA PSD · Niên vụ Market Year (1960–2025)', 'Sản xuất, tiêu dùng nội địa và tồn kho cuối kỳ.', (646, 96, 396, 64))
        ]
    )
    add(p, 'slicer', 'Năm FAOSTAT SUA', (430, 96, 204, 64), False, None, hide_slicer_header=True, Values=[col('CalendarYear', 'Year', 'Năm')])
    add(p, 'slicer', 'Niên vụ USDA PSD', (1052, 96, 204, 64), False, None, hide_slicer_header=True, Values=[col('PSD', 'market_year', 'Niên vụ')])

    add(p, 'lineChart', '[Gạo xay xát] FAOSTAT SUA · Tiêu thụ thực phẩm & chế biến (triệu tấn)', (24, 170, 610, 188),
        x_axis=(2010, 2023, 'Năm'), y_axis=('Triệu tấn', 1, 0),
        Category=[col('CalendarYear', 'Year', 'Năm')], Y=[ms('SUA Food Supply Mil', 'Thực phẩm'), ms('SUA Processed Mil', 'Chế biến')])
    add(p, 'lineChart', '[Gạo xay xát] USDA PSD · Sản xuất và tiêu dùng (triệu tấn, MY)', (646, 170, 610, 188),
        x_axis=(1960, 2025, 'Niên vụ'), y_axis=('Triệu tấn', 1, 0),
        Category=[col('PSD', 'market_year', 'Niên vụ')], Y=[ms('PSD Production Mil', 'Sản xuất'), ms('PSD Consumption Mil', 'Tiêu dùng')])

    add(p, 'lineChart', '[Gạo xay xát] FAOSTAT SUA · Tồn kho đầu kỳ (triệu tấn)', (24, 368, 610, 144),
        x_axis=(2010, 2023, 'Năm'), y_axis=('Triệu tấn', 1, 0),
        Category=[col('CalendarYear', 'Year', 'Năm')], Y=[ms('SUA Opening Stocks Mil', 'Tồn đầu kỳ')])
    add(p, 'columnChart', '[Gạo xay xát] FAOSTAT SUA · Biến động tồn & Sai số (triệu tấn)', (24, 522, 610, 152),
        x_axis=(2010, 2023, 'Năm'), y_axis=('', 1, -1.5, 1.0),
        Category=[col('CalendarYear', 'Year', 'Năm')], Y=[ms('SUA Stock Variation Mil', 'Biến động tồn'), ms('SUA Residuals Mil', 'Sai số thống kê (residual)')])
    add(p, 'lineChart', '[Gạo xay xát] USDA PSD · Tồn kho cuối kỳ (triệu tấn, MY)', (646, 368, 610, 306),
        x_axis=(1960, 2025, 'Niên vụ'), y_axis=('Triệu tấn', 1, 0),
        Category=[col('PSD', 'market_year', 'Niên vụ')], Y=[ms('PSD Ending Stocks Mil', 'Tồn cuối kỳ')])

    # ==============================================================================
    # Page 5: Prices (Keep v03..v16 indices untouched!)
    # ==============================================================================
    p5_interactions = [
        {'source': 'v06', 'target': 'v14', 'type': 'NoFilter'}
    ]
    p = page(
        'Biến động giá lúa gạo',
        'Giá FOB xuất khẩu (USDA), giá chuẩn quốc tế (World Bank) và giá lúa thu mua nội địa ĐBSCL theo giống.',
        f'Nguồn & Mốc mới nhất: World Bank (12/2025), USDA FOB (2025), Giá nội địa ĐBSCL (03/08/2026) · {batch_tag}',
        interactions=p5_interactions,
        nav_label='05 · Giá lúa gạo',
        pills=[(
            'World Bank · Benchmark giá gạo tham chiếu',
            'Biểu đồ chính giữ đủ 4 chuỗi; MoM & độ biến động theo slicer.',
            (648, 96, 354, 64)
        )]
    )
    # Top slicers (v03..v06, h=64)
    add(p, 'slicer', 'Nước xuất khẩu USDA', (24, 96, 194, 64), True, None, hide_slicer_header=True, Values=[col('USDAPrice', 'location_vi', 'Quốc gia')])
    add(p, 'slicer', 'Phẩm cấp gạo USDA', (226, 96, 208, 64), True, None, hide_slicer_header=True, Values=[col('USDAPrice', 'commodity_class_vi', 'Phẩm cấp gạo')])
    add(p, 'slicer', 'Kỳ tham chiếu USDA', (442, 96, 190, 64), False, None, hide_slicer_header=True, Values=[col('USDAPrice', 'reference_period', 'Kỳ tham chiếu')])
    add(p, 'slicer', 'Chuỗi giá World Bank', (1012, 96, 244, 64), True, None, hide_slicer_header=True,
        Values=[col('WB', 'series_name_vi', 'Chuỗi giá WB')])

    # WB Cards (v07..v09) - 3-tier left-aligned cards (h=82, zero truncation)
    card(p, 'Latest WB Thai 5 Hero', '[Gạo] Thái 5% mới nhất', 648, y=168, w=196, h=82, ref='Latest WB Thai 5 MoM Ref', font_size=14.5)
    card(p, 'Latest WB VN 5 Hero', '[Gạo] VN 5% mới nhất', 854, y=168, w=196, h=82, ref='Latest WB VN 5 MoM Ref', font_size=14.5)
    card(p, 'WB Spread VN5 vs Thai5 Display', '[Gạo] Chênh lệch VN5 − Thái5', 1060, y=168, w=196, h=82, ref='Latest Common WB Ref', font_size=14.5)

    # USDA Line chart (v10)
    add(p, 'lineChart', '', (24, 168, 608, 200),
        x_axis=(1985, 2025, 'Năm tham chiếu'), y_axis=('USD/tấn', 1), dynamic_title_measure='USDA FOB Chart Title',
        Category=[col('USDAPrice', 'reference_year', 'Năm tham chiếu')], Series=[col('USDAPrice', 'location_vi', 'Quốc gia')], Y=[ms('USDA FOB Chart Price', 'USD/tấn')])

    # Domestic Slicers (v11, v12, h=64)
    add(p, 'slicer', 'Thị trường nội địa VN', (24, 376, 298, 64), False, None, hide_slicer_header=True, Values=[col('VNQuote', 'market', 'Thị trường')])
    add(p, 'slicer', 'Giống lúa & Loại giá', (334, 376, 298, 64), True, None, hide_slicer_header=True, Values=[col('VNQuote', 'series_label', 'Giống lúa & Loại giá')])

    # Domestic Quote Line chart (v13)
    add(p, 'lineChart', '', (24, 448, 608, 226),
        y_axis=('VND/kg', 1), dynamic_title_measure='Domestic Quote Chart Title',
        Category=[col('QuoteDate', 'Date', 'Ngày')], Series=[col('VNQuote', 'market', 'Thị trường')], Y=[ms('Domestic Quote Chart Price', 'VND/kg')])

    # WB Benchmark Line chart (v14)
    add(p, 'lineChart', '[Gạo xay xát] Benchmark giá gạo World Bank (USD/tấn)', (648, 258, 608, 178),
        y_axis=('USD/tấn', 1),
        Category=[col('BenchmarkMonth', 'Date', 'Tháng')], Series=[col('WB', 'series_name_vi', 'Chuỗi giá')], Y=[ms('WB Price', 'USD/tấn')],
        Tooltips=[ms('WB MoM', 'MoM'), ms('WB Peak', 'Đỉnh'), ms('WB Minimum', 'Đáy')])

    # WB MoM & Volatility charts (v15, v16)
    add(p, 'lineChart', '', (648, 444, 298, 230),
        y_axis=('Biến động (%)', 1), dynamic_title_measure='WB MoM Title',
        Category=[col('BenchmarkMonth', 'Date', 'Tháng')], Series=[col('WB', 'series_name_vi', 'Chuỗi giá')], Y=[ms('WB MoM Comparison', 'MoM')])
    add(p, 'lineChart', '', (958, 444, 298, 230),
        y_axis=('Biến động (%)', 1), dynamic_title_measure='WB Volatility Title',
        Category=[col('BenchmarkMonth', 'Date', 'Tháng')], Series=[col('WB', 'series_name_vi', 'Chuỗi giá')], Y=[ms('WB Volatility Comparison', 'Volatility 12M')])

    # ==============================================================================
    # Page 6: Trade Exports
    # ==============================================================================
    p = page(
        'Thương mại lúa gạo quốc tế',
        'Xuất khẩu gạo xay xát của Việt Nam sang 7 đối tác thương mại trọng điểm giai đoạn 2019–2023.',
        f'Nguồn dữ liệu: Thương mại FAOSTAT · Mốc mới nhất: 2023 (2019–2023, 7 đối tác chính, không phải tổng xuất khẩu quốc gia) · {batch_tag}',
        nav_label='06 · Thương mại',
        pills=[(
            'Phạm vi FAOSTAT Thương mại · 7 đối tác xuất khẩu trọng điểm (2019–2023)',
            'Lọc Năm hoặc Đối tác để xem quy mô xuất khẩu, đơn giá và giá trị giao dịch.',
            (24, 96, 800, 64)
        )]
    )
    # Row 1 Right Slicers (h=64)
    add(p, 'slicer', 'Năm', (836, 96, 200, 64), False, None, hide_slicer_header=True, Values=[col('CalendarYear', 'Year', 'Năm')])
    add(p, 'slicer', 'Đối tác', (1048, 96, 208, 64), False, None, hide_slicer_header=True, Values=[col('Trade', 'partner_name_vi', 'Đối tác')])

    # Row 2: 6 symmetrical 3-tier KPI cards across 1232px (h=82)
    card(p, 'Trade Partners', '[Gạo] Số đối tác', 24, y=168, w=196, h=82, ref='Trade Partners Ref', font_size=14.5)
    card(p, 'Trade Paired Groups Hero', '[Gạo] Cặp đối tác–năm', 231, y=168, w=196, h=82, ref='Trade Paired Groups Ref', font_size=14.5)
    card(p, 'Latest Trade Quantity Hero', '[Gạo] XK năm gần nhất', 438, y=168, w=196, h=82, ref='Latest Trade Year Ref', font_size=14.5)
    card(p, 'Trade Quantity', '[Gạo] XK 7 nước (19–23)', 645, y=168, w=196, h=82, ref='Trade Span Ref', font_size=14.5)
    card(p, 'Trade Value', '[Gạo] Giá trị cộng dồn', 852, y=168, w=196, h=82, ref='Trade Value Ref', font_size=14.5)
    card(p, 'Trade Unit Value', '[Gạo] Đơn giá bình quân', 1059, y=168, w=197, h=82, ref='Trade Unit Value Ref', font_size=14.5)

    # Charts
    add(p, 'lineChart', '[Gạo xay xát] Lượng xuất khẩu theo đối tác (triệu tấn)', (24, 258, 610, 194),
        x_axis=(2019, 2023, 'Năm'), y_axis=('Triệu tấn', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Series=[col('Trade', 'partner_name_vi', 'Đối tác')], Y=[ms('Trade Quantity Mil', 'Triệu tấn')])
    add(p, 'lineChart', '[Gạo xay xát] Đơn giá báo cáo theo đối tác (USD/tấn)', (646, 258, 610, 194),
        x_axis=(2019, 2023, 'Năm'), y_axis=('USD/tấn', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Series=[col('Trade', 'partner_name_vi', 'Đối tác')], Y=[ms('Trade Unit Value', 'USD/tấn')])
    add(p, 'lineChart', '[Gạo xay xát] Giá trị xuất khẩu theo năm và đối tác (tỷ USD)', (24, 460, 1232, 214),
        x_axis=(2019, 2023, 'Năm'), y_axis=('Tỷ USD', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Series=[col('Trade', 'partner_name_vi', 'Đối tác')], Y=[ms('Trade Value Bil', 'Tỷ USD')])

    return specs


def build_tlcn_theme():
    """Modern airy SaaS TLCN Teal theme with 3-tier cardVisual KPIs, soft slate canvas, and subtle elevation."""
    border_soft = [{'show': True, 'width': 1, 'radius': 10, 'color': {'solid': {'color': '#DCE4EC'}}}]
    shadow_subtle = [{
        'show': True, 'color': {'solid': {'color': '#0F172A'}}, 'preset': 'Custom',
        'angle': 90, 'shadowDistance': 2, 'shadowBlur': 8, 'shadowSpread': 0, 'transparency': 93
    }]
    axis_base = {
        'showAxisTitle': True, 'gridlineStyle': 'dotted',
        'gridlineColor': {'solid': {'color': '#E2E8F0'}},
        'fontSize': 9.5, 'titleFontSize': 9.5, 'labelColor': {'solid': {'color': '#475569'}}
    }
    return {
        'name': 'TLCN Teal',
        'dataColors': ['#0F6B5E', '#2B6CB0', '#D97706', '#7C3AED', '#14B8A6', '#DC2626', '#475569', '#0EA5E9'],
        'foreground': '#0F172A',
        'foregroundNeutralSecondary': '#475569',
        'foregroundNeutralTertiary': '#64748B',
        'background': '#FFFFFF',
        'backgroundLight': '#F8FAFC',
        'backgroundNeutral': '#E2E8F0',
        'tableAccent': '#0F6B5E',
        'shapeStroke': '#DCE4EC',
        'good': '#16A34A',
        'neutral': '#D97706',
        'bad': '#DC2626',
        'textClasses': {
            'largeTitle': {'fontSize': 18, 'fontFace': 'Segoe UI Semibold', 'color': '#0E3B34'},
            'title': {'fontSize': 10.5, 'fontFace': 'Segoe UI Semibold', 'color': '#0F172A'},
            'header': {'fontSize': 10.5, 'fontFace': 'Segoe UI Semibold', 'color': '#0F172A'},
            'callout': {'fontSize': 18, 'fontFace': 'Segoe UI Semibold', 'color': '#0F172A'},
            'label': {'fontSize': 9.5, 'fontFace': 'Segoe UI', 'color': '#475569'}
        },
        'visualStyles': {
            'page': {
                '*': {
                    'outspace': [{'color': {'solid': {'color': '#E4EAF1'}}}],
                    'background': [{'color': {'solid': {'color': '#EFF3F7'}}, 'transparency': 0}]
                }
            },
            '*': {
                '*': {
                    'background': [{'show': True, 'color': {'solid': {'color': '#FFFFFF'}}, 'transparency': 0}],
                    'border': border_soft,
                    'dropShadow': shadow_subtle,
                    'padding': [{'top': 10, 'bottom': 10, 'left': 12, 'right': 12}],
                    'title': [{
                        'show': True, 'fontFamily': 'Segoe UI Semibold', 'fontSize': 10.5,
                        'titleWrap': True
                    }],
                    'categoryAxis': [dict(axis_base, concatenateLabels=False)],
                    'valueAxis': [axis_base],
                    'legend': [{'show': True, 'position': 'Top', 'showTitle': False}],
                    'lineStyles': [{'strokeWidth': 2.5}],
                    'visualHeader': [{'show': True, 'foreground': {'solid': {'color': '#475569'}}, 'background': {'solid': {'color': '#FFFFFF'}}, 'border': {'solid': {'color': '#DCE4EC'}}, 'transparency': 10}],
                    'outspacePane': [{'backgroundColor': {'solid': {'color': '#FFFFFF'}}, 'foregroundColor': {'solid': {'color': '#0F172A'}}, 'borderColor': {'solid': {'color': '#CBD5E1'}}, 'border': True}],
                    'filterCard': [
                        {'$id': 'Applied', 'backgroundColor': {'solid': {'color': '#F2F7F6'}}, 'foregroundColor': {'solid': {'color': '#0F172A'}}, 'borderColor': {'solid': {'color': '#0F6B5E'}}, 'border': True},
                        {'$id': 'Available', 'backgroundColor': {'solid': {'color': '#FFFFFF'}}, 'foregroundColor': {'solid': {'color': '#0F172A'}}, 'borderColor': {'solid': {'color': '#DCE4EC'}}, 'border': True}
                    ]
                }
            },
            'textbox': {
                '*': {
                    'background': [{'show': False}],
                    'border': [{'show': False}],
                    'dropShadow': [{'show': False}],
                    'visualHeader': [{'show': False}],
                    'title': [{'show': False}],
                    'padding': [{'top': 0, 'bottom': 0, 'left': 0, 'right': 0}]
                }
            },
            'pageNavigator': {
                '*': {
                    'background': [{'show': False}],
                    'border': [{'show': False}],
                    'dropShadow': [{'show': False}],
                    'visualHeader': [{'show': False}],
                    'title': [{'show': False}],
                    'padding': [{'top': 0, 'bottom': 0, 'left': 0, 'right': 0}],
                    'shape': [{'$id': 'default', 'tileShape': 'rectangle', 'roundEdge': 8}],
                    'layout': [{'cellPadding': 6}],
                    'outline': [
                        {'show': True},
                        {'$id': 'default', 'lineColor': {'solid': {'color': '#D0DCE5'}}},
                        {'$id': 'hover', 'lineColor': {'solid': {'color': '#0F6B5E'}}},
                        {'$id': 'press', 'lineColor': {'solid': {'color': '#0A4A41'}}},
                        {'$id': 'selected', 'lineColor': {'solid': {'color': '#0F6B5E'}}}
                    ],
                    'fill': [
                        {'show': True},
                        {'$id': 'default', 'fillColor': {'solid': {'color': '#FFFFFF'}}, 'transparency': 0},
                        {'$id': 'hover', 'fillColor': {'solid': {'color': '#E6F4F1'}}, 'transparency': 0},
                        {'$id': 'press', 'fillColor': {'solid': {'color': '#CCE3DF'}}, 'transparency': 0},
                        {'$id': 'selected', 'fillColor': {'solid': {'color': '#0F6B5E'}}, 'transparency': 0}
                    ],
                    'text': [
                        {'$id': 'default', 'fontColor': {'solid': {'color': '#1E3A36'}}, 'fontSize': 9.5, 'fontFamily': 'Segoe UI Semibold', 'leftMargin': 8, 'rightMargin': 8, 'topMargin': 4, 'bottomMargin': 4},
                        {'$id': 'hover', 'fontColor': {'solid': {'color': '#0F6B5E'}}, 'fontSize': 9.5, 'fontFamily': 'Segoe UI Semibold'},
                        {'$id': 'press', 'fontColor': {'solid': {'color': '#0A4A41'}}, 'fontSize': 9.5, 'fontFamily': 'Segoe UI Semibold'},
                        {'$id': 'selected', 'fontColor': {'solid': {'color': '#FFFFFF'}}, 'fontSize': 9.5, 'fontFamily': 'Segoe UI Semibold'}
                    ]
                }
            },
            'slicer': {
                '*': {
                    'visualHeader': [{'show': False}],
                    'border': [{'show': True, 'width': 1, 'radius': 10, 'color': {'solid': {'color': '#DCE4EC'}}}],
                    'dropShadow': [{'show': True, 'color': {'solid': {'color': '#0F172A'}}, 'preset': 'Custom', 'angle': 90, 'shadowDistance': 2, 'shadowBlur': 8, 'shadowSpread': 0, 'transparency': 93}],
                    'items': [{'fontFamily': 'Segoe UI', 'textSize': 9.5, 'fontColor': {'solid': {'color': '#0F172A'}}, 'background': {'solid': {'color': '#FFFFFF'}}, 'padding': 2}],
                    'title': [{'show': True, 'fontFamily': 'Segoe UI Semibold', 'fontSize': 9.5, 'titleWrap': False}],
                    'padding': [{'top': 4, 'bottom': 6, 'left': 10, 'right': 10}]
                }
            },
            'cardVisual': {
                '*': {
                    'background': [{'show': True, 'color': {'solid': {'color': '#FFFFFF'}}, 'transparency': 0}],
                    'border': [{'show': True, 'width': 1, 'radius': 10, 'color': {'solid': {'color': '#DCE4EC'}}}],
                    'dropShadow': [{'show': True, 'color': {'solid': {'color': '#0F172A'}}, 'preset': 'Custom', 'angle': 90, 'shadowDistance': 2, 'shadowBlur': 8, 'shadowSpread': 0, 'transparency': 93}],
                    'visualHeader': [{'show': False}],
                    'title': [{'show': False}],
                    'layout': [{'alignment': 'middle', 'cellPadding': 0, 'maxTiles': 3, 'calloutSize': 70, 'backgroundShow': False, 'paddingIndividual': False, 'paddingUniform': 0}],
                    'shapeCustomRectangle': [{'$id': 'default', 'tileShape': 'rectangleRoundedByPixel', 'rectangleRoundedCurve': 10, 'rectangleRoundedCurveCustomStyle': False}],
                    'fillCustom': [{'$id': 'default', 'show': False}],
                    'outline': [{'$id': 'default', 'show': False}],
                    'shadowCustom': [{'$id': 'default', 'show': False}],
                    'padding': [{'$id': 'default', 'paddingSelection': 'Custom', 'paddingIndividual': True, 'topMargin': 0, 'rightMargin': 0, 'bottomMargin': 0, 'leftMargin': 0}],
                    'spacing': [{'$id': 'default', 'verticalSpacing': 5}],
                    'divider': [{'$id': 'default', 'show': False}],
                    'label': [{'$id': 'default', 'position': 'aboveValue', 'horizontalAlignment': 'left', 'alignBaselines': False, 'textWrap': False, 'fontSize': 9, 'fontFamily': 'Segoe UI', 'fontColor': {'solid': {'color': '#475569'}}}],
                    'value': [{'$id': 'default', 'horizontalAlignment': 'left', 'bold': True, 'fontFamily': "'Segoe UI Semibold', wf_segoe-ui_semibold, helvetica, arial, sans-serif", 'fontColor': {'solid': {'color': '#0F172A'}}}],
                    'referenceLabel': [{'$id': 'default', 'backgroundShow': False, 'paddingIndividual': False, 'paddingUniform': 0}],
                    'referenceLabelTitle': [{'$id': 'default', 'show': False}],
                    'referenceLabelValue': [{'$id': 'default', 'valueFontFamily': "'Segoe UI Semibold', wf_segoe-ui_semibold, helvetica, arial, sans-serif", 'valueFontSize': 9, 'valueBold': True, 'valueFontColor': {'solid': {'color': '#475569'}}}],
                    'referenceLabelLayout': [{'$id': 'default', 'arrangement': 'rows', 'style': 'sentence', 'horizontalAlignment': 'left', 'verticalAlignment': 'middle', 'labelSpace': 0, 'outerPadding': 0}]
                }
            },
            'card': {
                '*': {
                    'visualHeader': [{'show': False}],
                    'border': [{'show': True, 'width': 1, 'radius': 10, 'color': {'solid': {'color': '#DCE4EC'}}}],
                    'title': [{'show': True, 'fontFamily': 'Segoe UI Semibold', 'fontSize': 9.5, 'titleWrap': True}],
                    'padding': [{'top': 6, 'bottom': 6, 'left': 6, 'right': 6}]
                }
            },
            'tableEx': {
                '*': {
                    'stylePreset': [{'name': 'None'}],
                    'grid': [{'gridHorizontal': True, 'gridHorizontalColor': {'solid': {'color': '#E2E8F0'}}, 'gridHorizontalWeight': 1, 'rowPadding': 6, 'outlineColor': {'solid': {'color': '#0F6B5E'}}}],
                    'columnHeaders': [{'fontColor': {'solid': {'color': '#FFFFFF'}}, 'backColor': {'solid': {'color': '#0F6B5E'}}, 'fontFamily': 'Segoe UI Semibold'}],
                    'values': [{'backColorPrimary': {'solid': {'color': '#FFFFFF'}}, 'backColorSecondary': {'solid': {'color': '#F8FAFC'}}}],
                    'total': [{'fontColor': {'solid': {'color': '#0F5C51'}}, 'backColor': {'solid': {'color': '#E8F2EF'}}}]
                }
            },
            'barChart': {
                '*': {
                    'legend': [{'show': False}],
                    'categoryAxis': [{'showAxisTitle': False}],
                    'valueAxis': [{'showAxisTitle': True, 'gridlineStyle': 'dotted'}]
                }
            },
            'scatterChart': {
                '*': {
                    'legend': [{'show': True, 'position': 'Top', 'showTitle': False}],
                    'fillPoint': [{'show': True}],
                    'bubbles': [{'bubbleSize': 0, 'markerRangeType': 'auto'}]
                }
            }
        }
    }


def styled_textbox(identifier, paragraphs, x, y, w, h, bg, border_color=None, radius=6, shadow=False, padding=(6, 6, 12, 12)):
    """Build a styled textbox visual with explicit background, optional border/shadow, and hidden header."""
    tb = visual(identifier, 'textbox', '', x, y, w, h)
    pad_top, pad_bottom, pad_left, pad_right = padding
    c_objs = {
        'title': [{'properties': {'show': literal(False)}}],
        'visualHeader': [{'properties': {'show': literal(False)}}],
        'background': [{'properties': {'show': literal(True), 'color': {'solid': {'color': literal(bg)}}, 'transparency': literal(0)}}],
        'border': [{'properties': (
            {'show': literal(True), 'color': {'solid': {'color': literal(border_color)}}, 'radius': literal(radius), 'width': literal(1)}
            if border_color else {'show': literal(False)}
        )}],
        'dropShadow': [{'properties': (
            {'show': literal(True), 'color': {'solid': {'color': literal('#0D2F2A')}}, 'preset': literal('Custom'),
             'angle': literal(90), 'shadowDistance': literal(2), 'shadowBlur': literal(6), 'shadowSpread': literal(0), 'transparency': literal(88)}
            if shadow else {'show': literal(False)}
        )}],
        'padding': [{'properties': {'top': literal(pad_top), 'bottom': literal(pad_bottom), 'left': literal(pad_left), 'right': literal(pad_right)}}]
    }
    tb['visual']['visualContainerObjects'] = c_objs
    tb['visual']['objects'] = {'general': [{'properties': {'paragraphs': [{'textRuns': [run]} for run in paragraphs]}}]}
    return tb


def build_project(root, bundle):
    root = Path(root).resolve()
    bundle = Path(bundle).resolve()
    manifest = verify_bundle(bundle)
    if manifest['format_version'] != 'powerbi-csv-2':
        raise ValueError('Report requires a verified mart bundle')

    model = root / 'M'
    report = root / 'R'
    definition = model / 'definition'
    definition.mkdir(parents=True, exist_ok=True)

    write_json(root / 'TLCN_Rice_Analytics.pbip', {
        '$schema': BASE + 'pbip/pbipProperties/1.0.0/schema.json',
        'version': '1.0',
        'artifacts': [{'report': {'path': 'R'}}],
        'settings': {'enableAutoRecovery': True}
    })
    write_json(model / 'definition.pbism', {
        '$schema': BASE + 'item/semanticModel/definitionProperties/1.0.0/schema.json',
        'version': '4.0',
        'settings': {}
    })
    write_json(report / 'definition.pbir', {
        '$schema': BASE + 'item/report/definitionProperties/2.0.0/schema.json',
        'version': '4.0',
        'datasetReference': {'byPath': {'path': '../M'}}
    })

    (definition / 'database.tmdl').write_text('database TLCN_Rice_Analytics\n\tcompatibilityLevel: 1600\n', encoding='utf-8')
    (definition / 'model.tmdl').write_text('model Model\n\tculture: vi-VN\n\tdefaultPowerBIDataSourceVersion: powerBI_V3\n\tsourceQueryCulture: en-US\n\tdiscourageImplicitMeasures\n\tmaxParallelismPerRefresh: 1\n\tannotation PBI_ProTooling = ["DevMode"]\n\tannotation __PBI_TimeIntelligenceEnabled = 0\n', encoding='utf-8')

    path = str(bundle).replace('"', '""')
    (definition / 'expressions.tmdl').write_text('expression ExportFolder = "' + path + '" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]\n\tkind: m\n', encoding='utf-8')

    tables = definition / 'tables'
    tables.mkdir(exist_ok=True)
    for filename, name in TABLES.items():
        csv_schema = CSV_SCHEMAS[filename]
        added_cols = TABLE_EXTRA_COLUMNS.get(filename)
        full_table_schema = SCHEMAS[filename]
        m_code = m_import(filename, csv_schema, added_cols)
        (tables / (name + '.tmdl')).write_text(tmdl_table(name, full_table_schema, 'm', m_code, MEASURES if name == 'Overview' else None), encoding='utf-8')

    calendar = 'let ys=List.Combine({NSOAnnual[year],FAO[year],SUA[year],Trade[year]}), rows=Table.FromList({List.Min(ys)..List.Max(ys)},Splitter.SplitByNothing(),{"Year"}), typed=Table.TransformColumnTypes(rows,{{"Year",Int64.Type}}) in typed'
    (tables / 'CalendarYear.tmdl').write_text(tmdl_table('CalendarYear', [('Year', 'integer')], 'm', calendar), encoding='utf-8')

    for name, base, col in [('BenchmarkMonth', 'WB', 'month_date'), ('QuoteDate', 'VNQuote', 'date')]:
        source = 'let lo=List.Min(VNQuote[date]), hi=List.Max(VNQuote[date]), rows=Table.FromList(List.Dates(lo,Duration.Days(hi-lo)+1,#duration(1,0,0,0)),Splitter.SplitByNothing(),{"Date"}) in Table.TransformColumnTypes(rows,{{"Date",type date}})' if name == 'QuoteDate' else 'Table.RenameColumns(Table.Distinct(Table.SelectColumns(WB,{"month_date"})),{{"month_date","Date"}})'
        (tables / (name + '.tmdl')).write_text(tmdl_table(name, [('Date', 'text')], 'm', source), encoding='utf-8')

    relations = []
    for name in ('NSOAnnual', 'NSOSeasonal', 'NSOYield', 'FAO', 'SUA', 'Trade'):
        relations.extend([f'relationship {uuid.uuid5(uuid.NAMESPACE_URL, "tlcn/" + name)}', f'\tfromColumn: {name}.year', '\ttoColumn: CalendarYear.Year', '\tcrossFilteringBehavior: oneDirection', ''])
    for name, src, col in [('BenchmarkMonth', 'WB', 'month_date'), ('QuoteDate', 'VNQuote', 'date')]:
        relations.extend([f'relationship {uuid.uuid5(uuid.NAMESPACE_URL, "tlcn/" + name)}', f'\tfromColumn: {src}.{col}', f'\ttoColumn: {name}.Date', '\tcrossFilteringBehavior: oneDirection', ''])
    (definition / 'relationships.tmdl').write_text('\n'.join(relations), encoding='utf-8')

    rd = report / 'definition'
    rd.mkdir(parents=True, exist_ok=True)
    write_json(rd / 'version.json', {'$schema': BASE + 'item/report/definition/versionMetadata/1.0.0/schema.json', 'version': '2.0.0'})

    theme = build_tlcn_theme()
    write_json(root / 'theme.json', theme)
    write_json(report / 'StaticResources/RegisteredResources/TLCN.json', theme)
    write_json(rd / 'report.json', {
        '$schema': BASE + 'item/report/definition/report/2.0.0/schema.json',
        'themeCollection': {'baseTheme': {'name': 'CY24SU06', 'reportVersionAtImport': '5.55', 'type': 'SharedResources'}, 'customTheme': {'name': 'TLCN.json', 'reportVersionAtImport': '5.55', 'type': 'RegisteredResources'}},
        'resourcePackages': [{'name': 'RegisteredResources', 'type': 'RegisteredResources', 'items': [{'name': 'TLCN.json', 'path': 'TLCN.json', 'type': 'CustomTheme'}]}],
        'settings': {'defaultFilterActionIsDataFilter': True, 'useStylableVisualContainerHeader': True, 'exportDataMode': 'AllowSummarized'}
    })

    page_ids = []
    inventory = []
    batch_short = manifest['published_batch'][:12]
    export_date = str(manifest.get('export_timestamp', ''))[:10]
    for i, p in enumerate(pages(batch_short=batch_short, export_date=export_date), 1):
        pid = f'p{i:02}'
        page_ids.append(pid)
        pd = rd / 'pages' / pid

        page_data = {
            '$schema': BASE + 'item/report/definition/page/2.0.0/schema.json',
            'name': pid,
            'displayName': p.get('nav_label') or f'{i:02} · ' + p['title'],
            'displayOption': 'FitToPage',
            'width': 1280,
            'height': 720
        }
        if p.get('interactions'):
            page_data['visualInteractions'] = p['interactions']

        if i in (2, 3, 4, 6):
            lo, hi = {2: (1990, 2024), 3: (1961, 2024), 4: (2010, 2023), 6: (2019, 2023)}[i]
            page_data['filterConfig'] = {
                'filters': [
                    {
                        'name': pid + '_eligible_years',
                        'field': field('CalendarYear', 'Year'),
                        'type': 'Categorical',
                        'filter': {
                            'Version': 2,
                            'From': [{'Name': 'y', 'Entity': 'CalendarYear', 'Type': 0}],
                            'Where': [{
                                'Condition': {
                                    'In': {
                                        'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'y'}}, 'Property': 'Year'}}],
                                        'Values': [[{'Literal': {'Value': str(y) + 'L'}}] for y in range(lo, hi + 1)]
                                    }
                                }
                            }]
                        },
                        'isLockedInViewMode': True
                    }
                ]
            }
        write_json(pd / 'page.json', page_data)

        # Header banner (full width) + unified single-row pill navigation bar
        banner = styled_textbox(
            'v01',
            [
                {'value': p['title'], 'textStyle': {'fontFamily': 'Segoe UI Semibold', 'fontSize': '14pt', 'fontWeight': 'bold', 'color': '#FFFFFF'}},
                {'value': p['note'], 'textStyle': {'fontFamily': 'Segoe UI', 'fontSize': '8.5pt', 'color': '#C5E3DC'}}
            ],
            0, 0, 1280, 52, bg='#0E4D44', border_color=None, radius=0, shadow=True, padding=(6, 4, 24, 24)
        )
        nav = visual('v02', 'pageNavigator', '', 24, 57, 1232, 34)
        all_visuals = [banner, nav]

        for j, item in enumerate(p['visuals'], 3):
            kind, title, pos, roles = item[0], item[1], item[2], item[3]
            single_select = item[4] if len(item) > 4 else False
            sort = item[5] if len(item) > 5 else None
            x_axis = item[6] if len(item) > 6 else None
            y_axis = item[7] if len(item) > 7 else None
            hide_slicer_header = item[8] if len(item) > 8 else False
            filter_config = item[9] if len(item) > 9 else None
            dynamic_title_measure = item[10] if len(item) > 10 else None
            card_meta = item[11] if len(item) > 11 else None
            all_visuals.append(visual(
                f'v{j:02}', kind, title, *pos, roles=roles, single_select=single_select, sort=sort,
                x_axis=x_axis, y_axis=y_axis, hide_slicer_header=hide_slicer_header, filter_config=filter_config,
                dynamic_title_measure=dynamic_title_measure, card_meta=card_meta
            ))

        # Subtle styled business footer bar per page
        note = styled_textbox(
            'v90',
            [{'value': p['footer'], 'textStyle': {'fontFamily': 'Segoe UI', 'fontSize': '8.5pt', 'color': '#475569'}}],
            24, 680, 1232, 30, bg='#E5EBF1', border_color='#D3DCE6', radius=8, shadow=False, padding=(6, 4, 12, 12)
        )
        all_visuals.append(note)

        # Optional context / section pills (v91, v92) so v03..v16 indices stay stable
        for idx, (pill_title, pill_sub, pill_pos) in enumerate(p.get('pills', []), 91):
            all_visuals.append(styled_textbox(
                f'v{idx:02}',
                [
                    {'value': pill_title, 'textStyle': {'fontFamily': 'Segoe UI Semibold', 'fontSize': '9.5pt', 'fontWeight': 'bold', 'color': '#0F5C51'}},
                    {'value': pill_sub, 'textStyle': {'fontFamily': 'Segoe UI', 'fontSize': '8.5pt', 'color': '#475569'}}
                ],
                *pill_pos, bg='#FFFFFF', border_color='#DCE4EC', radius=10, shadow=False, padding=(10, 6, 12, 12)
            ))

        # Clean existing visual dirs on disk for this page before writing
        page_vis_dir = pd / 'visuals'
        if page_vis_dir.exists():
            import shutil
            shutil.rmtree(page_vis_dir)
        for v in all_visuals:
            write_json(pd / 'visuals' / v['name'] / 'visual.json', v)
        inventory.append({'page': pid, 'title': p['title'], 'visuals': len(all_visuals), 'note': p['note']})

    write_json(rd / 'pages/pages.json', {'$schema': BASE + 'item/report/definition/pagesMetadata/1.0.0/schema.json', 'pageOrder': page_ids, 'activePageName': 'p01'})
    write_json(root / 'report_inventory.json', {
        'project': 'TLCN_Rice_Analytics',
        'batch': manifest['published_batch'],
        'bundle_format': manifest['format_version'],
        'pages': inventory,
        'measures': MEASURES,
        'tables': TABLES,
        'desktop_validation': 'PENDING'
    })
    return root / 'TLCN_Rice_Analytics.pbip'
