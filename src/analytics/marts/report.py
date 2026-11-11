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

measure('Latest Common WB Month', 'MAXX(FILTER(ALL(WB[month_date]), CALCULATE(COUNTROWS(WB), WB[series_code] = "rice_thai_5", NOT ISBLANK(WB[price_usd_t])) > 0 && CALCULATE(COUNTROWS(WB), WB[series_code] = "rice_viet_namese_5", NOT ISBLANK(WB[price_usd_t])) > 0), WB[month_date])', 'yyyy-MM-dd', 'Tháng mới nhất chung có giá của cả Thái 5% và VN 5%.')
measure('WB Spread VN5 vs Thai5', 'VAR m = [Latest Common WB Month] VAR vn = CALCULATE(MAX(WB[price_usd_t]), REMOVEFILTERS(WB), WB[series_code] = "rice_viet_namese_5", WB[month_date] = m) VAR thai = CALCULATE(MAX(WB[price_usd_t]), REMOVEFILTERS(WB), WB[series_code] = "rice_thai_5", WB[month_date] = m) RETURN IF(NOT ISBLANK(vn) && NOT ISBLANK(thai), vn - thai)', '#,0.00', 'Chênh lệch giá World Bank: Việt Nam 5% − Thái 5% (USD/tấn).')
measure('WB Spread VN5 vs Thai5 Display', 'IF(ISBLANK([WB Spread VN5 vs Thai5]), "Không áp dụng", FORMAT([WB Spread VN5 vs Thai5], "+$#,0.00;-$#,0.00;$0.00") & " /tấn")', '@', 'Thẻ chênh lệch giá VN 5% − Thái 5%.')

measure('WB MoM Comparison', 'IF(HASONEVALUE(WB[series_code]), [WB MoM], IF(SELECTEDVALUE(WB[series_code]) IN {"rice_thai_5", "rice_viet_namese_5"}, [WB MoM]))', '+0.00%;-0.00%;0.00%', 'Biến động MoM: 2 chuỗi 5% đối sánh mặc định, hoặc chuỗi được chọn.')
measure('WB Volatility Comparison', 'IF(HASONEVALUE(WB[series_code]), [WB Volatility], IF(SELECTEDVALUE(WB[series_code]) IN {"rice_thai_5", "rice_viet_namese_5"}, [WB Volatility]))', '0.00%', 'Độ biến động 12M: 2 chuỗi 5% đối sánh mặc định, hoặc chuỗi được chọn.')
measure('WB MoM Title', 'IF(HASONEVALUE(WB[series_code]), "[Gạo xay xát] Biến động MoM – " & SELECTEDVALUE(WB[series_name_vi]) & " (%)", "[Gạo xay xát] Biến động MoM – Gạo Thái 5% & VN 5% (%)")', '@', 'Tiêu đề động biểu đồ MoM.')
measure('WB Volatility Title', 'IF(HASONEVALUE(WB[series_code]), "[Gạo xay xát] Biến động 12M – " & SELECTEDVALUE(WB[series_name_vi]) & " (VN khuyết 12/2005–03/2010)", "[Gạo xay xát] Biến động 12M – Thái 5% & VN 5% (VN khuyết 12/2005–03/2010)")', '@', 'Tiêu đề động biểu đồ biến động 12M kèm ghi chú khoảng khuyết dữ liệu VN.')

# Domestic Quote: guard commodity AND price_type; lock v13 standard series and return BLANK() on invalid/missing quotes
measure('Domestic Quote', 'IF(HASONEVALUE(VNQuote[commodity]) && HASONEVALUE(VNQuote[price_type]), IF(HASONEVALUE(QuoteDate[Date]), MAX(VNQuote[price_vnd_kg]), AVERAGE(VNQuote[price_vnd_kg])))', '#,0', 'Vietnam domestic quote VND/kg; guardrail: yêu cầu chọn đúng 1 giống lúa và 1 loại giá xác định.')
measure('Domestic Quoted Dates', 'DISTINCTCOUNT(VNQuote[date])', '#,0', 'Số ngày có báo giá duy nhất trong phạm vi chọn.')
measure('Domestic Quote Chart Price', 'VAR chosen_series = "Lúa IR 50404 (lúa tươi) · Thương lái thu mua" VAR valid_rows = FILTER(VNQuote, VNQuote[series_label] = chosen_series && NOT ISBLANK(VNQuote[price_vnd_kg]) && VNQuote[price_vnd_kg] > 0) RETURN IF(COUNTROWS(valid_rows) > 0, AVERAGEX(valid_rows, VNQuote[price_vnd_kg]), BLANK())', '#,0', 'Giá nội địa chuỗi chuẩn (Lúa IR 50404 tươi · Thương lái thu mua); trả BLANK() khi không có quan sát hợp lệ.')
measure('Domestic Quote Chart Title', 'VAR chosen_series = "Lúa IR 50404 (lúa tươi) · Thương lái thu mua" VAR valid_markets = FILTER(VALUES(VNQuote[market]), CALCULATE(COUNTROWS(VNQuote), VNQuote[series_label] = chosen_series, NOT ISBLANK(VNQuote[price_vnd_kg]), VNQuote[price_vnd_kg] > 0) > 0) VAR market_list = CONCATENATEX(valid_markets, VNQuote[market], ", ", VNQuote[market], ASC) RETURN IF(ISBLANK(market_list) || market_list = "", "[Lúa] Giá nội địa – " & chosen_series & " (không có dữ liệu)", "[Lúa] Giá nội địa (" & market_list & ") – " & chosen_series & " (VND/kg)")', '@', 'Tiêu đề động biểu đồ giá nội địa liệt kê đúng tập thị trường hiển thị.')

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
# CARD DISPLAY MEASURES
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
           x_axis=None, y_axis=None, hide_slicer_header=False, filter_config=None, dynamic_title_measure=None):
    title_obj = {
        'show': literal(bool(title) or bool(dynamic_title_measure)),
        'fontSize': literal(12),
        'fontColor': {'solid': {'color': literal('#17342F')}}
    }
    if dynamic_title_measure:
        title_obj['text'] = {'expr': {'Measure': {'Expression': {'SourceRef': {'Entity': 'Overview'}}, 'Property': dynamic_title_measure}}}
    else:
        title_obj['text'] = literal(title)

    config = {
        'visualType': kind,
        'drillFilterOtherVisuals': True,
        'visualContainerObjects': {
            'title': [{'properties': title_obj}],
            'background': [{'properties': {'show': literal(True), 'color': {'solid': {'color': literal('#FFFFFF')}}, 'transparency': literal(0)}}]
        }
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
    if kind == 'card' and roles:
        name = next(iter(roles.values()))[0][1]
        fmt = MEASURES.get(name, {}).get('format', '')
        if name in ('Latest WB Thai 5 Display', 'Latest WB VN 5 Display', 'WB Spread VN5 vs Thai5 Display', 'Latest Trade Quantity Display', 'Trade Paired Groups Display'):
            font_size = 14
        elif fmt != '@' or name.endswith(' Display'):
            font_size = 20
        else:
            font_size = 11
        objs['labels'] = [{'properties': {'labelDisplayUnits': literal(1), 'labelPrecision': literal(2 if '.00' in fmt else 0), 'fontSize': literal(font_size)}}]
        objs['categoryLabels'] = [{'properties': {'show': literal(False)}}]
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

    def page(title, note, footer_freshness, interactions=None):
        p = {'title': title, 'note': note, 'footer': footer_freshness, 'interactions': interactions or [], 'visuals': []}
        specs.append(p)
        return p

    def add(p, kind, title, pos, single_select=False, sort=None, x_axis=None, y_axis=None,
            hide_slicer_header=False, filter_config=None, dynamic_title_measure=None, **roles):
        p['visuals'].append((kind, title, pos, roles, single_select, sort, x_axis, y_axis, hide_slicer_header, filter_config, dynamic_title_measure))

    def card(p, name, label, x, y=112, w=290, h=65):
        m_name = name if name.endswith(' Display') else name + ' Display'
        add(p, 'card', label, (x, y, w, h), False, None, Values=[ms(m_name, label)])

    # ==============================================================================
    # Page 1: Overview (R4: 1 bảng 9 Fact sắp xếp theo %NULL giảm dần, tên tiếng Việt, vi-VN format; S3: Sửa thẻ bị cắt)
    # ==============================================================================
    p = page(
        'Tổng quan dữ liệu lúa gạo',
        'Bảng thống kê 9 tập dữ liệu lúa gạo, sắp xếp theo tỷ lệ dữ liệu khuyết giảm dần.',
        f'Nguồn dữ liệu: Tổng hợp 9 bảng Fact lớp Gold (GSO, FAOSTAT, USDA, World Bank, ĐBSCL) · Mốc dữ liệu mới nhất: 03/08/2026 · {batch_tag}'
    )
    card(p, 'Total Observations', 'Tổng quan sát', 32, y=120, w=290, h=90)
    card(p, 'Numeric Coverage', 'Tỷ lệ có số', 336, y=120, w=290, h=90)
    card(p, 'Null Percentage', 'Tỷ lệ khuyết', 640, y=120, w=290, h=90)
    card(p, 'Highest Null Fact Pct Display', 'Fact khuyết cao nhất: USDA · Giá XK tham chiếu', 944, y=120, w=290, h=90)
    add(p, 'tableEx', 'Danh mục 9 bảng Fact lớp Gold & Tỷ lệ khuyết (Present-NULL)', (32, 220, 1216, 424),
        sort=('Overview', 'Null Percentage', True, 'Descending'),
        Values=[
            col('Overview', 'fact_name_vi', 'Bộ dữ liệu'),
            col('Overview', 'rows_total', 'Số quan sát'),
            col('Overview', 'numeric_rows', 'Có số'),
            col('Overview', 'present_null_rows', 'NULL'),
            ms('Null Percentage', '%NULL'),
            col('Overview', 'period_min', 'Kỳ đầu'),
            col('Overview', 'period_max', 'Kỳ cuối'),
            col('Overview', 'represented_periods', 'Số kỳ hiện diện'),
            col('Overview', 'period_grain_vi', 'Đơn vị thời gian')
        ])

    # ==============================================================================
    # Page 2: Vietnam Production (S1: Measure chia sẵn triệu tấn/ha; R11: Bổ sung 4 thẻ KPI)
    # ==============================================================================
    p = page(
        'Sản xuất lúa Việt Nam',
        'Số liệu chính thức theo Niên giám Thống kê Việt Nam, tách biệt số cả năm và số theo mùa vụ.',
        f'Nguồn dữ liệu: Niên giám Thống kê GSO · Mốc mới nhất: 2024 (Sản lượng, diện tích 1990–2024; Năng suất 1995–2024) · {batch_tag}'
    )
    # Row 1 KPI cards
    card(p, 'First Production', '[Lúa] Sản lượng kỳ đầu', 32, y=110, w=290, h=65)
    card(p, 'Latest Production', '[Lúa] Sản lượng kỳ cuối', 336, y=110, w=290, h=65)
    card(p, 'Production Change Pct', '[Lúa] Thay đổi đầu–cuối', 640, y=110, w=290, h=65)
    card(p, 'Latest Production YoY', '[Lúa] YoY kỳ cuối', 944, y=110, w=208, h=65)
    add(p, 'slicer', 'Năm', (1160, 110, 88, 135), False, None, hide_slicer_header=True, Values=[col('CalendarYear', 'Year', 'Năm')])

    # Row 2 KPI cards (R11: Peak, Peak vs Latest, Area change, Yield change)
    card(p, 'Peak Production', '[Lúa] Đỉnh sản lượng (năm)', 32, y=180, w=290, h=65)
    card(p, 'Latest vs Peak Pct', '[Lúa] So với đỉnh', 336, y=180, w=290, h=65)
    card(p, 'Area Change Pct', '[Lúa] Thay đổi diện tích', 640, y=180, w=290, h=65)
    card(p, 'Yield Change Pct', '[Lúa] Thay đổi năng suất', 944, y=180, w=208, h=65)

    # Line charts (S1: measure chia sẵn / 1e6; tiêu đề khớp trục triệu tấn / triệu ha)
    add(p, 'lineChart', '[Lúa] Sản lượng cả năm (triệu tấn)', (32, 255, 390, 175),
        x_axis=(1990, 2024, 'Năm'), y_axis=('Triệu tấn', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Y=[ms('Annual Production Mil', 'Sản lượng')])
    add(p, 'lineChart', '[Lúa] Diện tích cả năm (triệu ha)', (438, 255, 390, 175),
        x_axis=(1990, 2024, 'Năm'), y_axis=('Triệu ha', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Y=[ms('Annual Area Mil', 'Diện tích')])
    add(p, 'lineChart', '[Lúa] Năng suất lúa cả năm (kg/ha)', (844, 255, 404, 175),
        x_axis=(1995, 2024, 'Năm'), y_axis=('Năng suất (kg/ha)', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Y=[ms('Reported Yield', 'Năng suất')])

    add(p, 'lineChart', '[Lúa] Sản lượng theo vụ · không gồm cả năm (triệu tấn)', (32, 440, 1216, 205),
        x_axis=(1990, 2024, 'Năm'), y_axis=('Triệu tấn', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Series=[col('NSOSeasonal', 'season_label_vi', 'Vụ')], Y=[ms('Seasonal Production Mil', 'Triệu tấn')])

    # ==============================================================================
    # Page 3: International Production (R7: Chặn trục 1961-2024; R8: Tiếng Việt, R9: 5 nước; S1: triệu tấn)
    # ==============================================================================
    p = page(
        'Sản xuất lúa gạo quốc tế',
        '5 quốc gia sản xuất lúa lớn: Ấn Độ, Indonesia, Pakistan, Thái Lan và Việt Nam.',
        f'Nguồn dữ liệu: FAOSTAT Sản xuất · Mốc mới nhất: 2024 (1961–2024; Nhóm 5 nước đối thủ xuất khẩu / sản xuất chính đã chọn: Ấn Độ, Indonesia, Pakistan, Thái Lan, Việt Nam) · {batch_tag}'
    )
    add(p, 'slicer', 'Năm', (860, 120, 180, 55), False, None, hide_slicer_header=True, Values=[col('CalendarYear', 'Year', 'Năm')])
    add(p, 'slicer', 'Quốc gia', (1056, 120, 192, 55), False, None, hide_slicer_header=True, Values=[col('FAO', 'area_name_vi', 'Quốc gia')])

    add(p, 'lineChart', '[Lúa] Chỉ số sản lượng lúa so với năm 1961 (1961=100)', (32, 192, 590, 195),
        x_axis=(1961, 2024, 'Năm'), y_axis=('Chỉ số (1961=100)', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Series=[col('FAO', 'area_name_vi', 'Quốc gia')], Y=[ms('FAO Production Index', 'Chỉ số 1961=100')])
    add(p, 'lineChart', '[Lúa] Năng suất cặp hợp lệ (kg/ha)', (638, 192, 610, 195),
        x_axis=(1961, 2024, 'Năm'), y_axis=('Năng suất (kg/ha)', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Series=[col('FAO', 'area_name_vi', 'Quốc gia')], Y=[ms('FAO Weighted Yield', 'Năng suất (kg/ha)')])

    add(p, 'barChart', '[Lúa] Sản lượng lúa tại năm chung (triệu tấn)', (32, 404, 590, 235),
        sort=('Overview', 'FAO Common Production Mil', True, 'Descending'),
        y_axis=('Sản lượng (triệu tấn)', 1), dynamic_title_measure='FAO Common Production Title',
        Category=[col('FAO', 'area_name_vi', 'Quốc gia')], Y=[ms('FAO Common Production Mil', 'Triệu tấn')],
        Tooltips=[ms('FAO Common Year', 'Năm chung'), ms('FAO Common Rank', 'Hạng trong nhóm')])
    add(p, 'scatterChart', '[Lúa] Diện tích và sản lượng · năm chọn hoặc năm chung', (638, 404, 610, 235),
        x_axis=(None, None, 'Diện tích (triệu ha)'), y_axis=('Sản lượng (triệu tấn)', 1),
        Category=[col('FAO', 'area_name_vi', 'Quốc gia')],
        X=[ms('FAO Scatter Area Mil', 'Diện tích triệu ha')], Y=[ms('FAO Scatter Production Mil', 'Sản lượng triệu tấn')],
        Size=[ms('FAO Scatter Yield', 'Năng suất kg/ha')], Tooltips=[ms('FAO Scatter Year', 'Năm')])

    # ==============================================================================
    # Page 4: Supply-Demand & Stocks (S1: Triệu tấn; S7: Sửa 2 chart SUA và chữ gạo)
    # ==============================================================================
    p = page(
        'Cung cầu và tồn kho lúa gạo',
        'Trái: Cân đối cung sử dụng FAOSTAT SUA. Phải: Cân đối cung cầu gạo USDA PSD.',
        f'Nguồn dữ liệu: FAOSTAT Cân đối cung sử dụng (2010–2023; chỉ công bố Thực phẩm & Chế biến) & Cân đối gạo USDA PSD (1960–2025) · {batch_tag}'
    )
    add(p, 'slicer', 'Năm FAOSTAT SUA', (32, 120, 290, 55), False, None, hide_slicer_header=True, Values=[col('CalendarYear', 'Year', 'Năm')])
    add(p, 'slicer', 'Niên vụ USDA PSD', (638, 120, 610, 55), False, None, hide_slicer_header=True, Values=[col('PSD', 'market_year', 'Niên vụ')])

    add(p, 'lineChart', '[Gạo xay xát] FAOSTAT SUA · Năm dương lịch: Tiêu thụ thực phẩm & chế biến (triệu tấn)', (32, 180, 590, 195),
        x_axis=(2010, 2023, 'Năm'), y_axis=('Triệu tấn', 1, 0),
        Category=[col('CalendarYear', 'Year', 'Năm')], Y=[ms('SUA Food Supply Mil', 'Thực phẩm'), ms('SUA Processed Mil', 'Chế biến')])
    add(p, 'lineChart', '[Gạo xay xát] USDA PSD · Niên vụ: Sản xuất và tiêu dùng (triệu tấn, MY)', (638, 180, 610, 195),
        x_axis=(1960, 2025, 'Niên vụ'), y_axis=('Triệu tấn', 1, 0),
        Category=[col('PSD', 'market_year', 'Niên vụ')], Y=[ms('PSD Production Mil', 'Sản xuất'), ms('PSD Consumption Mil', 'Tiêu dùng')])

    add(p, 'lineChart', '[Gạo xay xát] FAOSTAT SUA · Năm dương lịch: Tồn kho đầu kỳ (triệu tấn)', (32, 380, 590, 130),
        x_axis=(2010, 2023, 'Năm'), y_axis=('Triệu tấn', 1, 0),
        Category=[col('CalendarYear', 'Year', 'Năm')], Y=[ms('SUA Opening Stocks Mil', 'Tồn đầu kỳ')])
    add(p, 'columnChart', '[Gạo xay xát] FAOSTAT SUA · Năm dương lịch: Biến động tồn & Sai số (triệu tấn)', (32, 515, 590, 130),
        x_axis=(2010, 2023, 'Năm'), y_axis=('', 1, -1.5, 1.0),
        Category=[col('CalendarYear', 'Year', 'Năm')], Y=[ms('SUA Stock Variation Mil', 'Biến động tồn'), ms('SUA Residuals Mil', 'Sai số thống kê (residual)')])
    add(p, 'lineChart', '[Gạo xay xát] USDA PSD · Niên vụ: Tồn kho cuối kỳ (triệu tấn, MY)', (638, 380, 610, 265),
        x_axis=(1960, 2025, 'Niên vụ'), y_axis=('Triệu tấn', 1, 0),
        Category=[col('PSD', 'market_year', 'Niên vụ')], Y=[ms('PSD Ending Stocks Mil', 'Tồn cuối kỳ')])

    # ==============================================================================
    # Page 5: Prices (S2: Không phụ thuộc slicer mặc định; S1: Trục VND/kg)
    # ==============================================================================
    p5_interactions = [
        {'source': 'v06', 'target': 'v14', 'type': 'NoFilter'}
    ]
    p = page(
        'Biến động giá lúa gạo',
        'Giá FOB USDA theo phẩm cấp; Benchmark giá World Bank; Giá lúa nội địa ĐBSCL theo giống.',
        f'Nguồn & Mốc mới nhất: World Bank (12/2025), USDA FOB (2025), Giá nội địa ĐBSCL (03/08/2026) · {batch_tag}',
        interactions=p5_interactions
    )
    # Top slicers (y=112, h=55)
    add(p, 'slicer', 'Nước xuất khẩu USDA', (32, 112, 180, 55), True, None, hide_slicer_header=True, Values=[col('USDAPrice', 'location_vi', 'Quốc gia')])
    add(p, 'slicer', 'Phẩm cấp gạo USDA', (220, 112, 210, 55), True, None, hide_slicer_header=True, Values=[col('USDAPrice', 'commodity_class_vi', 'Phẩm cấp gạo')])
    add(p, 'slicer', 'Kỳ tham chiếu USDA', (438, 112, 184, 55), False, None, hide_slicer_header=True, Values=[col('USDAPrice', 'reference_period', 'Kỳ tham chiếu')])
    add(p, 'slicer', 'Chuỗi giá World Bank', (638, 112, 610, 55), True, None, hide_slicer_header=True,
        Values=[col('WB', 'series_name_vi', 'Chuỗi giá WB')])

    # Fixed WB Cards (S2: never display "Chọn 1 chuỗi" at "All")
    card(p, 'Latest WB Thai 5 Display', '[Gạo xay xát] Giá Thái 5% mới nhất', 638, y=172, w=200, h=62)
    card(p, 'Latest WB VN 5 Display', '[Gạo xay xát] Giá Việt Nam 5% mới nhất', 843, y=172, w=200, h=62)
    card(p, 'WB Spread VN5 vs Thai5 Display', '[Gạo xay xát] Chênh lệch VN 5% − Thái 5%', 1048, y=172, w=200, h=62)

    # USDA Line chart (Left top) - S2.4: compares 4 countries on 5-PERCENT BROKEN by default
    add(p, 'lineChart', '', (32, 172, 590, 195),
        x_axis=(1985, 2025, 'Năm tham chiếu'), y_axis=('USD/tấn', 1), dynamic_title_measure='USDA FOB Chart Title',
        Category=[col('USDAPrice', 'reference_year', 'Năm tham chiếu')], Series=[col('USDAPrice', 'location_vi', 'Quốc gia')], Y=[ms('USDA FOB Chart Price', 'USD/tấn')])

    # Domestic Slicers (Left middle)
    add(p, 'slicer', 'Thị trường nội địa VN', (32, 375, 285, 55), False, None, hide_slicer_header=True, Values=[col('VNQuote', 'market', 'Thị trường')])
    add(p, 'slicer', 'Giống lúa & Loại giá', (325, 375, 297, 55), True, None, hide_slicer_header=True, Values=[col('VNQuote', 'series_label', 'Giống lúa & Loại giá')])

    def make_categorical_filter(name, entity, alias, column_name, values):
        """Locked visual-level categorical In filter."""
        return {
            'filters': [
                {
                    'name': name,
                    'field': {
                        'Column': {
                            'Expression': {
                                'SourceRef': {
                                    'Entity': entity
                                }
                            },
                            'Property': column_name
                        }
                    },
                    'type': 'Categorical',
                    'filter': {
                        'Version': 2,
                        'From': [
                            {
                                'Name': alias,
                                'Entity': entity,
                                'Type': 0
                            }
                        ],
                        'Where': [
                            {
                                'Condition': {
                                    'In': {
                                        'Expressions': [
                                            {
                                                'Column': {
                                                    'Expression': {
                                                        'SourceRef': {
                                                            'Source': alias
                                                        }
                                                    },
                                                    'Property': column_name
                                                }
                                            }
                                        ],
                                        'Values': [[{'Literal': {'Value': "'" + v.replace("'", "''") + "'"}}] for v in values]
                                    }
                                }
                            }
                        ]
                    },
                    'isLockedInViewMode': True
                }
            ]
        }

    # Domestic Quote Line chart (Left bottom) - U5: lock standard series_label so Đồng Tháp non-standard series & 7.3/0 outliers cannot leak
    add(p, 'lineChart', '', (32, 435, 590, 205),
        y_axis=('VND/kg', 1), dynamic_title_measure='Domestic Quote Chart Title',
        filter_config=make_categorical_filter('p05_v13_standard_quote', 'VNQuote', 'q', 'series_label', ['Lúa IR 50404 (lúa tươi) · Thương lái thu mua']),
        Category=[col('QuoteDate', 'Date', 'Ngày')], Series=[col('VNQuote', 'market', 'Thị trường')], Y=[ms('Domestic Quote Chart Price', 'VND/kg')])

    # WB Benchmark Line chart (Right middle) - ID v14 (4 series preserved via NoFilter)
    add(p, 'lineChart', '[Gạo xay xát] Benchmark giá gạo World Bank (USD/tấn)', (638, 240, 610, 190),
        y_axis=('USD/tấn', 1),
        Category=[col('BenchmarkMonth', 'Date', 'Tháng')], Series=[col('WB', 'series_name_vi', 'Chuỗi giá')], Y=[ms('WB Price', 'USD/tấn')],
        Tooltips=[ms('WB MoM', 'MoM'), ms('WB Peak', 'Đỉnh'), ms('WB Minimum', 'Đáy')])

    # WB MoM & Volatility charts (Right bottom side-by-side) - ID v15 & v16 (S2.2: 2 dedicated charts; Thai 5% & VN 5% only)
    wb_filter_values = ['rice_thai_5', 'rice_viet_namese_5']
    add(p, 'lineChart', '', (638, 436, 300, 204),
        y_axis=('Biến động (%)', 1), dynamic_title_measure='WB MoM Title',
        filter_config=make_categorical_filter('p05_v15_wb_5pct', 'WB', 'w', 'series_code', wb_filter_values),
        Category=[col('BenchmarkMonth', 'Date', 'Tháng')], Series=[col('WB', 'series_name_vi', 'Chuỗi giá')], Y=[ms('WB MoM Comparison', 'MoM')])
    add(p, 'lineChart', '', (948, 436, 300, 204),
        y_axis=('Độ biến động (%)', 1), dynamic_title_measure='WB Volatility Title',
        filter_config=make_categorical_filter('p05_v16_wb_5pct', 'WB', 'w', 'series_code', wb_filter_values),
        Category=[col('BenchmarkMonth', 'Date', 'Tháng')], Series=[col('WB', 'series_name_vi', 'Chuỗi giá')], Y=[ms('WB Volatility Comparison', 'Volatility 12M')])

    # ==============================================================================
    # Page 6: Trade Exports (S5: Khôi phục thẻ Đơn giá bình quân; sửa nhãn bị cắt; S1: Triệu tấn/Tỷ USD)
    # ==============================================================================
    p = page(
        'Thương mại lúa gạo quốc tế',
        'Xuất khẩu gạo của Việt Nam sang các đối tác thương mại trọng điểm.',
        f'Nguồn dữ liệu: Thương mại FAOSTAT · Mốc mới nhất: 2023 (2019–2023, 7 đối tác chính, không phải tổng xuất khẩu quốc gia) · {batch_tag}'
    )
    # Top 6 KPI cards across the canvas (width ~194 each)
    card(p, 'Trade Partners', '[Gạo xay xát] Số đối tác', 32, y=118, w=194, h=88)
    card(p, 'Trade Paired Groups', '[Gạo xay xát] Cặp đối tác–năm', 236, y=118, w=194, h=88)
    card(p, 'Latest Trade Quantity', '[Gạo xay xát] XK năm gần nhất', 440, y=118, w=194, h=88)
    card(p, 'Trade Quantity', '[Gạo xay xát] XK 7 đối tác (2019–2023)', 644, y=118, w=194, h=88)
    card(p, 'Trade Value', '[Gạo xay xát] Giá trị cộng dồn', 848, y=118, w=194, h=88)
    card(p, 'Trade Unit Value', '[Gạo xay xát] Đơn giá bình quân', 1052, y=118, w=196, h=88)

    # Slicers top right
    add(p, 'slicer', 'Năm', (860, 28, 180, 80), False, None, hide_slicer_header=True, Values=[col('CalendarYear', 'Year', 'Năm')])
    add(p, 'slicer', 'Đối tác', (1056, 28, 192, 80), False, None, hide_slicer_header=True, Values=[col('Trade', 'partner_name_vi', 'Đối tác')])

    # Charts (S1: Triệu tấn và Tỷ USD)
    add(p, 'lineChart', '[Gạo xay xát] Lượng xuất khẩu theo đối tác (triệu tấn)', (32, 218, 590, 200),
        x_axis=(2019, 2023, 'Năm'), y_axis=('Triệu tấn', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Series=[col('Trade', 'partner_name_vi', 'Đối tác')], Y=[ms('Trade Quantity Mil', 'Triệu tấn')])
    add(p, 'lineChart', '[Gạo xay xát] Đơn giá báo cáo theo đối tác (USD/tấn)', (638, 218, 610, 200),
        x_axis=(2019, 2023, 'Năm'), y_axis=('USD/tấn', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Series=[col('Trade', 'partner_name_vi', 'Đối tác')], Y=[ms('Trade Unit Value', 'USD/tấn')])
    add(p, 'lineChart', '[Gạo xay xát] Giá trị xuất khẩu theo năm và đối tác (tỷ USD)', (32, 428, 1216, 216),
        x_axis=(2019, 2023, 'Năm'), y_axis=('Tỷ USD', 1),
        Category=[col('CalendarYear', 'Year', 'Năm')], Series=[col('Trade', 'partner_name_vi', 'Đối tác')], Y=[ms('Trade Value Bil', 'Tỷ USD')])

    return specs


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

    theme = {'name': 'TLCN Teal', 'dataColors': ['#136F63', '#3274A1', '#D69832', '#8E649A', '#50A897', '#C65D5B'], 'background': '#F4F7F6', 'foreground': '#17342F', 'tableAccent': '#136F63'}
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
            'displayName': f'{i:02} · ' + p['title'],
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

        # Header title and navigation
        all_visuals = [visual('v01', 'textbox', '', 32, 10, 1216, 52), visual('v02', 'pageNavigator', '', 32, 64, 770, 42)]
        all_visuals[0]['visual']['objects'] = {'general': [{'properties': {'paragraphs': [{'textRuns': [{'value': p['title'], 'textStyle': {'fontFamily': 'Segoe UI', 'fontSize': '20pt', 'fontWeight': 'bold', 'color': '#136F63'}}]}]}}]}

        for j, item in enumerate(p['visuals'], 3):
            kind, title, pos, roles = item[0], item[1], item[2], item[3]
            single_select = item[4] if len(item) > 4 else False
            sort = item[5] if len(item) > 5 else None
            x_axis = item[6] if len(item) > 6 else None
            y_axis = item[7] if len(item) > 7 else None
            hide_slicer_header = item[8] if len(item) > 8 else False
            filter_config = item[9] if len(item) > 9 else None
            dynamic_title_measure = item[10] if len(item) > 10 else None
            all_visuals.append(visual(
                f'v{j:02}', kind, title, *pos, roles=roles, single_select=single_select, sort=sort,
                x_axis=x_axis, y_axis=y_axis, hide_slicer_header=hide_slicer_header, filter_config=filter_config,
                dynamic_title_measure=dynamic_title_measure
            ))

        for v in all_visuals:
            if v['visual']['visualType'] == 'slicer':
                if v['position']['height'] < 55:
                    v['position']['height'] = 55
                elif v['position']['y'] < 120 and v['position']['height'] > 70:
                    v['position']['y'] = 28
                    v['position']['height'] = 88

        # Clean business footer per page
        note_text = p['footer']
        note = visual('v90', 'textbox', '', 32, 654, 1216, 56)
        note['visual']['objects'] = {'general': [{'properties': {'paragraphs': [{'textRuns': [{'value': note_text, 'textStyle': {'fontFamily': 'Segoe UI', 'fontSize': '10pt', 'color': '#435B55'}}]}]}}]}
        all_visuals.append(note)

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
