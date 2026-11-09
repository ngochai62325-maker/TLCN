# Insight matrix — report và EDA

Ngày 06/11/2026. Reconciliation mới từ CSV thật, cùng batch `ec421297c194…`; không sửa [EDA_FINDINGS.md](../analytics/EDA_FINDINGS.md) ngày 27/10. [Machine results](report_evidence/reconciliation.json) chứa values/precision/method; [DAX evidence](report_acceptance.json) là một gate riêng.

Các giá trị dưới đây là **reference tại scope mặc định**. Slicers làm DAX/visuals tính lại trong phạm vi chọn; không giữ một historical finding dưới một filter khác. Nguồn NSO có release status provisional/unconfirmed theo evidence gốc; Gold PUBLISHED không có nghĩa số nguồn đã FINAL. Mọi diễn giải dưới đây là descriptive, không phải causal explanation.

| Finding | Kết quả đối chiếu | Page / visual / measure | Giới hạn |
|---|---|---|---|
| OV01 | 1.414.527 total; 1.402.167 numeric; 12.360 NULL. USDA reference NULL 4.523/12.936 ≈34,96% | 01 cards, fact bars/table; Total/Numeric Observations, Present NULL, Numeric Coverage | Count-based coverage; không gọi NULL rate là điểm chất lượng nguồn |
| OV02 | Full-data first/last/distinct represented periods của chín facts khớp output | 01 inventory table | Whole-fact range không chứng minh từng series liên tục |
| OV03 | 34 metrics, 1.042 series, sáu geography namespaces từ EDA cùng batch | Metadata reference; không tạo live inventory KPI giả trên report | Reuse accepted inventory vì same batch/physical identities; không query lại finding này |
| VN01 | Production 19.225.100t(1990) →43.451.600t(2024), +126,01% | 02 production line/cards; Annual Production, Production Change Pct | Chỉ V06.12 national annual quantity |
| VN02 | Area 6.042.800ha →7.127.100ha, +17,94% | 02 Annual Area line | Cùng V06.12, không cộng geography |
| VN03 | Reported yield 3.690→6.100kg/ha, +65,31%, 1995–2024 | 02 Reported Yield line | V06.14 riêng; trước1995 không bịa số |
| VN04 | Đông xuân2024 20.333.900t, lớn nhất trong ba source seasons | 02 Seasonal Production line/legend | Không cộng annual với seasons, không buộc seasonal sum=annual |
| VN05 | Peak2015 45.091.000t; production2015→24 −3,64%, area −8,95% | 02 Peak Year/Production, dynamic NSO Insight, area trend | Peak và endpoint thay đổi theo filter; không suy nguyên nhân |
| FAO01 | Việt Nam1961→24: production+382,92%, area+48,89%, computed yield+224,35% | 03 production/yield lines và single-year scatter | Source M49/CPC0113; không trộn NSO/USDA |
| FAO02 | Computed yield VN 1.896,59→6.151,66kg/ha | 03 FAO Weighted Yield | Paired production/area; không AVG row yields |
| FAO03 | India đứng đầu **nhóm bốn areas** năm chung2024: 217.867.864,16t | 03 common-year bar, rank/year tooltip | EDA hiển thị nguyên217.867.864; CSV không bị làm tròn để ép khớp. Không world rank |
| SUA01 | Food supply2010→23: 14.050.000→13.837.660,13t, −1,51% | 04 SUA Food Supply line | CPC23161.02, chỉ observed element5141 |
| SUA02 | Opening stocks2010→23: 5.917.024,29→6.113.011,31t, +3,31% | 04 SUA Opening Stocks, signed Stock Variation line | Stock levels không cộng qua thời gian; không suy closing balance |
| PSD01 | MY2010→25 production26.371.000→26.200.000t, −0,65% | 04 PSD Production/Consumption line | Vietnam/Rice, Milled, market year; không calendar-year join |
| PSD02 | Ending stocks1.941.000→2.297.000t, +18,34% cùng MY scope | 04 PSD Ending Stocks line | Không SUM stock qua năm |
| PR01 | WB max907USD/t(2008-04), min163,75USD/t(2001-04) | 05 WB Price + Peak/Minimum tooltips | rice_thai_5 only; extrema trong selected scope |
| PR02 | Max rolling volatility≈17,47% tại2009-01 | 05 WB Volatility line | 12 log returns, sample std, 13 positive consecutive prices; không annualize |
| PR03 | Exact quote An Giang IR50404:484 observations, range4.500–9.400VND/kg | 05 Domestic Quote + coverage table | Observed quote dates; không nội suy ngày thiếu, không USD conversion |
| TR01 | China2019→23:347.012,45→753.672,54t, +117,19% | 06 partner-filtered Trade Quantity line | VN export, partner156, CPC23161.02 |
| TR02 | Malaysia546.516,60→400.504,18t, −26,72% | 06 same line, partner458 | Không cộng mirrored import flows |
| TR03 | Philippines2.129.613,10→3.015.899,86t, +41,62% | 06 same line, partner608 | Reported source flow, không global share |
| TR04 | Philippines unit value414,96→560,88USD/t, +35,16% | 06 Trade Unit Value line | Khi chọn nhiều năm/partners: ratio-of-paired-sums, không AVG đơn giá |

Toàn bộ 22 IDs được đối chiếu. Các findings phần trăm không phải số constants gắn vào chart; chart/insight hiện tại đọc các imported mart measures. Insight labels giải thích scope; các visual không khẳng định thời điểm công bố/vintage hoặc nguyên nhân kinh tế chưa có dữ liệu.

Ví dụ filter acceptance trên Desktop: Philippines-only ở cả năm năm cho **5 paired groups**, quantity12.863.751,28t, value6.295.254.000USD và paired unit value≈489,38USD/t. All three partners cho15groups,17.966.768,23t,8.824.279.000USD,≈491,14USD/t. Đây là tổng flow trong explicit disjoint scope, không phải một annual KPI hoặc toàn bộ xuất khẩu Việt Nam.
