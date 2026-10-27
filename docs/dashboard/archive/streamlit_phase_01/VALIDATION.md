# Dashboard Phase 01 · nghiệm thu 27/10/2026

**DASHBOARD_MVP_COMPLETE.** Hai trang chạy bằng published Gold thật; full unit regression, live AppTest, browser, responsive và final preservation PASS. Đây là acceptance Dashboard read-only, không suy từ EDA fixtures và không chạy lại Gold ETL. Evidence máy đọc: [acceptance.json](acceptance.json). Ảnh browser thật: [Overview](overview.png), [Việt Nam](vietnam.png).

Baseline Git: nhánh `feat(gold)`, HEAD `712d00ffdce5c5a4689b8506e08d0fa8f261a43d`, working tree sạch trước thay đổi. Owner work được giữ. Không commit/push/merge, không sửa canonical Gold DBML/mapping, Gold code/semantic SQL hoặc container config.

## Thay đổi

- `dashboard/`: hai pages, components Plotly và metrics/service adapters; no cache.
- `requirements/dashboard.txt`: Streamlit 1.50.0 và Plotly 6.6.0 đã có trên host, pandas tương thích.
- `src/analytics/access.py`: publication metadata response bổ sung model/status/DQ/source count lấy từ manifest đã verified. Không phát sinh publication timestamp.
- `src/analytics/queries.py` và `queries/analytics/nso_scope_catalogue.sql`: operation fixed SELECT mới lấy valid source scopes thực tế; optional typed geography key ở NSO observations để pushdown một địa lý. 13 templates EDA cũ và bốn Gold semantic definitions giữ nguyên; tổng Analytics hiện có 14 SQL templates + metadata operation.
- `tests/unit/dashboard/`: services, metric edge cases, AppTest hai trang. `scripts/validate_dashboard.py`: acceptance read-only, instrument kết quả thật, đối chiếu metadata và DagRuns.
- `.gitignore`: cho phép track `docs/dashboard/`.

## Stage A · static và fixture

Final full regression: **573 PASS trong 30,53 giây**, gồm 522 tests baseline và 51 Dashboard tests; 0 failed/errors/skipped. Những lượt trước 570/571 cũng PASS; 573 là lượt full regression mới. Sau chỉnh bố cục controls, focused Dashboard + Analytics: **83 PASS**. `compileall` và `git diff --check` PASS.

Fixtures kiểm tra: whitelist/typed filters; publication/schema/memory/writer/timeout/overflow failures; không fallback; COUNT reconciliation; percentage change, NULL/zero, Y−1 calendar, duplicated grain, geography/publication/unit separation, equal peaks, Decimal precision; UI render, thay bộ lọc ẩn kết quả cũ, Apply tải giá trị mới, no-data/error rõ ràng.

Streamlit AppTest chạy bên ngoài managed Windows sandbox vì temp ACL mode-700 không truy cập được trong sandbox. Không thay system ACL. Tests không tạo catalog object.

Browser kiểm tra thêm fresh deep link `/vietnam`: cơ chế auto-discovery của thư mục `pages/` có thể bỏ qua bootstrap khi đây là URL đầu tiên. Đã chuyển page modules sang `dashboard/screens/`, chỉ route bằng `st.navigation`; thêm regression ngăn khôi phục auto-discovery. Tiến trình Streamlit cũ đã reload riêng, không restart Docker. Một fixture đầu tiên cũng dùng period year cho fact monthly/daily; đã sửa fixture sang YYYYMM / YYYYMMDD, không sửa production logic để ép PASS.

## Stage B · live read-only

**Kết quả cuối: PASS.** Batch `ec421297c194804dc7a3a63aacfda37a65dacfac07245d118e34b34506ca2235`, model `gold-20261025.1`. Live AppTest chạy 48 requests trong 135,08 giây; năm kịch bản PASS: Overview, NSO mặc định, range 2015–2024, area và V06.14 reported yield. Không mock data hoặc admission. Browser Chromium độc lập PASS fresh `/vietnam`, navigation sidebar sang Overview, 4 + 3 biểu đồ và KPI bên dưới. Click đợi Streamlit idle, không `force=True`.

| KPI live | Giá trị |
|---|---:|
| Tổng fact observations | 1.414.527 |
| Numeric observations | 1.402.167 |
| Present-NULL observations | 12.360 |
| Numeric coverage | 99,13% |
| USDA reference present-NULL | 4.523 / 12.936 = 34,96% |

| Fact | Rows | Có số | Present-NULL |
|---|---:|---:|---:|
| fact_nso_observation | 23.060 | 22.114 | 946 |
| fact_fao_production_observation | 22.738 | 22.509 | 229 |
| fact_fao_sua_observation | 36.380 | 36.123 | 257 |
| fact_fao_trade_observation | 1.226.470 | 1.226.470 | 0 |
| fact_usda_psd_observation | 990 | 990 | 0 |
| fact_price_monthly | 61.573 | 55.168 | 6.405 |
| fact_fao_price_annual | 9.993 | 9.993 | 0 |
| fact_usda_price_reference | 12.936 | 8.413 | 4.523 |
| fact_vn_market_quote_daily | 20.387 | 20.387 | 0 |

NSO V06.12.csv, national / annual / quantity:

- Production: **19.225.100 tấn (1990) → 43.451.600 tấn (2024)**; +24.226.500 tấn, **+126,01%**. Peak trong scope: **45.091.000 tấn (2015)**; peak→2024 **−3,64%**. YoY 2024 **−0,11%**, dùng đúng 2023.
- Area: **6.042.800 ha → 7.127.100 ha**, +1.084.300 ha / **+17,94%**; 2015→2024 **−8,95%**.
- Reported yield giữ riêng **V06.14.csv**: 3.690 kg/ha (1995) → 6.100 kg/ha (2024), **+65,31%**; không suy yield từ publication khác.

Tất cả số liệu trên lấy từ queries/current published batch và được tính lại trong UI, không phải constants trong app. Năm thiếu/NULL/zero và no-data UI có fixture tests; nguồn scope mặc định hiện liên tục 1990–2024 nên không tự tạo gap để gọi là physical gap test. Actual low-RAM error state đã kiểm tra bằng browser.

### Hiệu năng và responsive

- Full Overview: **51,79 giây AppTest / 55,95 giây browser**, 18 SELECT tuần tự ngoài metadata. NSO default: **19,03 / 28,16 giây**, gồm catalogue + load. Đây là measured runs, không SLA.
- Peak Streamlit RSS trong browser run: **144,18 MiB**. AppTest host peak: **140,17 MiB**. Windows available minimum tương ứng **2,44 / 2,92 GiB**; WSL gate ≥3 GiB PASS từng request.
- Largest serialized response trong final live run: **216.260 bytes**; max SQL **0,463 giây** ở lượt warm cuối. Lượt cold trước đã đo **3,774 giây**, vẫn dưới budget 15 giây. Query-memory cap vẫn 64 MiB; đây không phải số đo peak heap. Statement API không cung cấp peak/spill ở các responses này.
- Browser 1024 / 768 / 480 px: **PASS**, kiểm tra metric values không overflow/truncate. CSS responsive cuối cùng được kiểm tra riêng sau live AppTest; chỉ CSS thay đổi, hashes before/after và final source hashes được ghi rõ trong evidence. Không đổi query/business code sau live acceptance.

### Historical admission / diagnostic attempts

Đầu phiên: bảy services running, sáu Docker HEALTHCHECKs healthy; REST không khai báo HEALTHCHECK. Windows available **0,92 GiB**, dưới guard ≥2 GiB, nên dừng trước data SQL. Metadata baseline lấy bằng existing foundation helper: 36 tables, manifest fingerprint `631c0d372fcb37dfd81466622012a42311b867c028f570b4465658a9136a046b`. Đây chỉ là baseline, không phải kết quả live-data PASS ở thời điểm đó.

Script `--live` dùng real DashboardService + EDAClient trong AppTest, không mock query/admission. Đối chiếu Overview COUNTs và source Decimal endpoints độc lập với chuỗi UI; thử default national annual, đổi range 2015–2024, area, V06.14 reported yield. Mỗi lần chạy kiểm tra lại metadata và giữ fail status nếu không đủ scope.

Server host khởi động tại `127.0.0.1:8501`, HTTP health `ok`. Browser ban đầu render `RESOURCE_BLOCKED`, không stack trace; sample tiếp theo 1,21 GiB vẫn thấp, không bypass.

Mẫu sau đó Windows ≥3,14 GiB cho phép bắt đầu live acceptance. Lần đầu health adapter yêu cầu field Health của REST không tồn tại nên dừng trước data SQL; đã sửa bằng bounded HTTP probe thực, không coi Running là health và không sửa Compose. Historical attempts được giữ trong `acceptance.json`.

Một lượt AppTest sau đó dừng ở chuyển publication V06.14; bản script cũ chỉ ghi AssertionError nên không xác định được nguyên nhân chi tiết từ evidence. Reproduction catalogue nhỏ PASS; đã bổ sung stage/error-code/partial-progress diagnostics thay vì đoán nguyên nhân. Lượt full cuối cùng PASS cùng scope. Browser automation trước có selector exact-name không tính icon `play_arrow` và click trước Streamlit idle; đã sửa test locator/wait, không force click hoặc bỏ gate.

## Stage C · preservation

**PASS sau toàn bộ live/browser/responsive queries.** Đủ **36/36** tables giữ UUID, snapshot, metadata location và exact data-file paths. Gold context/pointer/manifest/four semantic hashes, mọi Bronze/Silver/Gold DagRun ID/state và protected source/contract hashes giữ nguyên. Không thêm object, không Iceberg write, không trigger pipeline. Hai Gold SUCCESS và hai failed historical runs giữ nguyên state.

Evidence chi tiết nằm ở `final_preservation` trong [acceptance.json](acceptance.json); per-table UUID/snapshot/metadata location và file-set hashes kèm theo. Full baseline tạm ở `.test-tmp/dashboard-baseline.json`; initial baseline cũng khớp EDA physical evidence. Không sửa Gold/EDA historical acceptance.

## Giới hạn

- Source-scoped NSO identities chưa official administrative crosswalk.
- Không có publication timestamp trong manifest, chỉ có thời điểm metadata readback.
- Không refresh tự động, kết quả hiển thị thuộc lần tải có timestamp; mọi tải mới đều gate lại.
- Không cache, toàn bộ chín-fact Overview cần nhiều round trips; chưa công bố latency SLA.
- Chỉ read-only acceptance; không chạy lại DQ01–18/Gold DAG hoặc formal NOOP writer test vốn đã accepted trước đó.
