# TLCN Dashboard · Local Analytics MVP

Hai trang: **Tổng quan dữ liệu** và **Sản xuất lúa Việt Nam**. Streamlit chạy trên Windows host, biểu đồ Plotly tương tác; không thêm service Docker. Giao diện tiếng Việt, chỉ đọc Gold đã published. Kết quả EDA lưu trước đây không được dùng làm dữ liệu hiện hành.

**DASHBOARD_MVP_COMPLETE · 27/10/2026.** 573 unit tests, live AppTest, browser với dữ liệu Gold thật và preservation 36/36 tables PASS. [Kết quả nghiệm thu](VALIDATION.md), [machine-readable evidence](acceptance.json), [ảnh Overview](overview.png), [ảnh Việt Nam](vietnam.png).

## Chạy trên PowerShell

Từ root repository, Python 3.10–3.13; môi trường đã nghiệm thu dùng Python 3.13:

```powershell
python -m pip install -r requirements/dashboard.txt
python -m streamlit run dashboard/app.py --server.address 127.0.0.1 --server.port 8501 --server.headless true --browser.gatherUsageStats false --theme.base light --theme.primaryColor '#136F63'
```

Mở [Dashboard local](http://localhost:8501). Cần Docker Desktop và bảy services của project `tlcn-local`: PostgreSQL, MinIO, Iceberg REST, Trino, runner, Airflow webserver và scheduler. Dashboard không start/restart container và không trigger DAG. Nếu stack đang tắt, chủ dự án dùng quy trình có sẵn:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/start.ps1
```

Startup stack có gate riêng: Windows ≥2 GiB, WSL ≥4 GiB. Khi query Dashboard: Windows ≥2 GiB và WSL guard hiện hành ≥3 GiB. Không tăng heap Trino 512 MiB để vượt gate. Dừng Dashboard bằng `Ctrl+C`; dữ liệu Iceberg vẫn trong volumes của Docker. Không dùng `down -v`.

Dashboard kiểm tra sáu Docker HEALTHCHECKs. Iceberg REST hiện không khai báo Docker HEALTHCHECK trong Compose, nên adapter yêu cầu container chạy **và HTTP `/v1/config` trả 200 từ runner**, sau đó catalogue readback vẫn phải PASS.

Sau khi sửa source Dashboard, dừng riêng Streamlit bằng `Ctrl+C` rồi chạy lại lệnh để nạp imports mới. Không cần restart Docker. Hai screen modules nằm trong `dashboard/screens/` và được route bởi entrypoint chung, nên mở trực tiếp `/vietnam` cũng đi qua cùng bootstrap và publication checks.

## Cách dùng

1. Vào **Tổng quan dữ liệu**, xem trạng thái publication rồi bấm **Tải tổng quan**. Các KPI đến từ full-data aggregates của chín facts; mỗi fact có thêm đối chiếu represented periods.
2. Vào **Sản xuất lúa Việt Nam**. Mặc định NSO `V06.12.csv`, national, production, quantity, annual, 1990–2024 nếu phạm vi này còn tồn tại trong batch hiện tại.
3. Chọn publication, cấp địa lý, địa lý nguồn, loại thống kê, chỉ tiêu, vụ và khoảng năm; bấm **Tải phạm vi**. Lựa chọn phụ thuộc catalogue thực sự có trong fact. Khi đổi bộ lọc, kết quả cũ biến mất; query observations chỉ chạy khi bấm tải.
4. Chọn `V06.14.csv` / yield khi muốn xem **năng suất báo cáo**, tách khỏi publication sản lượng/diện tích. Không suy năng suất bằng ghép các publication.
5. Zoom/hover, bật/tắt legend và tải PNG bằng toolbar Plotly. Diagnostics mở rộng chứa đầy đủ batch/manifest hash, query IDs và timings.
6. **Làm mới kết nối** chấp nhận kiểm tra batch hiện tại từ đầu. Khi batch đổi giữa phiên, request bị từ chối cho tới khi chủ động làm mới; không trộn batch.

## Đường truy cập và bảo vệ dữ liệu

```text
Streamlit host → DashboardService → EDAClient → docker exec (argument list)
→ jobs/analytics/query_eda.py → Analytics admission + Gold read_window
→ Trino 448 SELECT → iceberg.gold_local FOR VERSION AS OF published snapshots
```

Mỗi request giữ nguyên Gold manifest/semantic-definition verification, code fingerprint, 17 UUID/snapshots/schema readback, source locks và active-writer/DagRun checks. Dashboard chỉ cho phép bảy Analytics operations cần thiết, không nhận SQL hoặc identifier tùy ý. Credentials chỉ ở runner, không gửi tới browser. Process-local lock cộng PostgreSQL Analytics lock chặn query chồng lấn; thao tác luôn tuần tự.

Không cache result; không lưu DataFrame trong Streamlit session state. Chỉ giữ batch identity và widget state. Catalogue metadata được đọc lại qua gate khi rerun. Không tự động refresh theo timer. Kết quả đang hiển thị là ảnh chụp tại lần tải có timestamp kiểm tra, không phải subscription; mọi lần tải mới đều kiểm chứng lại.

Giới hạn mỗi request không tăng: **5.000 rows / 5 MiB / 15 giây SQL / 64 MiB query memory**, subprocess timeout 40 giây. `LIMIT 5001` trong query layer chỉ phát hiện overflow, không sampling KPI.

## Biểu đồ và định nghĩa metric

| Trang | Metric / biểu đồ | Grain, đơn vị, NULL và phép tính |
|---|---|---|
| Tổng quan | Tổng, có số, present-NULL, độ phủ số | `COUNT(*)`, `COUNT(value)`, `COUNT_IF(value IS NULL)`, có số/tổng ×100 trên chín facts; tổng chỉ công bố khi đủ chín aggregates hợp lệ |
| Tổng quan | Số quan sát theo fact | Số dòng, thang log; không phải sum physical measure values |
| Tổng quan | Coverage bar | Tỷ lệ các dòng có số và present-NULL; genuine zero là có số, absent row không nằm trong mẫu số |
| Tổng quan | Timeline và bảng represented periods | Min/max toàn dataset và distinct kỳ thực sự có mặt; không chứng minh liên tục của từng series. Marketing/reference year giữ semantics nguồn |
| NSO | Endpoints, chênh lệch, thay đổi | Một publication/địa lý/measure/kind/season; đầu/cuối **có số**, cuối−đầu; (cuối/đầu−1)×100 nếu mẫu số >0 |
| NSO | Đỉnh và YoY | Đỉnh trong phạm vi lọc, mọi năm đồng đỉnh; YoY chỉ dùng đúng Y−1 có số và mẫu số >0, không dùng previous available row |
| NSO | Production / area trends | Tấn / ha, hai biểu đồ riêng, không SUM quốc gia/vùng/tỉnh; khoảng năm vắng hoặc present-NULL làm đứt đường |
| NSO | Normalized comparison | Năm đầu chung có số dương =100; mỗi chuỗi chia giá trị của chính nó tại năm gốc; không kết luận causality |
| NSO | Seasonal production | Các vụ trong cùng publication/geography/kind/range; không cộng annual vào các vụ |
| NSO | Reported yield / development index | Yield nguồn kg/ha; development index nguồn % (năm trước=100) giữ riêng. Không tính YoY lượng từ biến động index |

Insights tính lại từ query hiện tại và gắn cùng scope. Không có constants 19.225.100 hay 45.091.000 trong UI. NULL không thay zero. Năm cuối có số có thể sớm hơn năm cuối có observation; giao diện nêu rõ. Không có official province crosswalk hoặc suy nguyên nhân từ tương quan.

Chỉ có một năm có số: vẫn hiện giá trị và đỉnh, nhưng chênh lệch theo thời gian / tỷ lệ thay đổi là **Không áp dụng**. Hai năm khác nhau có cùng giá trị cho thay đổi bằng 0 hợp lệ.

## Kiểm tra và xử lý lỗi

```powershell
python -m pytest tests/unit/dashboard tests/unit/analytics -q
```

Developer acceptance read-only (baseline được giữ, không tự ghi đè):

```powershell
python scripts/validate_dashboard.py --capture
python scripts/validate_dashboard.py --live
```

AppTest là cơ chế kiểm thử chính thức của Streamlit, thao tác các widget trên script thực; fixtures và live acceptance được tách biệt. Tham khảo [Streamlit 1.50 AppTest](https://docs.streamlit.io/1.50.0/develop/api-reference/app-testing/st.testing.v1.apptest), [forms](https://docs.streamlit.io/1.50.0/develop/api-reference/execution-flow/st.form) và [navigation](https://docs.streamlit.io/1.50.0/develop/api-reference/navigation/st.navigation).

| Thông báo | Hành động |
|---|---|
| Chưa đủ RAM | Đóng bớt ứng dụng, chờ memory guard PASS rồi tải lại; không tự restart Docker |
| Query không chạy được | Kiểm tra bảy containers healthy, runner và Trino host port 18080; Dashboard dùng runner nội bộ |
| Publication chưa hợp lệ/đổi batch | Không dùng dữ liệu cũ; kiểm tra Gold published state, rồi Làm mới kết nối |
| Writer/query khác đang chạy | Chờ kết thúc; không trigger pipeline từ Dashboard |
| Không có dữ liệu trong phạm vi lọc | Query thành công nhưng không có observation tương ứng; đổi bộ lọc, không điền zero |
| Present-NULL / Không áp dụng | Quan sát không có số, thiếu Y−1 hoặc mẫu số không dương; giữ nguyên semantics |

Dashboard chỉ bind loopback, phục vụ demo local một chủ sở hữu; không phải ứng dụng multi-user công khai có authentication. Tổng quan hiện chạy 18 SELECT tuần tự + metadata nên chậm hơn trang NSO; chưa cache. Không dùng full-history Trade semantic validation làm endpoint tương tác. Các trang Global/Trade/Prices/SUA và deployment sang laptop thứ hai ngoài scope.

Measured load trên laptop hiện tại: Overview khoảng 52–56 giây, NSO default khoảng 19–28 giây; Streamlit peak RSS khoảng 144 MiB. Không quảng bá real-time latency. Các cards/charts chuyển bố cục ở màn hình nhỏ; 1024/768/480 px đã kiểm tra không cắt số KPI.

Kết quả kiểm thử và giới hạn thực tế: [VALIDATION.md](VALIDATION.md).
