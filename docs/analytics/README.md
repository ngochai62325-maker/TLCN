# Jupyter EDA — Analytics Phase 01

> Reporting hiện hành (27/10/2026): **Power BI Desktop Free + published-Gold CSV Import**; Streamlit đã ngừng. [Exporter/schema](../powerbi/README.md), [hướng dẫn Desktop](../powerbi/POWER_BI_DESKTOP_GUIDE.md). Các EDA notebooks/findings/acceptance dưới đây được giữ nguyên làm evidence; không phải report PBIX đã được tạo.

Đọc [EDA_FINDINGS](EDA_FINDINGS.md) để xem measured insights và Dashboard plan;
[EDA_ACCEPTANCE](EDA_ACCEPTANCE.json) ghi execution/performance. Sáu notebooks tại
[notebooks/analytics](../../notebooks/analytics/) độc lập; các notebooks cũ trong
`tlcn_explain-for_eda/` không bị sửa. Không Dashboard, Spark job hay Iceberg write.

## Reproduce trên local profile hiện hành

Từ repo root, cùng Python environment với Jupyter kernel `python3`:

```powershell
python -m pip install -r requirements/analytics.txt
python scripts/run_eda_notebooks.py
python scripts/check_eda_preservation.py --phase EDA-05
python scripts/report_eda_findings.py
```

Hoặc chạy độc lập:

```powershell
python scripts/run_eda_notebooks.py --notebook 01_vietnam_rice_production.ipynb
python -m pytest tests/unit/analytics -q
```

Mở `.ipynb` trong IDE/Jupyter để đọc từng code/Markdown cell và rendered plots.
Notebook runner thực thi từng cell top-to-bottom, giữ lỗi nếu fail và chỉ render
verified Markdown từ evidence cuối thành công. Không `allow_errors` hoặc fallback
sang dữ liệu cũ. Run All trực tiếp cũng thực hiện queries mới; các Markdown measured
findings áp dụng đúng batch đã ghi. Dùng runner để refresh findings sau chỉnh sửa.

Các files chỉ được generator overwrite nếu có ownership tag `TLCN-EDA-01` và dùng
`--refresh-generated`. Không cần generate lại khi reproduce; thao tác đó xóa outputs
trong notebook được chọn. Evidence mới thuộc lần reproduce; không ghi đè Gold audits.

## Read protocol

`EDAClient` → JSON stdin → `jobs/analytics/query_eda.py` trong runner hiện có.
Không credentials trên host; không arbitrary SQL/table names trong parameters.
14 allowlisted operations = 13 fixed SQL templates + publication metadata operation.
Giữ shared source locks, serial Analytics lock và existing `gold.consumer.read_window`.
Mỗi request kiểm tra publication/semantic hashes/model/schema/UUID/snapshot và Gold
code fingerprint; mọi relation được bind đúng `FOR VERSION AS OF` published snapshot.
Không sửa bốn Gold SQL definitions. Production/SUA/Trade reuse approved SELECT logic,
với source/year/product/reporter filters trước pivot. Source flags và contributor
observation keys được giữ khi có trong subject output.

No result cache: mỗi request đi qua gate. Notebook outputs là historical evidence,
không được dùng làm live-cache fallback. Batch đổi giữa notebook bị từ chối; notebook
expected batch hiện cố định `ec421297…`. Muốn phân tích publication mới phải review
filters/series và cập nhật expected batch rõ ràng, không tự trộn batches.

## Resource/query limits

Windows available >=2 GiB; existing WSL gate >=3 GiB; không active pipeline DagRun,
source writer hoặc Spark trong runner. Thiếu tài nguyên sẽ fail closed, không tự
start/restart Docker hay tăng heap. Một query tại một thời điểm.

Trino cap 64 MB/query; run/execution timeout 15 s, planning 5 s; tối đa 5.000 output
rows và 5 MiB serialized response. `LIMIT 5001` chỉ phát hiện output overflow:
thấy row 5001 thì reject toàn bộ, không trả partial KPI. Không LIMIT/sample input
để tính totals. SQL grouping/pivot chạy tại Trino; Pandas chỉ giữ bounded results.
Source-period/date parameters được validate; Trade tối đa 10 calendar years/request,
Notebook 05 chỉ dùng 5 năm đã có coverage. Không collect full Trade fact.

Query IDs, definition/compiled-SQL hashes, typed filters, batch/snapshots và bytes/
timing nằm trong [evidence](evidence/). Benchmark là scoped queries thực tế, không
SLA cho unlimited datasets. Trino peak memory/spill không được statement response
trả thì giữ NULL. Notebook kernel RSS và Windows samples được đo riêng.

## Tests và semantics

Unit tests cover allowlists, parameters, resource rejection, snapshot/Gold fingerprint
mismatch, unpublished reads, cache absence, budget/cancel/failure, calendar-aware YoY,
NULL vs absent/zero và rolling windows. `tests/integration/analytics_select_fixtures.py`
chạy 4 small Trino VALUES cases (no catalog objects): paired yield + flags/present NULL,
signed stock variation, true-zero unit value, missing quantity. Các tests này không
thay actual notebook execution hoặc Gold E2E acceptance.

Không SUM overlapping geography, annual + seasonal, MY + TY, stock values qua năm,
rice products khác nhau hoặc currency khác nhau. Không official geography conversion,
FX/paddy-milled conversion, vintage/PIT claim, global mirrored trade totals hay
unverified partner shares. Empty scope phải được điều tra, không được tạo blank
chart rồi xem là insight. Xem hai scope discoveries SUA/Trade trong Findings.
