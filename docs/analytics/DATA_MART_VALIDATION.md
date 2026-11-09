# Nghiệm thu Analytics Data Marts — 06/11/2026

Baseline Git: `feat(gold)@cd2f1a77b27d5bcba6f1d925e3d9305952551bae`.
Thiết kế: [DATA_MART_DESIGN.md](DATA_MART_DESIGN.md). Evidence mới: [report_acceptance.json](../powerbi/report_acceptance.json). Evidence EDA/Gold cũ được giữ nguyên.

## Export thật

Bundle được report sử dụng:

```text
exports/powerbi/ec421297c194_20261106T031815Z_01d03710/
```

Batch live đã xác minh: `ec421297c194804dc7a3a63aacfda37a65dacfac07245d118e34b34506ca2235`.
Model `gold-20261025.1`; publication manifest SHA-256 `631c0d372fcb37dfd81466622012a42311b867c028f570b4465658a9136a046b`.
V2 không thay thế hay sửa các bundle v1 ngày 27/10. Bundle thử thành công trước đó ngày 06/11 cũng được giữ; cả mười CSV có checksum giống bundle cuối.

| CSV | Rows thật | Bytes | Phạm vi |
|---|---:|---:|---|
| fact_overview.csv | 9 | 870 | Chín full-data fact aggregates |
| nso_national_annual.csv | 35 | 2.539 | V06.12 national annual quantity, 1990–2024 |
| nso_national_seasonal.csv | 105 | 7.562 | Cùng publication/geography, ba vụ riêng |
| nso_reported_yield.csv | 30 | 1.096 | V06.14 national reported yield, 1995–2024 |
| fao_production.csv | 256 | 66.168 | Bốn M49 source areas, CPC0113, 1961–2024 |
| sua_components.csv | 112 | 23.718 | Các cell thực sự hiện diện, 2010–2023; không bịa đủ 12 cells/năm |
| usda_psd.csv | 990 | 218.926 | Vietnam / Rice, Milled, 15 attributes, MY1960–2025 |
| wb_benchmark.csv | 312 | 56.671 | rice_thai_5, monthly 2000–2025 |
| vn_quote.csv | 484 | 92.639 | Exact An Giang IR50404 quote series; 2021–2026 |
| trade_exports.csv | 15 | 5.168 | Reporter704, partners156/458/608, export2019–2023 |
| **Tổng** | **2.348** | **475.357** | |

Mỗi file có UTF-8 BOM, header/type/order ổn định, số không format, ngày ISO. Manifest giữ 17 UUID/snapshot bindings, 28 typed query records và definition/compiled hashes, contract/version/hash, scopes, contributor observation keys, exact reconciliation, file hashes/counts và performance ledger. Marts nằm ngoài Iceberg Gold.

## Kết quả correctness

- Source identities được tìm qua catalogue allowlisted; không join theo display name.
- Query lần lượt, không sampling/LIMIT để tính KPI. Full-data overview khớp 1.414.527 total = 1.402.167 numeric + 12.360 present-NULL.
- Annual production/area V06.12 pivot đúng source cells; reported yield V06.14 riêng. Không cộng geography hoặc annual với seasonal.
- Paired FAO/Trade bases có cùng eligible population; tỷ số được recompute bằng Decimal. NULL, absent, genuine zero và signed SUA không bị đổi nghĩa.
- WB scaffold, calendar MoM và rolling sample std 12 log returns được kiểm độc lập; không nội suy. Giá absent khác present-NULL.
- CSV serialization được đọc lại và so exact typed rows. Verifier kiểm schema/hash/count/grain/unit/presence/ratios và đủ SQL provenance inventory.
- Python độc lập đối chiếu 22 EDA finding IDs: PASS trong cùng published batch. OV03 reuse inventory EDA vì batch và physical identities không đổi; không tuyên bố đã query lại inventory. India2024 giữ **217.867.864,16 tonnes**; chỉ round khi so số nguyên hiển thị trong EDA.
- DAX và visual acceptance được tách riêng tại [REPORT_VALIDATION.md](../powerbi/REPORT_VALIDATION.md); tests synthetic không được gọi là physical acceptance.

## Resource và thời gian

Lượt export cuối: **89,99 s trước atomic promotion**, 28 Analytics operations, gồm **26 Trino SELECTs** và hai publication metadata operations. Tổng transport SQL time **11,26 s**; tổng client request time **86,93 s** gồm admission/readback. Query peak memory lớn nhất Trino báo **6.383.996 bytes (~6,09 MiB)**; spill báo 0. Không gọi đây là JVM peak RSS. Windows available thấp nhất tại request admission **2.794.188.800 bytes (~2,60 GiB)**. WSL guard ≥3GiB đạt cho từng request; không lưu chuỗi WSL samples của cả export. Không nâng heap hay đổi Compose.

Ngân sách giữ nguyên: 5.000 rows, 5MiB, 15s và 64MiB/query, một request tại một thời điểm. Trade chỉ đọc reporter/product từng năm trong năm năm; không collect full Trade. Có lượt trước bị resource gate từ chối khi RAM giảm dưới 2GiB; staging được cleanup, không có manifest COMPLETE giả. Không có query rejection trong lượt thành công cuối.

## Preservation và tests

Read-only before/after proof bằng nhau cho **36/36 tables**, bao gồm UUID, current snapshot, metadata location và sorted data-file paths; pointer/active/manifest và sáu persisted DagRuns không đổi. Protected Gold code/SQL/DBML/mapping và sáu notebooks/EDA files giữ nguyên SHA-256. Không tạo objects, commit Iceberg hoặc trigger DAG.

**589 unit tests PASS** (557 baseline +32 mới), không fail/skip trong lần full suite cuối. Tests mới gồm nguồn/đơn vị/grain sai, duplicate/multi-contributor, NULL/0, gap, negative/zero denominator, signed stock variation, paired-population ratio, timeout, staging cleanup, contract/snapshot/SQL hash drift. Client/resource/publication regressions cũ vẫn PASS. Xem commands và bằng chứng tại [REPORT_VALIDATION.md](../powerbi/REPORT_VALIDATION.md).

## Giới hạn

CSV là extract bất biến tại thời điểm export, không phải automatic live refresh. Offline checksum verification không xác thực publication hiện tại; chỉ lượt export thật mới vào live read gate. Không rerun toàn bộ EDA notebooks hay Gold ETL. Không tuyên bố universal geography/product conformance, world market share, full SUA balance, FX conversion hoặc causality.
