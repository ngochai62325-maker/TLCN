# Bronze → Silver coverage

Ngày kiểm chứng: 10/10/2026. Nguồn authoritative: REST `/v1/namespaces`/`tables`, Trino SELECT, 10 raw manifests, PostgreSQL ingestion runs/source snapshots. Không suy ra coverage từ sự tồn tại của file transformer.

Finalization đã revalidate inventory/metadata/preview, sau đó **B4 shared recovery pass** ([evidence](nso_b4_shared_validation_evidence.json)): NSO current snapshot `3499764399995403202`, schema 49 fields, 833 rows; mọi source counts và chín bảng Bronze khác giữ nguyên. B4 không tăng live Silver coverage; fixture probe đề xuất trong `silver_validation` không nằm trong business denominator.

Business sources = **9**. Fixture = **1**. Tổng Bronze = **1.338.087 rows**, trong đó nghiệp vụ **1.336.087**. Live validated Silver = **0/9**, vì REST không có namespace Silver. Preview/isolated integration đã xử lý 8 nguồn nhưng không làm thay đổi numerator live.

## Inventory toàn bộ catalog

Tất cả source tables ở `iceberg.bronze.<source_id>`. `Files` là per-file lineage trong Bronze, không phải số ZIP landing objects. Các bảng đều có một ingestion run thành công ngày 04/10/2026 và một run SKIPPED unchanged_snapshot; không phát hiện source bổ sung/missing so với registry/raw/manifests.

| Source ID | Files | Rows | Schema fields | Snapshots | Thời gian nguồn | Classification |
|---|---:|---:|---:|---:|---|---|
| faostat_production | 1 | 22.738 | 23 | 2 | 1961–2024 | IMPLEMENTED_NOT_VALIDATED |
| faostat_monthly_price | 1 | 15.334 | 24 | 2 | 1991–2025 | IMPLEMENTED_NOT_VALIDATED |
| faostat_supply_utilization | 1 | 36.380 | 23 | 2 | 2010–2023 | IMPLEMENTED_NOT_VALIDATED |
| faostat_trade | 1 | 1.226.470 | 24 | 26 | 1986–2024 | IMPLEMENTED_NOT_VALIDATED |
| nso_vietnam | 13 | 833 | 49 | 16 | National 1990–2024; matrices 1995–2024 | BLOCKED_BY_CONTRACT_MAPPING |
| usda_psd | 1 | 15 | 78 | 2 | 1960/1961–2025/2026, 66 crop years | IMPLEMENTED_NOT_VALIDATED |
| usda_rice_yearbook | 1 | 13.131 | 18 | 2 | 1985–2025; monthly/CALENDAR/MARKETING | IMPLEMENTED_NOT_VALIDATED |
| worldbank_pinksheet | 1 | 792 | 97 | 2 | 1960M01–2025M12 | IMPLEMENTED_NOT_VALIDATED |
| thitruongnongsan | 1 | 20.394 | 16 | 2 | 04/01/2021–03/08/2026, daily | IMPLEMENTED_NOT_VALIDATED |
| faostat_trade_validation | 1 | 2.000 | 24 | 3 | Trade slice 1986–2024 | TEST_FIXTURE_ONLY |

Complete column names/types, files/checksums/run IDs nằm trong [baseline inventory](bronze_inventory_evidence.json); NSO sau B4 xem [current audit](nso_b4_after_audit.json). NSO current snapshot `3499764399995403202`, baseline trước repair `8554544144555949741`. Snapshot IDs là identifier, không phải sequence để so độ mới.

Technical fields gồm run/batch/checksum/source/file `string`, chunk và source_snapshot `long`, ingestion_timestamp `timestamptz`. Một số source codes bị numeric inference: M49/CPC của Production/Price là long, CPC SUA double; Silver khôi phục code quan sát đã biết, không suy đoán ISO mapping.

## Mapping contract, grain và execution

Contract ở `contracts/silver/<source_id>.yaml`, target đều thuộc `iceberg.silver`.

| Source | Transformer tái dùng | Target suffix | Grain Silver | Runtime |
|---|---|---|---|---|
| Production | FaostatProductionTransformer | faostat_production | country × CPC × year × element | 22.738 output preview; isolated full/incremental pass |
| Price | FaostatMonthlyPriceTransformer | faostat_monthly_price | country × CPC × year × month_code × element | 15.334; annual/monthly/index tách biệt |
| SUA | FaostatSupplyUtilizationTransformer | faostat_supply_utilization | country × CPC × year × element | 36.144 + 236 quarantine dự kiến |
| Trade | FaostatTradeTransformer | faostat_trade | reporter × partner × CPC × year × element | 1.226.470; quantity/value/direction giữ riêng |
| NSO | NsoVietnamTransformer (proposal) | UNKNOWN/pending | source table × geography × year × season × measure × statistic kind | Bronze shared recovery pass; Silver blocked bởi approved contract/unit/geography |
| PSD | UsdaPsdTransformer | usda_rice_psd | country × commodity × attribute × market_year | 924 + 66 quarantine dự kiến |
| Yearbook | UsdaExportPriceTransformer | usda_export_price | exporter × class × year × reference_period × statistic | 12.936, dedup 195 |
| WB | WorldBankTransformer | worldbank_commodity_monthly | commodity × period_date | 56.232; explicit padding exclusion |
| Domestic | ThitruongnongsanTransformer | thitruongnongsan | market × commodity × price_type × date × unit × currency | 20.383, dedup 11 |
| Validation fixture | Production FaostatBulkAdapter dùng cho test | Không có production Silver | Trade test slice | Loại khỏi business denominator/lịch/marts |

Cả 9 business IDs có CLI registration và Airflow task; NSO entry point fail rõ thay vì skip. Các source wrappers cũ của Phúc giữ tương thích, alias `usda_export_price` resolve về canonical Bronze `usda_rice_yearbook`. Năm wrappers mới dùng shared runner.

Tám READY sources chạy source/common DQ và quarantine qua framework; tests bao gồm bounded Bronze samples, actual temporary Iceberg full + filtered incremental, no-op snapshots. Source-specific FAOSTAT có thêm unit/flag/negative/overflow/period tests. NSO chưa có approved production contract và chưa có live DQ/quarantine output. Không có source nào mang nhãn COVERED_AND_VALIDATED trên catalog chung.

Fixture có `extra.dataset_role: TEST_FIXTURE_ONLY`; vẫn enabled để gọi ingestion/test thủ công, nhưng ingestion DAG loại khỏi schedule và Silver registry không nhận fixture. Gold không được dùng bảng này để tăng số liệu trade.
