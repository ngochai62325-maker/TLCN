# Danh mục dữ liệu tầng Silver (Nine-Source Data Catalog)

Số liệu dưới đây là **chuẩn tham chiếu đã được kiểm chứng** trên bộ 21 file Raw đã pin trong [`config/local_raw_manifest.json`](../../config/local_raw_manifest.json) (cùng `data/raw/nso/metadata.txt`).
Trên một stack `tlcn-local` mới khởi tạo (`0 DagRuns`), các bảng Bronze và Silver chỉ được tạo và nạp dữ liệu sau khi người dùng kích hoạt DAG trên Airflow UI theo [Hướng dẫn trình diễn (`demo_guide.md`)](demo_guide.md).

Tất cả các bảng đích trong môi trường local nằm dưới namespace `iceberg.silver_local.`. Đặc tả schema và khóa nghiệp vụ được định nghĩa tại [`contracts/silver/`](../../contracts/silver/).

| Nguồn Bronze / Dataset Silver | Bảng đích Silver (`iceberg.silver_local.*`) | Số dòng Bronze | Số dòng Silver | Đơn vị chuẩn hóa (`unit`) |
|---|---|---:|---:|---|
| `faostat_production` / `faostat_production` | `faostat_production` | 22.738 | 22.738 | `hectare`, `kg/hectare`, `tonne` |
| `faostat_monthly_price` / `faostat_monthly_price` | `faostat_monthly_price` | 15.334 | 15.334 | `None`, `LCU`, `SLC`, `USD` |
| `faostat_supply_utilization` / `faostat_supply_utilization` | `faostat_supply_utilization` | 36.380 | 36.380 | `tonne` |
| `faostat_trade` / `faostat_trade` | `faostat_trade` | 1.226.470 | 1.226.470 | `1000 USD`, `tonne` |
| `usda_psd` / `usda_psd` | `usda_rice_psd` | 15 | 990 | `1000 MT`, `fraction`, `MT/HA`, `1000 HA` |
| `usda_rice_yearbook` / `usda_rice_yearbook` | `usda_rice_yearbook` | 13.131 | 12.936 | `USD/tonne` |
| `worldbank_pinksheet` / `worldbank_pinksheet` | `worldbank_commodity_monthly` | 792 | 56.232 | `USD/tonne`, `USD/kg`, `USD/barrel`, `USD/troy_oz`, `USD/dmtu`, `USD/MMBtu`, `USD/cubic_meter`, `index(2010=100)`, `cents/sheet` |
| `thitruongnongsan` / `thitruongnongsan` | `thitruongnongsan` | 20.394 | 20.387 | `VND/kg` |
| `nso` / `nso` | `nso_rice_statistics` | 833 | 23.060 | `hectare`, `tonne`, `percent`, `kg/hectare` |

- **Tổng số quan sát tại tầng Silver**: **1.414.527 quan sát**.
- `usda_psd` ở tầng Bronze có dạng bảng rộng (15 dòng), sau khi unpivot tạo thành 990 quan sát Silver.
- `usda_rice_yearbook` loại bỏ 195 bản ghi trùng lặp.
- `worldbank_pinksheet` loại bỏ 13.464 ô trống thuộc 17 cột trailing rỗng không có tên hàng hóa.
- `thitruongnongsan` giữ lại 20.387 dòng hợp lệ sau khi cách ly 4 dòng xung đột giá vào `dead_letters` và khử 3 dòng trùng lặp.
- `nso` chuẩn hóa 13 bảng CSV (833 dòng Bronze) thành **23.060 quan sát** và ghi nhận **13 bản ghi loại trừ cấu trúc** vào `dead_letters` (`source_dataset = 'nso_exclusions'`).

## 1. Độ mịn dữ liệu (Grain) & Khóa nghiệp vụ (Business Keys)

- **`faostat_production`**: `country_code, commodity_code, year, element_code`
- **`faostat_monthly_price`**: `country_code, commodity_code, year, month_code, element_code`
- **`faostat_supply_utilization`**: `country_code, commodity_code, year, element_code`
- **`faostat_trade`**: `reporter_country_code, partner_country_code, commodity_code, year, element_code`
- **`usda_psd`**: `country, commodity, attribute, market_year`
- **`usda_rice_yearbook`**: `location, commodity_class, year, reference_period, statistic_type`
- **`worldbank_pinksheet`**: `commodity_code, period_date`
- **`thitruongnongsan`**: `market, commodity, price_type, unit, currency, source, date`
- **`nso`**: `source_table, source_geography_key, year, measure, season, statistic_kind`

## 2. Chất lượng dữ liệu & Bảng `dead_letters`

- Các quy tắc kiểm tra chất lượng (DQ rules) được định nghĩa trong từng file YAML tại [`contracts/silver/`](../../contracts/silver/).
- Bản ghi vi phạm quy tắc có `action: QUARANTINE` sẽ bị chặn không ghi vào bảng Silver và được đưa vào `iceberg.silver_local.dead_letters`.
- Bản ghi vi phạm quy tắc `LOG` / `KEEP` vẫn được giữ trong bảng Silver kèm mảng cảnh báo `_dq_warnings` (ví dụ: 236 dòng `Residuals` âm trong `faostat_supply_utilization`, 29 dòng cảnh báo trong `thitruongnongsan`).
- Bảng `iceberg.silver_local.dead_letters` có tổng cộng **17 bản ghi**:
  - `4` bản ghi từ `thitruongnongsan` (xung đột giá cho cùng khóa nghiệp vụ).
  - `13` bản ghi từ `nso_exclusions` (các dòng cấu trúc không phải quan sát số liệu trong 13 bảng CSV của NSO).

## 3. Ngữ nghĩa chuẩn hóa nguồn `nso` (`nso_rice_statistics`)

- **Tổng số quan sát**: **23.060 dòng**, bao gồm **22.114** quan sát có giá trị số (`value IS NOT NULL`), **946** quan sát khuyết dữ liệu tường minh (`missing_reason IS NOT NULL`, `value IS NULL`), và **5** quan sát có giá trị `0` thực sự.
- **Đơn vị & Hệ số quy đổi**:
  - Diện tích (`area`): `Nghìn ha` → `hectare` (`conversion_factor = 1000`).
  - Sản lượng (`production`): `Nghìn tấn` → `tonne` (`conversion_factor = 1000`).
  - Năng suất (`yield`): `Tạ/ha` → `kg/hectare` (`conversion_factor = 100`).
  - Chỉ số phát triển (`index` / `PERCENT_PREVIOUS_YEAR_100`): `%` → `percent` (`conversion_factor = 1`).
- **Mùa vụ (`season`)**: Phân biệt rõ `annual`, `winter_spring` (Đông Xuân), `summer_autumn_fall` (Hè Thu - Mùa), `summer_autumn` (Hè Thu), `autumn_winter` (Thu Đông), `main_rainy` (Mùa).
- **Địa lý & Truy vết**: Giữ nguyên `geography_name_raw`, `source_geography_key`, và phân cấp `geography_level` (`COUNTRY`, `REGION`, `PROVINCE_CITY`). Mọi dòng đều lưu thêm `bronze_read_snapshot_id` (`BIGINT`), `metadata_version = "nso-v2-20261015.1"` và `unit_mapping_version = "nso-units-20261015.1"`.
