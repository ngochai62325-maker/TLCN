# Báo cáo DQ và reconciliation

Ngày: 10/10/2026. Counts tám nguồn dưới đây là **read-only Bronze → prospective Silver**, chưa ghi Silver chung. Riêng NSO đã ghi shared Bronze recovery B4 và kiểm chứng [PyIceberg/Trino](nso_b4_shared_validation_evidence.json): 833 rows/49 fields, snapshot `3499764399995403202`; không publish NSO Silver.

Finalization đã chạy lại [preview](silver_finalization_preview_evidence.json) và [SQL profiles](bronze_finalization_profile_evidence.json), baseline comparisons khớp và reconciliation PASS. B4 đã khôi phục 22.500 non-null matrix cells = 21.554 numeric + 946 missing markers trên shared Bronze, full-row/lineage equality PASS và rerun NOOP. Không coi các cells là Silver observations đã publish. [Business review](silver_business_quality_review.md) và [NSO E decisions](nso_phase_e_decisions.md) giữ policy hiện hành, gồm 236 SUA/66 PSD quarantine.

## Counts theo nguồn

| Source | Bronze rows | Expanded candidates | Valid trước dedup | Quarantine dự kiến | Explicit exclusions | Dedup removed | Prospective output |
|---|---:|---:|---:|---:|---:|---:|---:|
| Production | 22.738 | 22.738 | 22.738 | 0 | 0 | 0 | 22.738 |
| Price | 15.334 | 15.334 | 15.334 | 0 | 0 | 0 | 15.334 |
| SUA | 36.380 | 36.380 | 36.144 | 236 | 0 | 0 | 36.144 |
| Trade | 1.226.470 | 1.226.470 | 1.226.470 | 0 | 0 | 0 | 1.226.470 |
| PSD | 15 | 990 | 924 | 66 | 0 | 0 | 924 |
| Yearbook | 13.131 | 13.131 | 13.131 | 0 | 0 | 195 | 12.936 |
| WB | 792 | 69.696 | 56.232 | 0 | 13.464 | 0 | 56.232 |
| Domestic | 20.394 | 20.394 | 20.394 | 0 | 0 | 11 | 20.383 |
| NSO | 833 | Bronze matrix 22.500 non-null cells; Silver grain chưa duyệt | — | — | — | — | BLOCKED_BY_CONTRACT_MAPPING |

Tổng prospective output tám nguồn: **1.391.161**; quarantine **302**. Counts lỗi theo rule không được cộng thành số rows: mỗi SUA record vi phạm cả contract rule và source rule nhưng chỉ quarantine một lần.

Row-preserving: input = valid + quarantine + explicit exclusions. Wide-to-long: expanded candidates = valid + quarantine + explicit exclusions; valid = output + dedup removed. Không so 15 PSD rows hay 792 WB rows trực tiếp với long observations. Mọi phương trình preview đều khớp; NSO bị block rõ, không ghi một phần rồi báo complete.

## Kiểm tra source semantics

| Source | Kết quả |
|---|---|
| Production | Production/area/yield được kiểm tra code-name-unit; chuyển kg→tonne, hg/ha→kg/hectare theo source rule. 229 values NULL được giữ như missing; không biến thành zero. M49/CPC, source flags/note và raw đều giữ. |
| Price | 5.341 monthly + 9.993 annual; trong annual có 3.136 price-index observations. Currency giữ LCU/SLC/USD theo element; index currency NULL. Không sinh tháng từ annual 7021. |
| SUA | 12 long elements; 1.280 Stock Variation âm được giữ, 236 Residuals âm quarantine. 257 values NULL; rice và milled rice không bị gộp. |
| Trade | Import quantity 314.937, import value 314.937, export quantity 298.297, export value 298.299. `t` và `1000 USD` không gộp; reporter/partner và aggregate flags giữ. Không fallback native FAO IDs thành M49/CPC. |
| PSD | 15 attributes × 66 crop years. Area 1000 HA, yield MT/HA, quantities 1000 MT. Crop-year interval giữ nguyên. 66 Milling Rate observations cần xác minh scaled ratio. |
| Yearbook | Bốn table numbers thực tế 25–28, trái với note cũ 18–21 trong registry. 195 duplicates trong grain; reference period tháng/CALENDAR/MARKETING được giữ, không dựng calendar date. 4.523 NULL prices trong prospective output. |
| WB | 71 source series × 792 tháng = 56.232. 17 `commodity_72`…`commodity_88` là adapter-generated names cho header/unit trống, toàn bộ cells NULL: loại rõ 13.464 padding candidates. Nếu cột synthetic có dữ liệu sẽ không bị loại, phải qua DQ. Missing `…`, `...`, NA giữ NULL; 6.405 output prices NULL. Index không mang đơn vị USD/unit giả. |
| Domestic | Date raw `7/31/...` chứng minh M/d/yyyy; giữ daily grain. Bốn nhãn `VNĐ/Kg`, `VNĐ/kg`, `Vnđ/Kg`, `Đồng/kg` cùng chuẩn VND/kg, không đổi numeric scale. Province/product giữ source labels; 11 duplicate observations sau normalization. |

Counts NULL/warnings trong preview JSON được tính trên dataframe **sau dedup**, còn `valid_count` tính trước dedup. Duplicate ties có survivorship cố định timestamp → source_file → payload hash; khi raw không có thứ tự correction trong cùng timestamp, hash chỉ đảm bảo deterministic, không chứng minh bản thắng đúng hơn về nghiệp vụ. Cần review các duplicate có conflicting values trước Gold.

## Residuals SUA: giữ policy, nêu rõ giới hạn

Raw thực tế có element 5166 `Residuals`, unit `t`. Ví dụ Rice: Mexico 2022 −474.083; Mexico 2023 −428.268; Guinea 2014 −332.614; Paraguay 2023 −286.333. Các ví dụ này mang flag I; source value/flag/payload không bị xóa hoặc lấy absolute value.

FAO mô tả residuals trong food-balance methodology như thành phần phản ánh độ lệch giữa phương pháp ước lượng và nguồn dữ liệu. Đây là cơ sở để **xem xét** signed residuals, không phải bằng chứng đủ để tự đổi contract SUA hiện tại. Xem [FAO Analytical Brief 72, 2023](https://nutritionconnect.org/media/1193) và [FAO Food Balances metadata](https://data.fao.org/catalog/iso/2f264bb6-1238-459a-bf8b-0e2d0a16804a).

Suy luận cần business review: giá trị residual âm có thể biểu thị accounting/statistical imbalance, không nhất thiết là khối lượng sản xuất vật lý âm. Loại khỏi Silver làm thiếu thành phần khi Gold cân đối SUA; chuyển dấu/NULL sẽ thay ý nghĩa. Hiện giữ nguyên quarantine 236, chờ quyết định riêng D.

## PSD Milling Rate

Source HTML `data/raw/usda/usda.xls` đã có `Milling Rate (.9999)` với `Unit Description = (1000 MT)`; Bronze adapter giữ nguyên. Giá trị gần đây 6250. Không kết luận adapter tự gán unit. [USDA FAS report](https://apps.fas.usda.gov/newgainapi/api/Report/DownloadReportByFileName?fileName=Rice+Production+Update_Seoul_Korea+-+Republic+of_11-29-2011.pdf) cũng biểu diễn milling-rate theo `.9999` cùng bảng mass quantities; việc chia 10.000 là đề xuất cần xác nhận cho snapshot này. Source rule quarantine 66 observations để tránh công bố tỷ lệ như tonnes.

## Lineage, retry và giới hạn live

Tất cả Bronze rows có tám technical fields; `_source_snapshot_id=0` chưa liên kết PostgreSQL source_snapshots, nên mọi prospective valid output có warning `LINEAGE_REFERENCE`. Actual Iceberg snapshot được pin và lưu riêng; không ghi ngược vào Bronze.

Isolated tests kiểm chứng exact rerun/no new snapshot, filtered incremental, late correction, stale batch, deterministic tie, null-key refusal, schema-change refusal, duplicate quarantine/retry và retry sau lỗi giữa quarantine/Silver. Full/incremental đã kiểm thử actual temporary Iceberg cho cả tám READY sources, bao gồm Base.execute của Phúc. **Shared Silver retry/corrections/E2E chưa chạy**. Riêng NSO Bronze B4 đã pass shared Trino, cell/lineage/schema checks và NOOP; `_source_snapshot_id=0` không đổi. Profile NSO có 360 matrix NULL cells, 560 national non-null measure cells và một all-source-NULL national row; [E decisions](nso_phase_e_decisions.md) yêu cầu duyệt missing/footer/statistic handling trước normalization.
