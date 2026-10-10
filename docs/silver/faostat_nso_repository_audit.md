# Audit Silver FAOSTAT + NSO — 10/10/2026

## Repository và bằng chứng

- Root: `TLCN`; branch `feat_silver_role2`; working tree sạch trước khi làm việc.
- Không tìm thấy `AGENTS.md` trong repository/parent đã kiểm tra.
- Không tìm thấy ba tài liệu `phân công silver.docx`,
  `KTDL_26TLCN_Nhom10_3_10 - Copy.docx`,
  `KTDL_26TLCN_Nhom10_19-9 - Copy.docx`; không có DBML/data dictionary riêng.
  Dùng yêu cầu người dùng và YAML hiện có làm specification.
- Đã đọc README, Compose, dependencies, configs, adapters/writer Bronze,
  toàn bộ Silver framework/engines/contracts, các transformer/jobs và hai DAG,
  các test liên quan. Các kết quả dưới đây là audit code và truy vấn chỉ đọc,
  không phải nghiệm thu chạy pipeline Silver.

## Kiến trúc thực tế

Bronze sử dụng adapters Pandas theo chunk → MinIO raw/manifest → metadata
PostgreSQL → Bronze Iceberg append (PyIceberg, fallback Trino). Sanitizer biến
`Area Code (M49)` thành `area_code_m49_`; metadata có tám cột kỹ thuật.

Hai đường Silver cùng tồn tại:

1. Hải: `SilverTransformationFramework.run(dataset_id, pipeline_run_id)` đọc
   Bronze theo YAML, tự import `silver.transformers.<dataset_id>` và gọi
   `<DatasetId>Transformer().preprocess(df)`, cast/project contract,
   `SilverQualityEngine.apply_rules(df)` → quarantine →
   `IcebergMergeEngine.deduplicate/merge`. Extension hiện hữu là `preprocess`.
   Không thấy SchemaValidator/CountryNormalizer/CommodityNormalizer/
   UnitNormalizer/DateNormalizer riêng; YAML transformation descriptions chưa
   được thực thi. Framework bỏ metadata batch/chunk/source_id khi projection,
   không có incremental filter và thay bronze_count sau preprocessing.
2. Phúc: `BaseSilverTransformer.read_bronze/transform/deduplicate/write_silver/
   execute`, Spark factory, bốn transformer Trade/PSD/Export Price/World Bank,
   jobs và `run_all_silver.REGISTRY`. Hỗ trợ timestamp/run filter, MERGE,
   nhưng không gọi contract DQ/quarantine; writer yêu cầu target có sẵn.
   Không sửa logic các nguồn này trong phạm vi Hoàng.

Không viết framework/mapping engine thứ hai. Phần Hoàng tái sử dụng
SilverContractLoader, SilverQualityEngine, IcebergMergeEngine và
SilverQuarantineManager; source preprocessing theo pattern động của Hải.

## Audit nguồn thực tế

REST `/v1/namespaces` chỉ có `bronze`, chưa có `silver`. Trino 448 trả lời
query dù Docker health đang unhealthy. Spark container chưa chạy.
Counts từ snapshot summaries được kiểm chứng bằng Trino SELECT.

| Nguồn | Bronze rows | Contract | Transformer trước thay đổi | Pipeline |
|---|---:|---|---|---|
| Production | 22.738 | READY, long, country/CPC/year/element | Missing | Partially implemented: ingestion DAG gọi framework |
| Price | 15.334 | READY, thêm month_code | Missing | Missing trong hai DAG/runner |
| SUA | 36.380 | READY, long, country/CPC/year/element | Missing | Missing trong hai DAG/runner |
| NSO | 833 | NEEDS_PROFILING, UNKNOWN tables/schema | Missing | Blocked bởi contract và schema Bronze |

CSV FAOSTAT không có duplicate theo business keys hiện có. Profiling CSV là
evidence nguồn local, không thay thế duplicate/null audit toàn bảng live.

- Production: `5312/Area harvested/ha`, `5412/Yield/kg/ha`,
  `5510/Production/t`; CPC `0113/Rice`; 229 giá trị NULL, không âm.
- Price: `5530/LCU`, `5531/SLC`, `5532/USD`, `5539/Producer Price Index
  (2014-2016 = 100)/NULL unit`. Monthly `7001..7012`, annual `7021`.
  Không NULL value, không âm. 9.993 annual records không được gán tháng giả.
- SUA: CPC `0113` và `23161.02/Rice, milled`; unit `t`; 257 NULL value.
  Có 1.280 Stock Variation (`5071`) âm và 236 Residuals (`5166`) âm.
  Contract hiện chỉ cho `5071` âm: giữ rule hiện hành và quarantine `5166`
  âm, không tự sửa source hay áp balance equations.
- Bronze FAOSTAT đã infer mã M49/CPC thành long/double: `0113` thành `113`
  hoặc `113.0`. Khôi phục `0113` chỉ theo mapping đã quan sát; không pad
  tùy tiện mọi CPC. M49 lịch sử `810`, `890`, `736` và FAO China codes
  `158/159` cần giữ định danh nguồn, không tự ánh xạ sang quốc gia hiện tại.
- NSO: 13 CSV không đồng schema; V06.12 national wide, V06.13..24 matrix
  địa phương 1995..2024 (cột `So b? 2024`). metadata.txt xác định annual,
  winter_spring, summer_autumn_autumn_winter và mùa. File có NUL footer,
  development-index rows và tên tiếng Việt mất ký tự `?` từ source.
  Bronze schema chỉ có cột V06.12; SELECT cho 12 file khác có 0 dòng với
  year/measure thuộc schema hiện hành. Không thể khôi phục tỉnh/năm/measure
  từ schema queryable này bằng Silver preprocessing.

## Rủi ro và chênh lệch

- DQ PRICE/SUA chỉ là natural language, engine sẽ raise vì thiếu sql_expr.
- Engine `~condition` bỏ qua SQL NULL; source rules phải explicit coalesce
  false. Production có hardcoded rules nên không thể override bằng YAML.
- Framework casting có thể biến malformed numeric thành NULL hợp lệ;
  payload quarantine hiện chỉ giữ output đã project, mất raw observation.
- Tie-break dedup cùng timestamp/file chưa deterministic; MERGE dùng view
  chung `incoming_updates`, cập nhật kể cả rerun không đổi và không chặn
  batch cũ ghi đè batch mới. Quarantine MERGE có thể có duplicate ID trong
  batch. Partition/ZSTD/v2 trong docs chưa được merge engine cấu hình.
- Ingestion DAG chỉ hook Production + PSD; Silver DAG chỉ bốn nguồn Phúc.
  `gate_check_bronze` chỉ ping API; DQ audit trả PASSED mà không query.
- Tài liệu kiến trúc ghi kết quả E2E ngày 09/10; catalog hiện tại không có
  Silver nên không thể xác nhận kết quả đó trong môi trường hiện tại.
- Local Python 3.13, Java 22; khả năng chạy Spark phải test thực tế.
- Existing integration PoC có DROP shared dead_letters; không chạy test đó.

## Kế hoạch theo file và quyền sửa

Được triển khai ngay, không thay contract/schema/DAG đã công bố:

- `src/silver/transformers/faostat_source.py`: helpers riêng FAOSTAT, typed
  output theo YAML hiện tại, source-specific SQL DQ dùng engine hiện hữu,
  raw payload, metadata, deterministic source mapping. Không phải framework.
- `src/silver/transformers/faostat_production.py`, `faostat_monthly_price.py`,
  `faostat_supply_utilization.py`: extension `preprocess` và validation cho
  từng source; không pivot, không aggregate, không fill/drop.
- `src/silver/transformers/nso_vietnam.py`: kiểm tra completeness schema,
  fail rõ khi Bronze mất cột. Chỉ unpivot nếu input thật có cột matrix;
  output là proposal chưa xuất bản, không tự ghi bảng UNKNOWN.
- `jobs/silver/preview_faostat_nso.py`: runner chỉ đọc, normalize/DQ/reconcile,
  không ghi Silver/quarantine, không tạo namespace/snapshot.
- `tests/unit/silver/test_faostat_nso_transformers.py`, fixtures và integration
  không ghi shared tables: kiểm chứng edge cases, source mapping, shared DQ,
  dedup và framework dispatch/projection; không tuyên bố Iceberg idempotency
  nếu chưa thật sự MERGE.
- `docs/silver/faostat_nso_source_transformations.md`: mapping, grain, limitation,
  test/run commands và kết quả.

Chờ approval, proposal cụ thể:

1. Framework: giữ thêm `_source_payload`, `_ingestion_batch_id`,
   `_ingestion_chunk_id`, `_source_id` và derived columns theo source contract;
   hook source SQL DQ sau cast; giữ input/preprocessed counters riêng;
   thêm optional full/incremental timestamp/run filter mà giữ API cũ.
2. Price/SUA YAML: thêm explicit sql_expr cho rule hiện hữu; Price bổ sung
   `time_grain`, `month` nullable, `currency`, `price_kind` để phân biệt
   annual/index/monetary. Business key và decimal hiện tại giữ nguyên.
   Annual: month=NULL, month_code=7021; currency index=NULL; LCU/SLC/USD
   giữ nguyên, không currency conversion.
3. NSO contract mới: long grain source table × geography identity/level ×
   year × season × measure × statistic_kind; national/region/province riêng,
   code tỉnh unknown=NULL, không đoán tên hỏng. Cần khôi phục Bronze matrix
   qua owner ingestion trước khi publish contract production.
4. Runner registry/DAG hiện hữu: thêm ba FAOSTAT sau khi hooks DQ hoạt động;
   NSO chỉ khi contract/schema đạt. Không tạo DAG mới.
5. Shared engine/storage fixes (tie-break, partition, rerun snapshots,
   freshness guard) cần Hải/Phúc cùng duyệt. Không sửa tại audit này.

Không chạy jobs có ghi Iceberg dùng chung khi chưa có approval riêng.
