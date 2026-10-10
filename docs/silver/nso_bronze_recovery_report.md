# NSO Bronze recovery — B1/B2/B3

Ngày kiểm chứng: 10/10/2026 (Asia/Bangkok). **B1/B2/B3 hoàn thành; sau phê duyệt riêng, B4 shared recovery cũng đã verified.** Current snapshot `3499764399995403202`, 49 fields/833 rows, shared NOOP PASS ([B4 report](nso_b4_shared_recovery_results.md)). Nội dung B1/B2/B3 dưới đây giữ baseline trước write: [recovery](nso_recovery_evidence.json), [baseline inventory](bronze_finalization_inventory_evidence.json), [metadata](ingestion_metadata_finalization_evidence.json), [business examples](nso_business_profile_evidence.json).

## Đối chiếu nguồn thực tế — baseline B1 trước B4

REST vẫn có `iceberg.bronze.nso_vietnam`, snapshot `8554544144555949741`, schema 18 fields, 833 rows và 13 source files. Mười Bronze tables/9 business sources không thay đổi; chưa có namespace Silver. Registry dùng `NsoVietnamAdapter`, local `data/raw/nso`, pattern `V06.*.csv`, không yêu cầu schema đồng nhất. Adapter emits một chunk mỗi file, giữ checksum và `_source_file`.

ZIP `s3://bronze/raw/nso_vietnam/ingestion_date=2026-10-04/run_id=nso_vietnam_20261004145836_dbf324d8/nso_vietnam_raw.zip` chứa đúng 13 CSV. Bytes từng member khớp local. `metadata.txt` chỉ có ở local, không nằm trong ZIP vì file pattern chỉ lấy CSV; SHA riêng đã lưu trong evidence. ZIP là landing format hợp lệ, được giữ nguyên.

| Đại lượng | Kết quả |
|---|---|
| ZIP SHA-256 thực | `eb231503f3a1051d226100c699983c7f1b420f11e205cc1e419a90373f5d700d` |
| Directory-content checksum trong manifest | `b4bbdad365bf03e4a1867c4ff3b73829335955047d6e6f007da74bcb5993580a` |
| CSV checksums/local equality | 13/13 khớp; từng SHA và header mapping có trong JSON |
| Physical rows | 833; V06.12: 71, V06.13–24: 762 |
| Missing fields | 31: `t_nh_th_nh_ph_`, `col_1995`…`col_2023`, `so_b_2024` |
| Matrix cells non-null | 22.500 |
| Numeric cells | 21.554 |
| Missing-marker cells | 946; giữ raw token, không tính thành numeric observation |
| Schema sau recovery isolated | 49 fields; 31 additions là string nullable |
| Retained national values + eight lineage fields | Multiset equality PASS; không đổi timestamps/run/batch/chunk/checksum/source snapshot |

Báo cáo trước gán nhầm directory checksum thành ZIP hash. `FullLoader._compute_source_checksum` hash filename + CSV bytes theo thứ tự; `_find_source_artifact` tạo ZIP sau đó. Hai checksum khác nhau đúng theo hai miền dữ liệu, không chứng minh archive hỏng. Không đổi raw/manifest để làm chúng giống nhau.

## Root cause đã tái hiện

Schema đầu tiên lấy V06.12. Writer cũ trả về sớm khi table tồn tại và Arrow conversion chọn duy nhất schema fields; national fields của matrix trở thành NULL, 31 extra fields không được ghi. Unit reproduction cho thấy field-only alignment bỏ cell có dữ liệu; writer hiện tại từ chối thao tác đó trước DELETE. Không thể khôi phục geography/year bằng Silver từ 762 rows hiện tại nếu bỏ qua raw.

CSV parser giữ physical footer row như ingestion adapter. Spark CSV có thể bỏ footer toàn rỗng nên không được dùng thay parser để ép 833 thành 832. Recovery parse UTF-8/BOM rồi Latin-1 theo adapter, sanitize headers với collision guard, union theo tên, giữ NULL và missing markers. National stored projection khớp raw parser trực tiếp trong lần kiểm chứng này; không cần sửa encoding hay labels cũ.

## Writer đã sửa

`BronzeIcebergWriter` mặc định `schema_policy="fail_fast"`. Cột mới/collision/schema thiếu technical metadata gây `SchemaError`. `approved_evolution` chỉ cho phép các source fields **string nullable** trong allowlist của đúng source; không đổi type/field ID hoặc drop fields. Native `update_schema` commit toàn bộ additions; khi native path thất bại, không fallback ALTER từng cột. Nine other sources tiếp tục policy mặc định; không bật evolution trong registry.

Conversion/preflight chạy trước legacy retry DELETE. Malformed/fractional/out-of-range numeric values bị từ chối thay vì `errors="coerce"` thành NULL. Legacy `write_chunk` vẫn có DELETE+append không atomic; **không dùng đường này cho shared recovery**.

## Replay identity và transaction

Evidence định danh mỗi physical record bằng run × chunk × file × checksum × ordinal trong raw CSV. Có 833 IDs duy nhất; ordinal chỉ dùng trong audit, không thêm business key hay cột fake vào Bronze. Recovery không MERGE theo geography/year chưa được duyệt và không loại duplicate raw rows.

`atomic_recover` thêm 31 columns và overwrite full verified 833-row payload trong **một table transaction**. Schema trước commit, old values và technical lineage được đối chiếu. Rerun so multiset toàn bộ payload để trả NOOP, không blind append. Snapshot thay đổi ngoài recovery gây stop; optimistic concurrent append được test từ chối bằng commit/validation exception.

Actual SQLite/filesystem isolated đã kiểm chứng: failure sau staging/trước commit giữ schema/snapshot/data cũ; success đọc đủ 833/49 và từng cell bằng nhau; rerun giữ snapshot; rollback khôi phục old projection, giữ history và nullable schema additions. Không suy ra shared REST success từ các test này. Theo [PyIceberg transaction API](https://py.iceberg.apache.org/api/), schema/data updates có thể đặt trong cùng transaction; runtime khả dụng ghi trong evidence.

Compatibility cũng PASS trên PyIceberg 0.12.0/PyArrow **16.1.0** trong container network none, đúng Arrow version scheduler: [portable evidence](nso_arrow16_isolated_evidence.json). ZIP 54.901 bytes, decompressed CSV content 137.301 bytes. Shared commit/Trino validation sau phê duyệt B4 đã pass, evidence riêng không suy ra từ isolated tests.

Lệnh thực tế đã chạy, chỉ đọc shared services và ghi warehouse tạm:

```powershell
$env:PYTHONPATH = 'src;.pytest_cache/nso_recovery_deps'
python jobs/bronze/recover_nso.py --mode isolated --output docs/silver/nso_recovery_evidence.json
```

Lệnh test, log và failures ban đầu: [test results](silver_integration_test_results.md). Shared execution đã verified tại [B4 results](nso_b4_shared_recovery_results.md), rollback/partial-failure plan giữ nguyên. NSO contract vẫn NEEDS_PROFILING; [E decisions](nso_phase_e_decisions.md) chưa được duyệt và chưa publish Silver.
