# Engineering foundation — nghiệm thu 27/10/2026

**FOUNDATION_READY_WITH_LIMITATIONS.** H01–H05 đã được triển khai và kiểm tra
trong phạm vi bên dưới. Có thể chuyển sang thiết kế Analytics query interface.
Không coi đây là nghiệm thu FULL replay chín nguồn thật trên ingestion code mới.
Không xây Dashboard, thêm dependency Dashboard hay chạy lại Gold.

## Baseline và bảo vệ dữ liệu

- Branch `feat(gold)`, HEAD `4bccf05ef1ba91f58d94df141cbbe6b97a28a6f4`;
  working tree sạch trước nhiệm vụ. Không commit/push/pull/merge/rebase.
- [Trước sửa](baseline_before.json), [sau isolated tests](baseline_after.json),
  [readback cuối](baseline_final.json): **36/36 bảng hiện hữu** giữ nguyên UUID,
  snapshot, metadata location và data-file paths. Gồm 10 Bronze (có fixture
  `faostat_trade_validation` cũ), 9 Silver, 17 Gold. Active whitelist vẫn đúng 9 nguồn.
- Published batch `ec421297c194804dc7a3a63aacfda37a65dacfac07245d118e34b34506ca2235`;
  manifest hash `631c0d372fcb37dfd81466622012a42311b867c028f570b4465658a9136a046b`
  và pointer/context/code/reference fingerprints không đổi.
- Gold model `gold-20261025.1`, semantic version `gold-semantic-20261026.1`;
  Gold code fingerprint `ce5f23517281b552d02f25529a70b064af320902976b4bf9c5291c99a564fa98`.
  Không sửa Gold modules/DAG/SQL/DBML/mapping, Silver contracts hoặc registry.
- Hai Gold DagRuns SUCCESS cũ và hai FAILED cũ giữ nguyên; không tạo DagRun mới.
  DQ01–18 và full reconciliation của Gold là evidence nghiệm thu G12 được bảo toàn,
  **không phải các gates đã chạy lại trong nhiệm vụ này**.
- Writer tests chỉ dùng namespace ngẫu nhiên `foundation_h05_<uuid>` và metadata
  có đúng source ID đó, hoặc Hadoop catalog không có network. Owned REST tables,
  files/namespaces/metadata rows được dọn sau test. Không reset volume/service.
  PostgreSQL sequences có thể tăng vì fixture INSERT; không sửa các source rows thật.

## H01 — Diagnostic quarantine

Lỗi cũ được tái hiện bằng test đỏ: `is_quarantined=True` nhưng repository không
được gọi. `QuarantineManager` hiện yêu cầu source/run/path và failure context hợp lệ;
ghi diagnostic vào `ingestion.dead_letter_records`, commit và đọc lại ID.
Advisory transaction lock theo diagnostic identity chống trùng khi retry.
URI dạng `postgresql://ingestion.dead_letter_records/<id>` chỉ được trả sau readback.
`quarantined_uri` trỏ Raw hiện hữu, không tuyên bố đã copy/archive. Raw không thay đổi.

Sink lỗi trả `QuarantinePersistenceError` kèm validation cause; FullLoader FAILED,
không ghi Bronze. Missing context không bị biến thành một lỗi corrupt-file tự bịa.
Tests bao gồm schema/corrupt/empty/source/context, sink failure, retry, references
và FullLoader. Physical fixture xác nhận một diagnostic sau hai lần gọi và Raw giữ nguyên.

## H02 — Silver Trino verifier

Loại `geography_name_vi`/`UNMAPPED`; dùng schema và DQ expressions trong contract
`nso.yaml`, dịch rõ Spark SHA-256 lowercase, `RLIKE`, `right` sang Trino. Schema type,
requiredness, key uniqueness, quantity/index, units, source identity, missing và
provisional/unconfirmed được kiểm tra; LOG warnings không thành lỗi blocking.
`unknown` hợp lệ không bị loại. Không giả định NSO có official geography codes.
NSO-only CLI ghi FAILED artifact khi query lỗi, không giữ báo cáo PASS cũ gây nhầm.

[NSO read-only](nso_trino.json): **23.060 rows/keys**, 0 blocking failures,
946 missing, 5 true zero, 766 provisional, 22.294 unconfirmed. Giữ 22.500 geography
review warnings và 344 discrepancy warnings. Query này chỉ chứng minh retained
Silver; không dùng sự vắng mặt để suy ra quarantine/exclusion count bằng 0.

## H03 — Môi trường test

Default pytest chỉ collect tests không cần JVM/Airflow/catalog; runtime files bị
loại trước import, opt-in `--with-runtime`. Dev dependencies khai báo PySpark cho
Python schema/expression types và `responses` cho HTTP mocks. Không blanket skip.
Silver jobs import được bằng package và chạy CLI `--help` từ repo root.

Python declaration 3.10–3.13; môi trường đã thực thi: host 3.13, Spark 3.10,
Airflow 3.11. Chưa thực thi riêng 3.12. Spark JVM fixtures không hỗ trợ host 3.13.
Windows managed sandbox có ACL lỗi với pytest mode-700 temp paths: các suites
chạy được ngoài sandbox và trong Linux fixture containers, không đổi ACL hệ thống.
Xem [commands và environment](TESTING.md).

## H04 — Recovery/commit correctness

[Failure matrix và retention policy](RECOVERY.md). Các thay đổi:

1. Bỏ DELETE/APPEND hai commits và multi-INSERT fallback. Một append per immutable
   run/chunk; chunk đã có phải khớp exact multiset, gồm NULL/zero/sign/precision và
   multiplicity. Chỉ bỏ qua timestamp khi so retry; mọi lineage field khác phải khớp.
2. Append mất response phải đọc catalog mới. Chỉ nhận thành công nếu exact content
   đã commit; không ghi lần hai để đoán. Catalog/content không xác định thì UNKNOWN_COMMIT.
3. Trước write, lưu immutable input context (Raw checksum, adapter/chunk layout,
   physical run/batch identity) và durable `physical_write_started`. Metadata lỗi
   chặn write. Chỉ checkpoint SUCCESS sau physical readback.
4. Recovery tạo logical run mới, giữ FAILED cũ; chỉ dùng lại physical identity nếu
   input context khớp. Re-extract/verify tất cả chunks, không bỏ qua theo checkpoint.
   Changed checksum/layout, legacy context thiếu hoặc UNKNOWN_COMMIT chưa giải quyết
   bị chặn. Input bị quarantine trước mọi write có thể sửa rồi ingest bình thường.
5. Physical full-run row/source readback có UUID/snapshot/metadata trước khi lưu
   batch commit, source snapshot và run completion. Metadata exceptions không bị nuốt;
   unchanged-checksum skip cũng phải kiểm tra persisted parent và physical rows.
6. `_source_snapshot_id` mới là NULL khi chưa có PostgreSQL source-snapshot identity,
   không phải 0 giả làm Iceberg pin. Catalog snapshot ID là proof riêng.
7. Repository connections được đóng; source locks vẫn session-scoped, fail fast.
   Local Bronze proof hiểu logical→physical run alias của recovery.

**Physical verification:** append thật → mất response → exact recovery; metadata
failure sau commit → run mới SUCCESS với physical identity cũ, FAILED history còn,
UUID/snapshot/metadata/files/contents không đổi; replay tiếp theo SKIPPED/NOOP;
immutable context chống thay đổi; hai DB connections không cùng giữ source lock;
quarantine → sửa Raw → ingest. Các ca còn lại là failure-injection mocks, không
được gọi là physical kill/process-crash acceptance.

## H05 — Current source acceptance

[Raw evidence](raw_inputs.json): **21/21 files, 183.728.724 bytes** khớp SHA-256/size
trong `config/local_raw_manifest.json`; 9 configured inputs có thật. Năm CSV nguồn
đã PASS preflight với `expected_columns` hiện tại; workbook/NSO parsing dùng fixtures
và evidence source contracts, không gọi binary preflight là full parser validation.

[Pinned Silver audit](silver_readonly.json): **9/9 PASS**, 66 read-only queries,
37,33 s tổng client-query time (không phải wall time/SLA). Schema/types, requiredness,
business keys, blocking contract DQ, missing và lineage checks đều PASS. Pins khớp
persisted Silver SUCCESS proofs và published Gold inputs.

| Source | Retained Silver rows |
|---|---:|
| faostat_production | 22.738 |
| faostat_monthly_price | 15.334 |
| faostat_supply_utilization | 36.380 |
| faostat_trade | 1.226.470 |
| usda_psd | 990 |
| usda_rice_yearbook | 12.936 |
| worldbank_pinksheet | 56.232 |
| thitruongnongsan | 20.387 |
| nso | 23.060 |
| **Total** | **1.414.527** |

Trade uniqueness/DQ kiểm tra đủ 39 year partitions **1986–2024**, kể cả 2017.
Year thuộc business key nên group không thể nằm ở hai partitions. Domain lấy từ
pinned data; tổng partition rows phải bằng global count, không sampling/checksum thay thế.
Đây là Silver audit, không gọi nó là chạy lại full Gold Trade semantic validator.

Bốn published consumers PASS: mỗi query trả 20 rows từ latest validated year;
NSO ~5,41 s, production ~2,10 s, SUA ~1,58 s, Trade ~2,31 s. Đó là **bounded samples**,
không chứng minh unrestricted full-history Dashboard performance. Exact definition
và bound-SQL hashes được xác minh bằng manifest/read gate.

## Test results và giới hạn

| Suite | Kết quả đã chạy |
|---|---:|
| Host unit, gồm Gold regression và job import/CLI | **489 PASS**, [JUnit](unit_tests.xml) |
| Same unit suite, approved Spark 3.5.5 Python APIs / no JVM | **490 PASS**, [pinned-environment JUnit](unit_tests_pinned.xml) |
| Spark DQ/MERGE/quarantine, disposable Hadoop | **17 PASS** |
| Gold Spark transformations + Silver FAOSTAT/NSO fixtures, isolated container | **11 PASS** (7 + 4) |
| Bounded live Trino Gold SELECT fixtures | **9 PASS** |
| Airflow DAG import/guard tests, existing image | **12 PASS** |
| Owned REST/PG recovery integration | **6 PASS**, [JUnit](owned_runtime_tests.xml) |
| Contract validator | **9/9 PASS** |

Một lượt Spark đầu bỏ qua 7 Gold fixtures do chưa bật `GOLD_SPARK_TEST`; lượt đúng
opt-in sau đó đã PASS đủ 7. Không cộng chúng thành physical Gold E2E.
Các runtime tests không thuộc default unit collection là excluded theo profile,
không phải failures được đổi thành skip.

Host ban đầu có PySpark 4.1.2; kết quả đầu tiên không được gọi là runtime Spark
3.5.5. Lượt tương thích bổ sung dùng Python archives của approved image; Spark JVM
fixtures luôn dùng image 3.5.5. Requirements dev khai báo PyArrow/lxml và pinned
test dependencies; không thay container locks/platform. Fresh-venv install chưa chạy.

Không có correctness blocker đang tái hiện trong scope nghiệm thu. Giới hạn còn lại:

- Không FULL replay hoặc failure injection trên 9 targets thật; không test mọi
  adapter/recovery combination bằng physical crash/kill. Thay đổi ingestion được
  chứng minh trên owned real runtime fixtures + unit regressions, không tự chứng nhận
  toàn bộ Bronze/Silver DagRun mới. Silver MERGE business logic không đổi.
- Atomicity chỉ per chunk; không per source/multi-table. Writer ngoài advisory-lock
  protocol không được bảo vệ. Legacy partial chunks/context và unresolved UNKNOWN_COMMIT
  cần operator readback/recovery plan; không sửa metadata trạng thái bằng tay để PASS.
- FULL Raw disappearance không xóa Silver key. Withdrawals, official geography,
  cross-provider conformance/FX/forecasting còn ngoài scope, cần business decision riêng.
- Lần resource sample cuối Windows available **0,62 GiB**, dưới gate 2 GiB. Đã dừng
  runtime work, không restart/rebuild hoặc tăng heap. Containers có healthcheck healthy;
  sampled runner/Trino cgroup OOM counters = 0 là trạng thái sample, không phải peak
  measurement cho toàn bộ tests hoặc trước các lần restart bên ngoài.

Evidence UTC timestamps trong JSON giữ nguyên. Collector v1 ở `baseline_before.json`
đã chiếu nhầm field `hash` thành NULL; không dùng field đó làm proof. Manifest hash và
code/reference fingerprints lúc đầu vẫn hợp lệ. Collector cuối ghi đúng
`definition_sha256`/`bound_sql_sha256` và consumer kiểm tra chúng.

## Next milestone

[Analytics Readiness](../analytics/ANALYTICS_READINESS.md): xây query allowlist nhỏ,
parameter validation, snapshot-bound read gate, cache identity và performance budget
trước UI. Không đổi bốn semantic definitions đã publish. Teammate tiếp tục sở hữu
portability/second-laptop acceptance; nhiệm vụ này không tiếp quản phần đó.
