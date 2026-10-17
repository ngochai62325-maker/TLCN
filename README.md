# TLCN Rice Lakehouse — local Silver

Đồ án Data Engineering chạy local với Docker, Airflow, Spark, Apache Iceberg,
MinIO, PostgreSQL và Trino. Profile `local_nine` tích hợp9 nguồn Bronze → Silver;
namespace local là `iceberg.silver_local`.

- [Quickstart và handoff laptop mới](docs/silver/local_runbook.md)
- [Trình diễn: một local stack, Airflow localhost:8085](docs/silver/demo_guide.md)
- [Kiến trúc và tài liệu Silver](docs/silver/README.md)
- [Chín nguồn / schema / units / lineage](docs/silver/data_catalog.md)
- [Validation và evidence](docs/silver/validation.md)

Cần repository cùng bộ21 Raw files riêng biệt, thêm NSO metadata theo
[manifest](config/local_raw_manifest.json). Full Trade CSV không đưa vào Git.
Dùng đúng Compose local trong runbook; `docker-compose.yml` là cấu hình shared
legacy, không phải lệnh startup mặc định cho laptop mới.

Hai DAG manual-only; không Gold, production scheduling hay cloud deployment.
NSO production contract giữ DRAFT; local profile có overlay riêng. Không xóa
volumes/snapshots hoặc force reprocess để xử lý một writer chưa rõ outcome.
