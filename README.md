# TLCN Rice Lakehouse — Local Development Stack (`tlcn-local`)

Đồ án Data Engineering chạy local trên một Docker Compose duy nhất ([`compose.yaml`](compose.yaml)) với **Airflow**, **Apache Spark**, **Apache Iceberg (REST Catalog)**, **MinIO**, **PostgreSQL** và **Trino**.
Profile `local_nine` tích hợp **9 nguồn dữ liệu thực tế** từ Raw → Bronze (`iceberg.bronze.*`) → Silver (`iceberg.silver_local.*`).

Gold MVP gồm **8 physical dimensions, 9 physical facts và 4 version-controlled semantic SQL queries**, DAG manual `rice_lakehouse_gold_pipeline`. Giữ Trino 448/REST hiện hữu, không tạo catalog views. Xem [hướng dẫn chạy/demo](docs/gold/IMPLEMENTATION.md) và [trạng thái nghiệm thu thực tế](docs/gold/VALIDATION.md).

**Gold G12: GOLD_MVP_COMPLETE.** Hai Gold DagRuns SUCCESS; đủ 17 tables, 9-source reconciliation, DQ01–18, bốn semantic queries, publication/consumer và formal physical NOOP 17/17 tables PASS. Giữ Trino 448/512 MiB. Chi tiết và giới hạn vận hành tại [Gold validation](docs/gold/VALIDATION.md).

## Analytics và báo cáo local — 27/10/2026

**Streamlit Dashboard đã ngừng vĩnh viễn. Power BI Desktop Free là công cụ reporting hiện hành.** Bronze/Silver/Gold và sáu notebooks EDA được giữ; Python chỉ xuất CSV từ một published Gold batch qua Analytics allowlist/read gate. Không cần Power BI Pro hoặc cloud publication để tự dựng/lưu report local.

```powershell
python -m jobs.analytics.export_powerbi
```

Lấy đường dẫn bundle được CLI báo thành công dưới `exports/powerbi/`, import bốn CSV cùng bundle vào Desktop. Report `.pbix` do owner tạo thủ công; dữ liệu là extract, không tự live refresh với Gold. [Exporter/schema](docs/powerbi/README.md) · [Hướng dẫn kéo thả + DAX](docs/powerbi/POWER_BI_DESKTOP_GUIDE.md) · [EDA findings lịch sử](docs/analytics/EDA_FINDINGS.md) · [Archive UI đã ngừng](docs/dashboard/archive/README.md).

## 1. Dịch vụ & Cổng Localhost

| Dịch vụ | URL / Endpoint | Tài khoản mặc định (`.env.example`) | Ghi chú |
|---|---|---|---|
| **Airflow UI** | `http://localhost:8085` | `AIRFLOW_ADMIN_USER=admin` / `AIRFLOW_ADMIN_PASSWORD=admin` | Bronze, Silver và Gold DAG; Gold manual và có admission gate |
| **MinIO Console** | `http://localhost:9001` | `MINIO_ROOT_USER=minioadmin` / `MINIO_ROOT_PASSWORD=minioadmin123` | Bucket `warehouse`: Iceberg và Gold publication metadata; Raw giữ local |
| **Trino Coordinator** | `http://localhost:18080` | Không mật khẩu (local header `X-Trino-User`) | Truy vấn SQL trực tiếp catalog `iceberg` |
| **PostgreSQL / Iceberg REST / Spark API** | Internal Docker network (`postgres:5432`, `iceberg-rest:8181`, `runner:5005`) | Cấu hình trong `.env` | Chỉ giao tiếp nội bộ trong bridge network của project `tlcn-local` |

> Sao chép [`.env.example`](.env.example) sang `.env` nếu muốn tùy chỉnh tài khoản hoặc cổng. File `.env` đã được `.gitignore` bảo vệ để không commit API keys cá nhân.

## 2. Vòng đời vận hành (Persistent Mode & Controlled Reset)

Tất cả thao tác vận hành đi qua bộ script PowerShell tại [`scripts/local_demo/`](scripts/local_demo/):

```powershell
# Kiểm tra cấu hình compose.yaml và 21 file Raw + nso/metadata.txt (không bật container):
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/start.ps1 -CheckOnly

# Khởi động stack lần đầu hoặc mở lại stack hiện hữu (giữ nguyên dữ liệu & lịch sử):
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/start.ps1

# Kiểm tra trạng thái container, RAM và readiness smoke check:
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/status.ps1 -Smoke

# Dừng stack an toàn (kiểm tra idle, pause DAGs, gửi tín hiệu dừng êm, giữ nguyên volumes):
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/stop.ps1

# Xem kế hoạch reset (không xóa dữ liệu):
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/reset.ps1

# Xóa toàn bộ dữ liệu local của project tlcn-local để chạy lại từ đầu (yêu cầu gõ DELETE tlcn-local):
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/reset.ps1 -DeleteData
```

## 3. Tài liệu chi tiết

- [Hướng dẫn trình diễn & chạy E2E từ đầu](docs/silver/demo_guide.md)
- [Tổng quan tầng Silver & 9 nguồn dữ liệu](docs/silver/README.md)
- [Kiến trúc hệ thống & cơ chế bảo toàn dữ liệu](docs/silver/architecture.md)
- [Data Catalog: 9 nguồn, số dòng chuẩn, business keys & đơn vị](docs/silver/data_catalog.md)
- [Quy tắc làm sạch & chuẩn hóa dữ liệu (Cleaning Rules)](docs/silver/cleaning_rules.md)
- [Đặc tả Schema Bronze → Silver & Quarantine](docs/silver/schemas.md)
