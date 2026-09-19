# Open-Source Data Lakehouse (Medallion Architecture)

Hệ sinh thái Data Lakehouse mã nguồn mở chạy hoàn chỉnh trên Docker Compose với kiến trúc Medallion (Bronze, Silver, Gold).

## 1. Tech Stack
- **Lưu trữ Đối tượng (S3 Storage)**: [MinIO](https://min.io/) với các layers `bronze`, `silver`, `gold`, `warehouse`.
- **Định dạng Bảng & Metadata (Table Format & Catalog)**: [Apache Iceberg](https://iceberg.apache.org/) cùng Iceberg REST Catalog.
- **Engine Xử lý & Biến đổi (Processing Engine)**: [Apache Spark 3.5](https://spark.apache.org/) (hỗ trợ PySpark & Jupyter Lab).
- **Engine Truy vấn Phân tích (Query Engine)**: [Trino](https://trino.io/) kết nối trực tiếp catalog Iceberg.
- **Điều phối Luồng công việc (Orchestration)**: [Apache Airflow 2.10](https://airflow.apache.org/) cùng cơ sở dữ liệu PostgreSQL.

---

## 2. Cổng truy cập Web & Thông tin Đăng nhập

| Dịch vụ | URL | Thông tin đăng nhập |
| :--- | :--- | :--- |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `admin` / `password123` |
| **Trino Web UI** | [http://localhost:8080](http://localhost:8080) | Username: `trino` (không cần mật khẩu) |
| **Airflow Web UI** | [http://localhost:8085](http://localhost:8085) | `airflow` / `airflow` |
| **Spark Master UI** | [http://localhost:8081](http://localhost:8081) | Không yêu cầu |
| **Jupyter Lab (Spark)** | [http://localhost:8888](http://localhost:8888) | Không yêu cầu |
| **Iceberg REST API** | [http://localhost:8181/v1/config](http://localhost:8181/v1/config) | REST endpoint cho Spark & Trino |

---

## 3. Lệnh Quản lý Hệ thống (PowerShell)

Sử dụng script `lakehouse.ps1` để quản lý nhanh:

```powershell
# Kiểm tra trạng thái toàn bộ containers
powershell -ExecutionPolicy Bypass -File .\lakehouse.ps1 status

# Chạy test kiểm tra toàn diện tất cả các kết nối
powershell -ExecutionPolicy Bypass -File .\lakehouse.ps1 test-all

# Dừng tất cả containers
powershell -ExecutionPolicy Bypass -File .\lakehouse.ps1 stop

# Khởi động lại toàn bộ containers
powershell -ExecutionPolicy Bypass -File .\lakehouse.ps1 start

# Xem log các dịch vụ
powershell -ExecutionPolicy Bypass -File .\lakehouse.ps1 logs
```

---

## 4. Cấu trúc Thư mục Dự án

```
TLCN/
├── .env                                # Biến môi trường hệ thống
├── docker-compose.yml                  # Cấu hình toàn bộ containers
├── lakehouse.ps1                       # Script tiện ích quản lý và test
├── README.md                           # Tài liệu hướng dẫn sử dụng
├── config/
│   ├── trino/                          # Cấu hình Trino và Catalog Iceberg
│   │   └── etc/catalog/iceberg.properties
│   └── spark/                          # Cấu hình Spark Iceberg & S3A
│       └── spark-defaults.conf
├── dags/                               # Nơi đặt các file DAG của Airflow
├── data/                               # Dữ liệu nguồn (CSV)
├── jobs/                               # Thư mục mã nguồn Spark jobs (.py)
├── notebooks/                          # Thư mục lưu Jupyter notebooks
├── plugins/                            # Plugins cho Airflow
└── logs/                               # Logs của Airflow
```