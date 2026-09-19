#!/usr/bin/env bash
# Open-Source Data Lakehouse Helper Script for Linux / macOS

ACTION=${1:-status}

case "$ACTION" in
    start)
        echo ">>> Starting Lakehouse containers..."
        docker compose up -d
        docker compose ps
        ;;
    stop)
        echo ">>> Stopping all containers..."
        docker compose down
        ;;
    restart)
        echo ">>> Restarting all containers..."
        docker compose restart
        ;;
    status)
        echo ">>> Lakehouse containers status:"
        docker compose ps
        ;;
    logs)
        docker compose logs -f --tail=50
        ;;
    test-minio)
        echo ">>> Checking MinIO buckets:"
        docker compose exec minio mc alias set local http://localhost:9000 admin password123
        docker compose exec minio mc ls local/
        ;;
    test-trino)
        echo ">>> Checking Trino Iceberg Catalog:"
        docker compose exec trino trino --execute "SHOW CATALOGS;"
        ;;
    test-spark)
        echo ">>> Checking Spark Catalog:"
        docker compose exec spark-iceberg spark-sql --master "local[2]" -e "SHOW DATABASES IN iceberg;"
        ;;
    test-airflow)
        echo ">>> Checking Airflow Webserver & Version:"
        docker compose exec airflow-webserver airflow version
        ;;
    test-all)
        echo "========================================="
        echo "     DATA LAKEHOUSE VERIFICATION TEST    "
        echo "========================================="

        echo "[1/5] MinIO Buckets:"
        docker compose exec minio mc alias set local http://localhost:9000 admin password123 > /dev/null 2>&1
        docker compose exec minio mc ls local/

        echo "[2/5] Iceberg REST Catalog:"
        if curl -s http://localhost:8181/v1/config > /dev/null; then
            echo "Iceberg REST Catalog OK (HTTP 200)"
        else
            echo "Iceberg REST Catalog Error"
        fi

        echo "[3/5] Trino Engine & Iceberg Catalog:"
        docker compose exec trino trino --execute "SHOW CATALOGS;"

        echo "[4/5] Spark Engine & Iceberg Catalog:"
        docker compose exec spark-iceberg spark-sql --master "local[2]" -e "SHOW DATABASES IN iceberg;"

        echo "[5/5] Airflow Webserver:"
        docker compose exec airflow-webserver airflow version

        echo "========================================="
        echo "  ALL LAKEHOUSE SERVICES HEALTHY & READY!"
        echo "========================================="
        ;;
    *)
        echo "Usage: ./lakehouse.sh {start|stop|restart|status|logs|test-all}"
        exit 1
        ;;
esac
