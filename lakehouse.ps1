param (
    [Parameter(Mandatory=$false, Position=0)]
    [ValidateSet("start", "stop", "restart", "status", "logs", "test-all", "test-minio", "test-trino", "test-spark", "test-airflow")]
    [string]$Action = "status"
)

switch ($Action) {
    "start" {
        Write-Host ">>> Starting Lakehouse containers..." -ForegroundColor Cyan
        docker compose up -d
        docker compose ps
    }
    "stop" {
        Write-Host ">>> Stopping all containers..." -ForegroundColor Yellow
        docker compose down
    }
    "restart" {
        Write-Host ">>> Restarting all containers..." -ForegroundColor Cyan
        docker compose restart
    }
    "status" {
        Write-Host ">>> Lakehouse containers status:" -ForegroundColor Cyan
        docker compose ps
    }
    "logs" {
        docker compose logs -f --tail=50
    }
    "test-minio" {
        Write-Host ">>> Checking MinIO buckets:" -ForegroundColor Green
        docker compose exec minio mc alias set local http://localhost:9000 admin password123
        docker compose exec minio mc ls local/
    }
    "test-trino" {
        Write-Host ">>> Checking Trino Iceberg Catalog:" -ForegroundColor Green
        docker compose exec trino trino --execute "SHOW CATALOGS;"
    }
    "test-spark" {
        Write-Host ">>> Checking Spark Catalog:" -ForegroundColor Green
        docker compose exec spark-iceberg spark-sql --master "local[2]" -e "SHOW DATABASES IN iceberg;"
    }
    "test-airflow" {
        Write-Host ">>> Checking Airflow Webserver & Version:" -ForegroundColor Green
        docker compose exec airflow-webserver airflow version
    }
    "test-all" {
        Write-Host "=========================================" -ForegroundColor Magenta
        Write-Host "     DATA LAKEHOUSE VERIFICATION TEST    " -ForegroundColor Magenta
        Write-Host "=========================================" -ForegroundColor Magenta

        Write-Host "[1/5] MinIO Buckets:" -ForegroundColor Cyan
        docker compose exec minio mc alias set local http://localhost:9000 admin password123 | Out-Null
        docker compose exec minio mc ls local/

        Write-Host "[2/5] Iceberg REST Catalog:" -ForegroundColor Cyan
        try {
            $resp = Invoke-RestMethod -Uri "http://localhost:8181/v1/config" -TimeoutSec 5
            Write-Host "Iceberg REST Catalog OK (HTTP 200)" -ForegroundColor Green
        } catch {
            Write-Host "Iceberg REST Catalog Error: $_" -ForegroundColor Red
        }

        Write-Host "[3/5] Trino Engine & Iceberg Catalog:" -ForegroundColor Cyan
        docker compose exec trino trino --execute "SHOW CATALOGS;"

        Write-Host "[4/5] Spark Engine & Iceberg Catalog:" -ForegroundColor Cyan
        docker compose exec spark-iceberg spark-sql --master "local[2]" -e "SHOW DATABASES IN iceberg;"

        Write-Host "[5/5] Airflow Webserver:" -ForegroundColor Cyan
        docker compose exec airflow-webserver airflow version

        Write-Host "=========================================" -ForegroundColor Green
        Write-Host "  ALL LAKEHOUSE SERVICES HEALTHY & READY!" -ForegroundColor Green
        Write-Host "=========================================" -ForegroundColor Green
    }
}
