param([switch]$SkipDownload)

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$wheelDirectory = Join-Path $repositoryRoot '.pytest_cache/silver_wheels'
if (-not $SkipDownload) {
    python -m pip download --disable-pip-version-check --only-binary=:all: --platform any --implementation py --python-version 310 --abi none --dest $wheelDirectory pytest==8.4.2 exceptiongroup tomli typing_extensions
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

$mountArgument = "type=bind,source=$repositoryRoot,target=/workspace,readonly"
$testCommand = 'python3 -m pip install --no-index --find-links /workspace/.pytest_cache/silver_wheels --target /tmp/silver_test_deps pytest==8.4.2 && export PYTHONPATH=/tmp/silver_test_deps:/workspace/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip && python3 -m pytest tests/unit/test_silver_contract_loader.py tests/unit/test_silver_contract_paths.py tests/unit/test_silver_quality_engine.py tests/unit/test_silver_merge_engine.py tests/unit/test_silver_quarantine.py tests/unit/test_silver_orchestration.py tests/unit/test_nso_union_schema.py tests/unit/test_bronze_schema_preservation.py tests/unit/silver tests/integration/test_faostat_nso_silver.py tests/integration/test_silver_pipeline_isolated.py -q -p no:cacheprovider'

# Read-only bind, no network, ephemeral local warehouse. Existing Docker
# services and their catalogs are never used by this suite.
docker run --rm --network none --hostname localhost --mount $mountArgument --workdir /workspace -e SPARK_LOCAL_IP=127.0.0.1 -e SPARK_LOCAL_HOSTNAME=localhost -e HOANG_ISOLATED_ICEBERG_TESTS=1 -e 'PYSPARK_SUBMIT_ARGS=--conf spark.sql.defaultCatalog=spark_catalog --conf spark.sql.catalog.iceberg.type=hadoop --conf spark.sql.catalog.iceberg.warehouse=/tmp/silver-test-warehouse pyspark-shell' --entrypoint /bin/bash tabulario/spark-iceberg:latest -c $testCommand
exit $LASTEXITCODE
