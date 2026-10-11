param([switch]$SkipDownload)

# Run from any directory. The shared MinIO/REST catalog is unreachable because
# the test container has no network. Only a fresh /tmp Hadoop warehouse is used.
$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$wheelDirectory = Join-Path $repositoryRoot '.pytest_cache/hoang_wheels'

if (-not $SkipDownload) {
    python -m pip download --disable-pip-version-check --only-binary=:all: --platform any --implementation py --python-version 310 --abi none --dest $wheelDirectory pytest==8.4.2 exceptiongroup tomli typing_extensions
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

$mountArgument = "type=bind,source=$repositoryRoot,target=/workspace,readonly"
$testCommand = 'python3 -m pip install --no-index --find-links /workspace/.pytest_cache/hoang_wheels --target /tmp/hoang_test_deps pytest==8.4.2 && PYTHONPATH=/tmp/hoang_test_deps:/workspace/src:$PYTHONPATH python3 -m pytest tests/unit/silver/test_faostat_nso_transformers.py tests/integration/test_faostat_nso_silver.py tests/unit/silver/test_silver_transformers.py tests/unit/test_silver_quality_engine.py tests/unit/test_silver_merge_engine.py -q -p no:cacheprovider'

docker run --rm --network none --hostname localhost --mount $mountArgument --workdir /workspace -e SPARK_LOCAL_IP=127.0.0.1 -e SPARK_LOCAL_HOSTNAME=localhost -e HOANG_ISOLATED_ICEBERG_TESTS=1 -e 'PYSPARK_SUBMIT_ARGS=--conf spark.sql.defaultCatalog=spark_catalog pyspark-shell' --entrypoint /bin/bash tabulario/spark-iceberg:latest -c $testCommand
exit $LASTEXITCODE
