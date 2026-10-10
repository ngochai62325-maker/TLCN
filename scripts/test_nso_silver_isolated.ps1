param([switch]$SkipDownload)

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$wheelDirectory = Join-Path $repositoryRoot '.pytest_cache/hoang_wheels'
$parquetPath = Join-Path $repositoryRoot '.pytest_cache/nso_e1_bronze.parquet'
if (-not (Test-Path -LiteralPath $parquetPath)) {
    throw 'Missing pinned NSO export. Run the read-only profile_nso_contract.py job and copy its Parquet/profile evidence first.'
}
if (-not $SkipDownload) {
    python -m pip download --disable-pip-version-check --only-binary=:all: --platform any --implementation py --python-version 310 --abi none --dest $wheelDirectory pytest==8.4.2 exceptiongroup tomli typing_extensions
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
$mountArgument = "type=bind,source=$repositoryRoot,target=/workspace,readonly"
$testCommand = 'python3 -m pip install --no-index --find-links /workspace/.pytest_cache/hoang_wheels --target /tmp/nso_test_deps pytest==8.4.2 && PYTHONPATH=/tmp/nso_test_deps:/workspace/src:$PYTHONPATH python3 -m pytest tests/unit/silver/test_nso_contract.py tests/integration/test_nso_silver_contract.py -q -p no:cacheprovider'
docker run --rm --network none --hostname localhost --cpus 2 --memory 4g --mount $mountArgument --workdir /workspace -e SPARK_LOCAL_IP=127.0.0.1 -e SPARK_LOCAL_HOSTNAME=localhost -e HOANG_ISOLATED_ICEBERG_TESTS=1 -e 'PYSPARK_SUBMIT_ARGS=--driver-memory 2g --conf spark.sql.defaultCatalog=spark_catalog pyspark-shell' --entrypoint /bin/bash tabulario/spark-iceberg:latest -c $testCommand
exit $LASTEXITCODE
