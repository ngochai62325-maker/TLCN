param(
    [switch]$SkipDownload,
    [string]$Image = 'tlcn/silver-spark:local',
    [string[]]$TestPaths = @(
        'tests/unit/test_silver_contract_loader.py',
        'tests/unit/test_silver_contract_paths.py',
        'tests/unit/test_silver_quality_engine.py',
        'tests/unit/test_silver_merge_engine.py',
        'tests/unit/test_silver_quarantine.py',
        'tests/unit/test_silver_orchestration.py',
        'tests/unit/test_nso_union_schema.py',
        'tests/unit/test_bronze_schema_preservation.py',
        'tests/unit/silver',
        'tests/integration/test_faostat_nso_silver.py',
        'tests/integration/test_silver_pipeline_isolated.py',
        'tests/integration/test_local_nine_native.py',
        'tests/integration/test_nso_v2_read_context.py'
    )
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$wheelDirectory = Join-Path $repositoryRoot '.pytest_cache/silver_wheels'
if (-not $SkipDownload) {
    python -m pip download --disable-pip-version-check --only-binary=:all: --platform any --implementation py --python-version 310 --abi none --dest $wheelDirectory pytest==8.4.2 boto3==1.35.99 exceptiongroup tomli typing_extensions
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

$mountArgument = "type=bind,source=$repositoryRoot,target=/workspace,readonly"
# Only repository-relative test paths, never arbitrary shell fragments.
foreach ($testPath in $TestPaths) {
    if ($testPath -notmatch '^tests/[A-Za-z0-9_./-]+(?:::[A-Za-z0-9_]+)*$' -or $testPath.Contains('..')) {
        throw "Unsafe test path: $testPath"
    }
}
$selectedTests = $TestPaths -join ' '
$testCommand = 'python3 -m pip install --no-index --find-links /workspace/.pytest_cache/silver_wheels --target /tmp/silver_test_deps pytest==8.4.2 boto3==1.35.99 && export PYTHONPATH=/tmp/silver_test_deps:/workspace/src:/workspace/jobs/silver:/workspace:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip && python3 -m pytest ' + $selectedTests + ' --with-runtime -q -p no:cacheprovider --basetemp=/tmp/silver-test-pytest'


# Read-only bind, no network, ephemeral local warehouse. Existing Docker
# services and their catalogs are never used by this suite.
docker run --rm --pull never --network none --hostname localhost --memory 3g --memory-swap 3g --cpus 2 --mount $mountArgument --workdir /workspace -e SPARK_LOCAL_IP=127.0.0.1 -e SPARK_LOCAL_HOSTNAME=localhost -e HOANG_ISOLATED_ICEBERG_TESTS=1 -e GOLD_SPARK_TEST=1 -e SILVER_LOCAL_PROFILE= -e SILVER_LOCAL_ENABLED=0 -e 'PYSPARK_SUBMIT_ARGS=--master local[2] --driver-memory 768m --conf spark.ui.enabled=false --conf spark.sql.defaultCatalog=spark_catalog --conf spark.sql.catalog.iceberg.type=hadoop --conf spark.sql.catalog.iceberg.warehouse=/tmp/silver-test-warehouse pyspark-shell' --entrypoint /bin/bash $Image -c $testCommand
exit $LASTEXITCODE
