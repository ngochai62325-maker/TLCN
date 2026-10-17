param([switch]$SkipDownload, [string]$Image = 'tlcn/silver-spark:local')

# Compatibility entrypoint: use the single maintained network-isolated test runner.
$selected = @(
    'tests/unit/silver/test_faostat_nso_transformers.py',
    'tests/integration/test_faostat_nso_silver.py',
    'tests/unit/silver/test_silver_transformers.py',
    'tests/unit/test_silver_quality_engine.py',
    'tests/unit/test_silver_merge_engine.py'
)
& (Join-Path $PSScriptRoot 'test_silver_isolated.ps1') -SkipDownload:$SkipDownload -Image $Image -TestPaths $selected
exit $LASTEXITCODE
