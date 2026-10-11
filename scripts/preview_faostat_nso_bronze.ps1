param(
    [ValidateSet('all', 'faostat_production', 'faostat_monthly_price', 'faostat_supply_utilization', 'nso_vietnam')]
    [string]$Dataset = 'all',
    [string]$Network = 'tlcn_lakehouse-net'
)

# SELECT and DataFrame evaluation only. No table/namespace or quarantine writes.
$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$mountArgument = "type=bind,source=$repositoryRoot,target=/workspace,readonly"
$previewCommand = 'PYTHONPATH=/workspace/src:$PYTHONPATH python3 jobs/silver/preview_faostat_nso.py --dataset "$HOANG_PREVIEW_DATASET"'

docker run --rm --network $Network --mount $mountArgument --workdir /workspace -e AWS_REGION=us-east-1 -e "HOANG_PREVIEW_DATASET=$Dataset" --entrypoint /bin/bash tabulario/spark-iceberg:latest -c $previewCommand
exit $LASTEXITCODE
