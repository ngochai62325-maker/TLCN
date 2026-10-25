param([switch]$DeleteData)
. (Join-Path $PSScriptRoot '_common.ps1')
$config = Get-DemoConfig
Test-DemoRaw $config
$containers = @(Get-DemoContainers)
$volumes = @(& docker volume ls --filter "label=com.docker.compose.project=$script:DemoProject" --format '{{.Name}}')
if ($LASTEXITCODE -ne 0) { throw 'Cannot list project volumes.' }
Write-Host 'IRREVERSIBLE: demo Airflow users/runs/logs, ingestion metadata, Iceberg catalog, Bronze/Silver objects will be lost.'
Write-Host 'Only project tlcn-local is eligible. Raw binds, Git evidence and images are retained.'
$containers | Select-Object Id, Name | Format-Table | Out-Host
$volumes | Out-Host
if (-not $DeleteData) { Write-Host 'PLAN ONLY. Separate confirmation: reset.ps1 -DeleteData'; return }
if ((Read-Host 'Type DELETE tlcn-local to confirm permanent loss') -cne 'DELETE tlcn-local') { throw 'Reset cancelled.' }
# Require a live read-only idle proof first, then the normal graceful stop.
Assert-DemoIdle $containers
& (Join-Path $PSScriptRoot 'stop.ps1')
if (@(Get-DemoContainers | Where-Object { $_.State.Running }).Count) { throw 'A container is still running.' }
# Refuse volumes shared with another project. No global prune or force option.
foreach ($volume in $volumes) {
    $users = @(& docker ps -aq --filter "volume=$volume")
    if ($LASTEXITCODE -ne 0) { throw 'Cannot verify volume references.' }
    foreach ($id in $users) {
        $c = Invoke-DemoDocker -Arguments @('inspect', $id) -Capture | ConvertFrom-Json
        if ($c.Config.Labels.'com.docker.compose.project' -ne $script:DemoProject) { throw 'Volume referenced outside demo; reset refused.' }
    }
}
foreach ($c in $containers) { Invoke-DemoDocker -Arguments @('rm', $c.Id) }
foreach ($volume in $volumes) { Invoke-DemoDocker -Arguments @('volume', 'rm', $volume) }
$networks = @(& docker network ls --filter "label=com.docker.compose.project=$script:DemoProject" --format '{{.ID}}')
if ($LASTEXITCODE -ne 0) { throw 'Cannot list project networks.' }
foreach ($network in $networks) { Invoke-DemoDocker -Arguments @('network', 'rm', $network) }
Write-Host 'Only the confirmed demo resources were removed. Run start.ps1 for a fresh empty project.'
