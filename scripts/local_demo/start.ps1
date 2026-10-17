param([switch]$CheckOnly)
. (Join-Path $PSScriptRoot '_common.ps1')
Set-Location $script:DemoRoot
$config = Get-DemoConfig
Test-DemoRaw $config
$memory = Get-DemoMemory
$memory | Format-List | Out-Host
if ($CheckOnly) {
    Write-Host 'Config/Raw check only. No image build, container start, deletion or pipeline trigger.'
    return
}
Assert-NoCompetingStacks
if ($memory.WindowsAvailableGiB -lt 2 -or $memory.WslAvailableGiB -lt 4) {
    throw 'Insufficient RAM before build/start: require Windows >=2GiB, WSL >=4GiB. No limits were raised.'
}
$existing = @(Get-DemoContainers)
if ($existing.Count) {
    # Subsequent openings preserve the initializer and all existing container IDs.
    $postgresId = $null
    foreach ($service in @('postgres', 'minio', 'iceberg-rest', 'trino', 'runner', 'airflow-scheduler', 'airflow-webserver')) {
        $container = @($existing | Where-Object { $_.Config.Labels.'com.docker.compose.service' -eq $service })
        if ($container.Count -ne 1) { throw "Incomplete existing demo: $service. Reconcile it; no automatic recreate." }
        if ($service -eq 'airflow-scheduler') { Assert-DemoPersistedPause $postgresId }
        if (-not $container[0].State.Running) { Invoke-DemoDocker -Arguments @('start', $container[0].Id) }
        Wait-DemoContainer $container[0].Id
        if ($service -eq 'postgres') { $postgresId = $container[0].Id }
    }
    Assert-DemoReady
} else {
    foreach ($v in $config.volumes.PSObject.Properties.Value) {
        $found = @(& docker volume ls --filter "name=^$($v.name)$" --format '{{.Name}}')
        if ($LASTEXITCODE -ne 0 -or $found.Count) { throw 'Existing/orphaned demo volume requires reconciliation before a fresh initializer.' }
    }
    Invoke-DemoDocker -Arguments ($script:DemoCompose + @('build', 'runner', 'airflow-webserver'))
    Invoke-DemoDocker -Arguments ($script:DemoCompose + @('up', '-d', '--wait', '--wait-timeout', '120', 'postgres', 'minio', 'iceberg-rest'))
    Invoke-DemoDocker -Arguments ($script:DemoCompose + @('up', '--no-deps', '--exit-code-from', 'airflow-init', 'airflow-init'))
    Invoke-DemoDocker -Arguments ($script:DemoCompose + @('up', '-d', '--wait', '--wait-timeout', '180', '--no-deps', 'trino', 'runner'))
    Invoke-DemoDocker -Arguments ($script:DemoCompose + @('up', '-d', '--wait', '--wait-timeout', '180', '--no-deps', 'airflow-scheduler', 'airflow-webserver'))
    Assert-DemoReady -Fresh
}
Write-Host 'Open Airflow UI: http://localhost:8085 | MinIO Console: http://localhost:9001 | Trino: http://localhost:18080 . DAGs remain paused.'
