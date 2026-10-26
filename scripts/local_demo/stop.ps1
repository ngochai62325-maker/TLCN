. (Join-Path $PSScriptRoot '_common.ps1')
$containers = @(Get-DemoContainers)
if (-not @($containers | Where-Object { $_.State.Running }).Count) { Write-Host 'Demo is already stopped; volumes preserved.'; return }
Assert-DemoIdle $containers
$scheduler = @($containers | Where-Object { $_.Config.Labels.'com.docker.compose.service' -eq 'airflow-scheduler' })[0]
foreach ($dag in @('rice_lakehouse_gold_pipeline', 'rice_lakehouse_silver_pipeline', 'rice_lakehouse_ingestion')) {
    Invoke-DemoDocker -Arguments @('exec', $scheduler.Id, 'airflow', 'dags', 'pause', $dag)
}
Assert-DemoIdle $containers
foreach ($service in @('airflow-scheduler', 'airflow-webserver', 'runner', 'trino', 'iceberg-rest', 'minio', 'postgres')) {
    $c = @($containers | Where-Object { $_.Config.Labels.'com.docker.compose.service' -eq $service })[0]
    if (-not $c.State.Running) { continue }
    $signal = 'SIGTERM'; if ($service -eq 'runner') { $signal = 'SIGINT' }
    Invoke-DemoDocker -Arguments @('kill', "--signal=$signal", $c.Id)
    $deadline = (Get-Date).AddSeconds(60)
    do {
        Start-Sleep -Seconds 2
        $state = Invoke-DemoDocker -Arguments @('inspect', '--format', '{{.State.Running}}', $c.Id) -Capture
    } while ($state.Trim() -eq 'true' -and (Get-Date) -lt $deadline)
    if ($state.Trim() -eq 'true') { throw "$service did not exit; STOP. No second signal/forced kill was sent." }
}
Write-Host 'Demo stopped gracefully. Containers, volumes, Raw and Airflow history preserved.'
