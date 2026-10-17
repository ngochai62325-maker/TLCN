param([switch]$Smoke)
. (Join-Path $PSScriptRoot '_common.ps1')
$config = Get-DemoConfig
Invoke-DemoDocker -Arguments ($script:DemoCompose + @('ps', '-a'))
Get-DemoMemory | Format-List | Out-Host
$containers = @(Get-DemoContainers | Where-Object { $_.State.Running })
if ($containers.Count) { Invoke-DemoDocker -Arguments (@('stats', '--no-stream', '--format', '{{.Name}}  {{.MemUsage}}  {{.CPUPerc}}') + @($containers | ForEach-Object { $_.Id })) }
if ($Smoke) { Test-DemoRaw $config; Assert-DemoReady }
