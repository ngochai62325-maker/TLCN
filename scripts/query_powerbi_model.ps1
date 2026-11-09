param([Parameter(Mandatory=$true)][ValidateRange(1,65535)][int]$Port,
      [Parameter(Mandatory=$true)][string]$QueryFile,[string]$OutputPath)
$ErrorActionPreference='Stop'
$queryText=Get-Content -Raw -Encoding utf8 -LiteralPath $QueryFile
if ($queryText -notmatch '^\s*(EVALUATE|SELECT)' -or $queryText -match '\b(ALTER|CREATE|DELETE|DROP|UPDATE|INSERT)\b') { throw 'Only read-only DAX/DMV query files are accepted' }
$pbiPackage=Get-AppxPackage 'Microsoft.MicrosoftPowerBIDesktop' | Select-Object -First 1
[Reflection.Assembly]::LoadFrom((Join-Path $pbiPackage.InstallLocation 'bin\Microsoft.PowerBI.AdomdClient.dll')) | Out-Null
$connection=New-Object Microsoft.AnalysisServices.AdomdClient.AdomdConnection("Data Source=localhost:$Port")
try {
    $connection.Open();$command=$connection.CreateCommand();$command.CommandText=$queryText;$command.CommandTimeout=30
    $started=[Diagnostics.Stopwatch]::StartNew();$reader=$command.ExecuteReader();$rows=@()
    while ($reader.Read()) {
        $row=[ordered]@{}
        for ($i=0;$i -lt $reader.FieldCount;$i++) { $row[$reader.GetName($i)]=if ($reader.IsDBNull($i)) { $null } else { $reader.GetValue($i) } }
        $rows += $row
        if ($rows.Count -gt 5000) { throw 'DAX validation output limit exceeded' }
    }
    $reader.Close();$started.Stop()
    $result=@{Status='EXECUTED_IN_DESKTOP_ENGINE';Milliseconds=$started.ElapsedMilliseconds;Rows=$rows}|ConvertTo-Json -Depth 8
    if ($OutputPath) { [IO.File]::WriteAllText($OutputPath,$result,(New-Object Text.UTF8Encoding($false))) }
    $result
} finally { $connection.Dispose() }
