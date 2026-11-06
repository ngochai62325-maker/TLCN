param([string]$Definition = (Join-Path $PSScriptRoot '..\powerbi\M\definition'))
$ErrorActionPreference = 'Stop'
$pbiPackage = Get-AppxPackage 'Microsoft.MicrosoftPowerBIDesktop' | Select-Object -First 1
if (-not $pbiPackage) { throw 'Power BI Desktop Store installation not found; use its installed TOM library explicitly.' }
$pbiLibrary = Join-Path $pbiPackage.InstallLocation 'bin\Microsoft.PowerBI.Tabular.dll'
[Reflection.Assembly]::LoadFrom($pbiLibrary) | Out-Null
$parsedModel = [Microsoft.AnalysisServices.Tabular.TmdlSerializer]::DeserializeDatabaseFromFolder((Resolve-Path -LiteralPath $Definition).Path)
[pscustomobject]@{Status='TMDL_PARSE_PASS';Tables=$parsedModel.Model.Tables.Count;Relationships=$parsedModel.Model.Relationships.Count;Measures=($parsedModel.Model.Tables | ForEach-Object {$_.Measures.Count} | Measure-Object -Sum).Sum;DAXEngineExecution='NOT_PROVEN'} | ConvertTo-Json
