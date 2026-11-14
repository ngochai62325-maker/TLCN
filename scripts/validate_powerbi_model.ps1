param([string]$Definition = (Join-Path $PSScriptRoot '..\powerbi\M\definition'))
$ErrorActionPreference = 'Stop'

$installLocation = $null
try {
    $pbiPackage = Get-AppxPackage 'Microsoft.MicrosoftPowerBIDesktop' -ErrorAction Stop | Select-Object -First 1
    if ($pbiPackage -and $pbiPackage.InstallLocation) {
        $installLocation = $pbiPackage.InstallLocation
    }
} catch {
    # Get-AppxPackage can throw UnauthorizedAccessException in sandboxed or non-admin PowerShell sessions
}

if (-not $installLocation) {
    try {
        $regKey = Get-ItemProperty 'HKCU:\Software\Classes\Local Settings\Software\Microsoft\Windows\CurrentVersion\AppModel\Repository\Packages\*MicrosoftPowerBIDesktop*' -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($regKey -and $regKey.PackageRootFolder) {
            $installLocation = $regKey.PackageRootFolder
        }
    } catch {}
}

if (-not $installLocation) {
    # Check standard 64-bit standalone installer directory
    $standalone = 'C:\Program Files\Microsoft Power BI Desktop'
    if (Test-Path (Join-Path $standalone 'bin\Microsoft.PowerBI.Tabular.dll')) {
        $installLocation = $standalone
    }
}

if (-not $installLocation -or -not (Test-Path (Join-Path $installLocation 'bin\Microsoft.PowerBI.Tabular.dll'))) {
    throw 'Power BI Desktop Store or Desktop installation not found; use its installed TOM library explicitly.'
}

$pbiLibrary = Join-Path $installLocation 'bin\Microsoft.PowerBI.Tabular.dll'
[Reflection.Assembly]::LoadFrom($pbiLibrary) | Out-Null
$parsedModel = [Microsoft.AnalysisServices.Tabular.TmdlSerializer]::DeserializeDatabaseFromFolder((Resolve-Path -LiteralPath $Definition).Path)
[pscustomobject]@{Status='TMDL_PARSE_PASS';Tables=$parsedModel.Model.Tables.Count;Relationships=$parsedModel.Model.Relationships.Count;Measures=($parsedModel.Model.Tables | ForEach-Object {$_.Measures.Count} | Measure-Object -Sum).Sum;DAXEngineExecution='NOT_PROVEN'} | ConvertTo-Json
