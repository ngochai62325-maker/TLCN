param([string]$Definition = (Join-Path $PSScriptRoot 'powerbi\M\definition'))
& (Join-Path $PSScriptRoot 'scripts\validate_powerbi_model.ps1') -Definition $Definition
