param([switch]$Manual, [switch]$DryRun)

$ErrorActionPreference = 'Stop'
if (-not $Manual) {
  Write-Error 'El sondeo automatico esta retirado. Usa start_growth_daily.ps1 o llama con -Manual.'
  exit 2
}
$growthRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $growthRoot
$arguments = @((Join-Path $PSScriptRoot 'growth_once.py'))
if ($DryRun) { $arguments += '--dry-run' }
& python @arguments
exit $LASTEXITCODE
