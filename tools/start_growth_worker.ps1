$ErrorActionPreference = 'Stop'
$growthRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $growthRoot
& python (Join-Path $PSScriptRoot 'growth_worker.py')
exit $LASTEXITCODE
