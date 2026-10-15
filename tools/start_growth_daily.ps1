$ErrorActionPreference = 'Stop'
$growthRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $growthRoot
$growthState = Join-Path $growthRoot '.growth-runtime'
New-Item -ItemType Directory -Path $growthState -Force | Out-Null
$growthToday = Get-Date -Format 'yyyy-MM-dd'
$growthMarker = Join-Path $growthState 'daily-dispatch.txt'
if ((Test-Path -LiteralPath $growthMarker) -and ((Get-Content -LiteralPath $growthMarker -Raw).Trim() -eq $growthToday)) {
    exit 0
}
# Windows owns the daily trigger. The local worker creates the bounded result
# first; GitHub then validates and publishes it. No background polling is used.
& python (Join-Path $PSScriptRoot 'growth_once.py')
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Set-Content -LiteralPath $growthMarker -Value $growthToday -Encoding UTF8
