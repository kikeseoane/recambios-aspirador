param([switch]$Retry)

$ErrorActionPreference = 'Stop'
$growthRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $growthRoot
$growthWindsorKey = [Environment]::GetEnvironmentVariable('GROWTH_WINDSOR_API_KEY', 'User')
if ($growthWindsorKey) { $env:GROWTH_WINDSOR_API_KEY = $growthWindsorKey }
$growthState = Join-Path $growthRoot '.growth-runtime'
New-Item -ItemType Directory -Path $growthState -Force | Out-Null
$growthToday = Get-Date -Format 'yyyy-MM-dd'
$growthMarker = Join-Path $growthState 'daily-dispatch.txt'
if ((Test-Path -LiteralPath $growthMarker) -and ((Get-Content -LiteralPath $growthMarker -Raw).Trim() -eq $growthToday)) {
    exit 0
}
$retryRequest = Join-Path $growthState 'retry-request.json'
Remove-Item -LiteralPath $retryRequest -Force -ErrorAction SilentlyContinue
# Windows owns the daily trigger. The local worker creates the bounded result
# first; GitHub then validates and publishes it. No background polling is used.
& python (Join-Path $PSScriptRoot 'growth_once.py') --schedule-five-hour-retry
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if (Test-Path -LiteralPath $retryRequest) {
    $request = Get-Content -LiteralPath $retryRequest -Raw | ConvertFrom-Json
    & (Join-Path $PSScriptRoot 'register_growth_retry.ps1') -AtUtc $request.at_utc
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    exit 0
}
Set-Content -LiteralPath $growthMarker -Value $growthToday -Encoding UTF8
