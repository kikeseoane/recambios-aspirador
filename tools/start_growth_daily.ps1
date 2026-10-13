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
# Windows owns the daily trigger; GitHub still validates and publishes.
& gh workflow run codex-growth.yml --repo kikeseoane/recambios-aspirador --ref main -f dry_run=false
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Set-Content -LiteralPath $growthMarker -Value $growthToday -Encoding UTF8
