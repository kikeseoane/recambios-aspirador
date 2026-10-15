param([Parameter(Mandatory = $true)][string]$AtUtc)

$ErrorActionPreference = 'Stop'
$when = [DateTimeOffset]::Parse($AtUtc).ToLocalTime().DateTime
if ($when -le (Get-Date).AddSeconds(20)) { throw 'El reintento debe ser futuro.' }
$root = Split-Path -Parent $PSScriptRoot
$daily = Join-Path $PSScriptRoot 'start_growth_daily.ps1'
$action = New-ScheduledTaskAction -Execute "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" -Argument "-NoProfile -WindowStyle Hidden -File `"$daily`" -Retry" -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Once -At $when
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopIfGoingOnBatteries -AllowStartIfOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 2)
Register-ScheduledTask -TaskName 'Recambios-Codex-Growth-Retry' -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
