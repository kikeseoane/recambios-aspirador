# Configura de una vez y para siempre el acceso de Grow a Search Console y GA4
# usando las APIs oficiales de Google (gratuitas, sin suscripciones).
#
# Uso:
#   powershell -ExecutionPolicy Bypass -File tools\configurar_google_grow.ps1 -Login
#     Abre el navegador para autorizar tu cuenta de Google (una sola vez) y
#     despues ejecuta toda la configuracion.
#   powershell -ExecutionPolicy Bypass -File tools\configurar_google_grow.ps1
#     Solo la configuracion (requiere haber hecho -Login antes).
#
# El script es idempotente: se puede ejecutar varias veces sin romper nada.
param(
    [switch]$Login,
    [switch]$SkipGrants,
    [string]$Ga4PropertyId = '529119132',
    [string]$GscDomain = 'recambios-aspirador.com'
)

$ErrorActionPreference = 'Stop'
$adcScopes = 'https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/analytics.manage.users,https://www.googleapis.com/auth/siteverification'

function Find-Gcloud {
    try { return (Get-Command gcloud.cmd -ErrorAction Stop).Source } catch {}
    try { return (Get-Command gcloud -ErrorAction Stop).Source } catch {}
    $candidates = @(
        (Join-Path $env:LOCALAPPDATA 'Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'),
        'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd',
        'C:\Program Files\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
    )
    foreach ($candidate in $candidates) {
        if (Test-Path -LiteralPath $candidate) { return $candidate }
    }
    throw 'No se encontro gcloud. Instala Google Cloud SDK: winget install Google.CloudSDK'
}

$gcloud = Find-Gcloud
Write-Host "gcloud: $gcloud"

if ($Login) {
    $active = (& $gcloud auth list --filter=status:ACTIVE --format='value(account)' | Select-Object -First 1)
    if ($active) {
        Write-Host "Sesion ya iniciada como $active; no hace falta repetir el login."
    } else {
        Write-Host 'Se abrira el navegador. Usa la cuenta de Google propietaria de Search Console y GA4.'
        & $gcloud auth login
        if ($LASTEXITCODE -ne 0) { throw 'Fallo gcloud auth login.' }
    }
    # Nota: Google bloquea pedir los permisos de GA4/Site Verification desde la
    # aplicacion de gcloud, asi que esos dos accesos se dan a mano (el script
    # imprime al final exactamente que email anadir y donde).
}

$account = (& $gcloud auth list --filter=status:ACTIVE --format='value(account)' | Select-Object -First 1)
if (-not $account) {
    Write-Host 'Aun no hay sesion de Google. Ejecuta primero:'
    Write-Host '  powershell -ExecutionPolicy Bypass -File tools\configurar_google_grow.ps1 -Login'
    exit 2
}
Write-Host "Cuenta activa: $account"

# --- Proyecto (buscamos el nuestro por etiqueta; si no existe, lo creamos) ---
$projectId = (& $gcloud projects list --filter='labels.app=grow-recambios' --format='value(projectId)' | Select-Object -First 1)
if (-not $projectId) {
    $projectId = 'grow-recambios-' + (Get-Random -Minimum 100000 -Maximum 999999)
    Write-Host "Creando proyecto $projectId..."
    & $gcloud projects create $projectId --name='Grow recambios-aspirador' --labels=app=grow-recambios
    if ($LASTEXITCODE -ne 0) { throw 'No se pudo crear el proyecto.' }
} else {
    Write-Host "Proyecto existente: $projectId"
}
& $gcloud config set project $projectId --quiet | Out-Null

Write-Host 'Habilitando APIs (Search Console, Analytics Data, Analytics Admin, Site Verification)...'
& $gcloud services enable searchconsole.googleapis.com analyticsdata.googleapis.com analyticsadmin.googleapis.com siteverification.googleapis.com
if ($LASTEXITCODE -ne 0) { throw 'No se pudieron habilitar las APIs.' }

# --- Cuenta de servicio y clave ---
$saEmail = "grow-analytics@$projectId.iam.gserviceaccount.com"
$saExists = (& $gcloud iam service-accounts list --filter="email:$saEmail" --format='value(email)' | Select-Object -First 1)
if (-not $saExists) {
    Write-Host "Creando cuenta de servicio $saEmail..."
    & $gcloud iam service-accounts create grow-analytics --display-name='Grow analytics read-only'
    if ($LASTEXITCODE -ne 0) { throw 'No se pudo crear la cuenta de servicio.' }
}

$keyPath = Join-Path $env:USERPROFILE '.growth\google-service-account.json'
New-Item -ItemType Directory -Path (Split-Path $keyPath) -Force | Out-Null
if (-not (Test-Path -LiteralPath $keyPath)) {
    Write-Host "Creando clave JSON en $keyPath..."
    & $gcloud iam service-accounts keys create $keyPath --iam-account=$saEmail
    if ($LASTEXITCODE -ne 0) { throw 'No se pudo crear la clave de la cuenta de servicio.' }
} else {
    Write-Host "Clave ya presente en $keyPath"
}

[Environment]::SetEnvironmentVariable('GOOGLE_APPLICATION_CREDENTIALS', $keyPath, 'User')
[Environment]::SetEnvironmentVariable('GROWTH_WINDSOR_API_KEY', $null, 'User')
$env:GOOGLE_APPLICATION_CREDENTIALS = $keyPath
if (Test-Path Env:GROWTH_WINDSOR_API_KEY) { Remove-Item Env:GROWTH_WINDSOR_API_KEY }
Write-Host 'Variable GOOGLE_APPLICATION_CREDENTIALS guardada; clave antigua de Windsor retirada.'

# --- Permisos de lectura para la cuenta de servicio en GA4 y Search Console ---
$pending = @()
if (-not $SkipGrants) {
    $adcToken = $null
    if (Test-Path "$env:APPDATA\gcloud\application_default_credentials.json") {
        try { $adcToken = (& $gcloud auth application-default print-access-token | Select-Object -First 1) } catch {}
    }
    if (-not $adcToken) {
        $pending += ("GA4: en https://analytics.google.com -> Administrar -> Gestion de acceso a la propiedad -> boton + -> Anadir usuarios: pega $saEmail, rol Lector, desmarca 'Notificar por correo'.")
        $pending += ("Search Console: en https://search.google.com/search-console -> propiedad recambios-aspirador.com -> Configuracion -> Usuarios y permisos -> Anadir usuario: pega $saEmail, permiso Restringido.")
    } else {
        $headers = @{ Authorization = "Bearer $adcToken"; 'X-Goog-User-Project' = $projectId }

        # GA4: rol Lector para la cuenta de servicio
        try {
            $bindings = Invoke-RestMethod -Method Get -Headers $headers -Uri "https://analyticsadmin.googleapis.com/v1beta/properties/$Ga4PropertyId/accessBindings?pageSize=200"
            $already = $false
            foreach ($binding in @($bindings.accessBindings)) {
                if ($binding.user -eq $saEmail) { $already = $true }
            }
            if ($already) {
                Write-Host 'GA4: la cuenta de servicio ya tiene acceso.'
            } else {
                $body = @{ user = $saEmail; roles = @('predefinedRoles/viewer') } | ConvertTo-Json
                Invoke-RestMethod -Method Post -Headers $headers -ContentType 'application/json' -Body $body -Uri "https://analyticsadmin.googleapis.com/v1beta/properties/$Ga4PropertyId/accessBindings" | Out-Null
                Write-Host "GA4: rol Lector concedido a $saEmail."
            }
        } catch {
            $pending += ("GA4 manual: en analytics.google.com -> Administrar -> Gestion de acceso a la propiedad, anade $saEmail con rol Lector. (" + $_.Exception.Message + ')')
        }

        # Search Console: anadir la cuenta de servicio como propietaria verificada del dominio
        try {
            $resources = Invoke-RestMethod -Method Get -Headers $headers -Uri 'https://www.googleapis.com/siteVerification/v1/webResource'
            $target = $null
            foreach ($item in @($resources.items)) {
                if ($item.site.identifier -like "*$GscDomain*") { $target = $item; break }
            }
            if (-not $target) {
                $pending += ("GSC manual: en search.google.com/search-console -> Configuracion -> Usuarios y permisos, anade $saEmail (permiso Restringido basta). La cuenta $account no figura como propietaria verificada via API.")
            } elseif (@($target.owners) -contains $saEmail) {
                Write-Host 'Search Console: la cuenta de servicio ya es propietaria.'
            } else {
                $target.owners = @($target.owners) + $saEmail
                $encodedId = [uri]::EscapeDataString($target.id)
                Invoke-RestMethod -Method Put -Headers $headers -ContentType 'application/json' -Body ($target | ConvertTo-Json -Depth 5) -Uri "https://www.googleapis.com/siteVerification/v1/webResource/$encodedId" | Out-Null
                Write-Host "Search Console: $saEmail anadida como propietaria de $GscDomain."
            }
        } catch {
            $pending += ("GSC manual: en search.google.com/search-console -> Configuracion -> Usuarios y permisos, anade $saEmail (permiso Restringido basta). (" + $_.Exception.Message + ')')
        }
    }
}

Write-Host ''
Write-Host '--- Comprobacion final: consulta real a las APIs ---'
$repoRoot = Split-Path -Parent $PSScriptRoot
& python (Join-Path $PSScriptRoot 'growth_analytics.py')

if ($pending.Count -gt 0) {
    Write-Host ''
    Write-Host 'PASOS QUE QUEDAN (Google no permite automatizarlos):'
    foreach ($step in $pending) { Write-Host "  - $step" }
    Write-Host ''
    Write-Host 'Cuando los hagas, comprueba con: python tools\growth_analytics.py'
    exit 3
}
Write-Host ''
Write-Host 'Todo configurado. Grow usara las APIs oficiales de Google a partir de ahora.'
