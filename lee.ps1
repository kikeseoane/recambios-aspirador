$root = (Get-Location).Path
$out  = Join-Path $root "dump_state.txt"

$sb = New-Object System.Text.StringBuilder

# =========================
# CONFIG
# =========================

# Carpetas generadas / ruido (segmento de ruta)
$excludeDirNames = @(
  ".git",
  "public",
  "resources",
  "node_modules",
  ".vscode",
  ".idea",
  ".cache",
  "dist",
  "build"
)

# Excluir por extensión / nombre (SOLO nombre, NO ruta)
# Nota: patrones -like aplicados a $_.Name
$excludeNamePatterns = @(
  "dump_state.txt",
  ".hugo_build.lock",
  "*.map",
  "*.min.js",
  "*.min.css",
  "*.woff",
  "*.woff2",
  "*.ttf",
  "*.otf",
  "*.eot",
  "*.png",
  "*.jpg",
  "*.jpeg",
  "*.webp",
  "*.gif",
  "*.ico",
  "*.pdf",
  "*.zip",
  "*.7z",
  "*.rar",
  "*.gz",
  "*.tar",
  "*.exe",
  "*.dll"
)

# Excluir por ruta relativa (globs) — aquí SI se compara contra $rel
# Útil si quieres cortar cosas concretas por path
$excludeRelPathGlobs = @(
  ".\public\*",
  ".\resources\*"
)

# Excluir todo content/ y luego añadir muestra
$excludeContent = $true

# Whitelist de content/ (si existen, se incluyen)
$includeContentWhitelist = @(
  "content/_index.md",
  "content/index.md",
  "content/marcas/_index.md",
  "content/modelos/_index.md",
  "content/guias/_index.md",
  "content/problemas/_index.md",
  "content/recambios/_index.md"
)

# Muestra adicional: primeros N markdown de content/ (además del whitelist)
$maxExtraContentMd = 12

# =========================
# HELPERS
# =========================

function RelPath([string]$fullPath) {
  $rel = $fullPath.Substring($root.Length)
  if ($rel.StartsWith([System.IO.Path]::DirectorySeparatorChar)) { $rel = $rel.Substring(1) }
  return ".\" + $rel
}

function Is-UnderExcludedDir([string]$fullPath) {
  # Comparación por segmentos (sin regex)
  $p = $fullPath.ToLowerInvariant()

  foreach ($d in $excludeDirNames) {
    $seg = "\" + $d.ToLowerInvariant() + "\"
    if ($p.Contains($seg)) { return $true }

    # Caso borde: si la ruta acaba justo en el directorio (sin \ final)
    if ($p.EndsWith("\" + $d.ToLowerInvariant())) { return $true }
  }
  return $false
}

function Matches-AnyPattern([string]$text, [string[]]$patterns) {
  foreach ($p in $patterns) {
    if ($text -like $p) { return $true }
  }
  return $false
}

function Is-UnderContent([string]$relPath) {
  return ($relPath -like ".\content\*")
}

# =========================
# 1) DIRECTORIOS (filtrados)
# =========================
$null = $sb.AppendLine("==== ESTRUCTURA DE DIRECTORIOS (FILTRADA) ====")
$null = $sb.AppendLine("")

Get-ChildItem -Path $root -Recurse -Directory -Force |
  Sort-Object FullName |
  ForEach-Object {
    if (Is-UnderExcludedDir $_.FullName) { return }
    $null = $sb.AppendLine((RelPath $_.FullName))
  }

$null = $sb.AppendLine("")
$null = $sb.AppendLine("==== CONTENIDO DE ARCHIVOS (FILTRADO) ====")

# =========================
# 2) ARCHIVOS BASE (sin content/ si $excludeContent)
# =========================
$files = New-Object System.Collections.Generic.List[System.IO.FileInfo]

Get-ChildItem -Path $root -Recurse -File -Force |
  Sort-Object FullName |
  ForEach-Object {

    if ($_.FullName -eq $out) { return }
    if (Is-UnderExcludedDir $_.FullName) { return }

    $rel = RelPath $_.FullName

    # Excluir por nombre (extensiones/binarios)
    if (Matches-AnyPattern $_.Name $excludeNamePatterns) { return }

    # Excluir por ruta relativa explícita (si aplica)
    if ($excludeRelPathGlobs.Count -gt 0 -and (Matches-AnyPattern $rel $excludeRelPathGlobs)) { return }

    # Excluir content/ completo (luego lo muestreamos)
    if ($excludeContent -and (Is-UnderContent $rel)) { return }

    [void]$files.Add($_)
  }

# =========================
# 3) MUESTRA DE content/
# =========================
$included = New-Object System.Collections.Generic.HashSet[string]  # fullpath lower

# 3.1 Whitelist siempre entra (si existe)
foreach ($w in $includeContentWhitelist) {
  $p = Join-Path $root $w
  if (Test-Path -LiteralPath $p) {
    $fi = Get-Item -LiteralPath $p -ErrorAction SilentlyContinue
    if ($fi) {
      $k = $fi.FullName.ToLowerInvariant()
      if (-not $included.Contains($k)) {
        [void]$files.Add($fi)
        [void]$included.Add($k)
      }
    }
  }
}

# 3.2 Extra md: limitado a $maxExtraContentMd (además del whitelist)
if ($excludeContent) {
  $contentRoot = Join-Path $root "content"
  if (Test-Path -LiteralPath $contentRoot) {

    $extraCount = 0

    Get-ChildItem -LiteralPath $contentRoot -Recurse -File -Force -ErrorAction SilentlyContinue |
      Where-Object { $_.Extension -ieq ".md" } |
      Sort-Object FullName |
      ForEach-Object {
        if ($extraCount -ge $maxExtraContentMd) { return }

        $k = $_.FullName.ToLowerInvariant()
        if (-not $included.Contains($k)) {
          [void]$files.Add($_)
          [void]$included.Add($k)
          $extraCount++
        }
      }
  }
}

# Deduplicar por FullName
$files = $files | Sort-Object FullName -Unique

# =========================
# 4) VOLCADO CONTENIDOS
# =========================
foreach ($f in $files) {
  $null = $sb.AppendLine("")
  $null = $sb.AppendLine(("=" * 40))
  $null = $sb.AppendLine("ARCHIVO: " + (RelPath $f.FullName))
  $null = $sb.AppendLine(("=" * 40))

  try {
    $content = Get-Content -LiteralPath $f.FullName -Raw -Encoding UTF8 -ErrorAction Stop
  } catch {
    try {
      $content = Get-Content -LiteralPath $f.FullName -Raw -Encoding Default -ErrorAction Stop
    } catch {
      $content = "[NO SE PUDO LEER: $($_.Exception.Message)]"
    }
  }

  $null = $sb.AppendLine($content)
}

# =========================
# 5) ESCRITURA
# =========================
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($out, $sb.ToString(), $utf8NoBom)

Write-Host "Archivo generado: $out" -ForegroundColor Green
Write-Host "Filtrado aplicado: carpetas generadas + binarios (por nombre) + (content/ muestreado)" -ForegroundColor Yellow
