$root = (Get-Location).Path

# =========================
# CONFIG
# =========================

$excludeNamePatterns = @(
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
  "*.dll",
  ".env"
)

$groups = @(
  @{
    Name = "dump_core_data.txt"
    Title = "DATA"
    Include = @(
      ".\config.toml",
      ".\data\*.yaml"
    )
  },
  @{
    Name = "dump_core_layouts.txt"
    Title = "LAYOUTS"
    Include = @(
      ".\layouts\_default\baseof.html",
      ".\layouts\_default\list.html",
      ".\layouts\_default\single.html",
      ".\layouts\modelos\*.html",
      ".\layouts\marcas\*.html",
      ".\layouts\guias\*.html",
      ".\layouts\guia\*.html",
      ".\layouts\partials\footer.html",
      ".\layouts\partials\header.html",
      ".\layouts\partials\hero.html",
      ".\layouts\partials\marcas-grid.html",
      ".\layouts\partials\model_crosslinks.html",
      ".\layouts\partials\model_title.html",
      ".\layouts\partials\offer_btn.html",
      ".\layouts\partials\get_guia.html",
      ".\layouts\partials\card_icon.html",
      ".\layouts\partials\cat_icon.html",
      ".\layouts\partials\head\canonical.html",
      ".\layouts\partials\schema\*.html",
      ".\layouts\index.html",
      ".\layouts\robots.txt",
      ".\layouts\sitemap.xml"
    )
  },
  @{
    Name = "dump_core_tools.txt"
    Title = "TOOLS"
    Include = @(
      ".\tools\generar.py",
      ".\tools\build_catalog.py",
      ".\tools\armageddon_catalog.py",
      ".\tools\sync_aliexpress.py",
      ".\tools\sync_ofertas.py"
    )
  },
  @{
    Name = "dump_core_content.txt"
    Title = "CONTENT"
    Include = @(
      ".\content\marcas\_index.md",
      ".\content\modelos\_index.md",
      ".\content\guias\_index.md",
      ".\content\modelos\dyson-v11\_index.md",
      ".\content\modelos\dyson-v11\filtro\index.md",
      ".\content\modelos\dyson-v11\bateria\index.md",
      ".\content\modelos\dyson-v11\problemas\no-carga\index.md",
      ".\content\modelos\roborock-s7\_index.md",
      ".\content\modelos\roborock-s7\filtro\index.md",
      ".\content\modelos\roborock-s7\problemas\no-aspira\index.md"
    )
  }
)

# =========================
# HELPERS
# =========================

function RelPath([string]$fullPath) {
  $rel = $fullPath.Substring($root.Length)
  if ($rel.StartsWith([System.IO.Path]::DirectorySeparatorChar)) {
    $rel = $rel.Substring(1)
  }
  return ".\" + $rel
}

function Matches-AnyPattern([string]$text, [string[]]$patterns) {
  foreach ($p in $patterns) {
    if ($text -like $p) { return $true }
  }
  return $false
}

function Get-FileContentSafe([string]$path) {
  try {
    return Get-Content -LiteralPath $path -Raw -Encoding UTF8 -ErrorAction Stop
  } catch {
    try {
      return Get-Content -LiteralPath $path -Raw -Encoding Default -ErrorAction Stop
    } catch {
      return "[NO SE PUDO LEER: $($_.Exception.Message)]"
    }
  }
}

function Build-DumpFile($group) {
  $out = Join-Path $root $group.Name
  $sb = New-Object System.Text.StringBuilder

  $null = $sb.AppendLine("==== $($group.Title) ====")
  $null = $sb.AppendLine("")

  $allFiles = Get-ChildItem -Path $root -Recurse -File -Force | Sort-Object FullName
  $selected = New-Object System.Collections.Generic.List[System.IO.FileInfo]
  $included = New-Object System.Collections.Generic.HashSet[string]

  foreach ($f in $allFiles) {
    if ($f.FullName -eq $out) { continue }
    if (Matches-AnyPattern $f.Name $excludeNamePatterns) { continue }

    $rel = RelPath $f.FullName

    if (Matches-AnyPattern $rel $group.Include) {
      $key = $f.FullName.ToLowerInvariant()
      if (-not $included.Contains($key)) {
        [void]$selected.Add($f)
        [void]$included.Add($key)
      }
    }
  }

  $selected = $selected | Sort-Object FullName -Unique

  $null = $sb.AppendLine("==== ESTRUCTURA ====")
  $null = $sb.AppendLine("")
  foreach ($f in $selected) {
    $null = $sb.AppendLine((RelPath $f.FullName))
  }

  $null = $sb.AppendLine("")
  $null = $sb.AppendLine("==== CONTENIDO ====")

  foreach ($f in $selected) {
    $null = $sb.AppendLine("")
    $null = $sb.AppendLine(("=" * 50))
    $null = $sb.AppendLine("ARCHIVO: " + (RelPath $f.FullName))
    $null = $sb.AppendLine(("=" * 50))
    $null = $sb.AppendLine((Get-FileContentSafe $f.FullName))
  }

  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  [System.IO.File]::WriteAllText($out, $sb.ToString(), $utf8NoBom)

  Write-Host "Generado: $out" -ForegroundColor Green
}

# =========================
# GENERAR TODOS
# =========================

foreach ($g in $groups) {
  Build-DumpFile $g
}

Write-Host ""
Write-Host "Listo. Archivos generados por bloques." -ForegroundColor Yelloww