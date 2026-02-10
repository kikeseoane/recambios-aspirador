$root = Get-Location
$out  = Join-Path $root "~TODO_COMPLETE.txt"

$sb = New-Object System.Text.StringBuilder

# 1. PRIMERO: Estructura de directorios
$null = $sb.AppendLine("==== ESTRUCTURA DE DIRECTORIOS ====")
$null = $sb.AppendLine("")
Get-ChildItem -Recurse -Directory | Sort-Object FullName | ForEach-Object {
  $rel = $_.FullName.Replace($root.Path, ".")
  $null = $sb.AppendLine($rel)
}

$null = $sb.AppendLine("")
$null = $sb.AppendLine("==== CONTENIDO DE ARCHIVOS (ORDEN ALFABÉTICO) ====")

# 2. SEGUNDO: Archivos ordenados alfabéticamente
Get-ChildItem -Recurse -File | Sort-Object FullName | ForEach-Object {
  # Saltar el archivo de salida para no incluirlo
  if ($_.FullName -eq $out) { continue }
  
  $null = $sb.AppendLine("")
  $null = $sb.AppendLine("=" * 40)
  $null = $sb.AppendLine("ARCHIVO: " + $_.FullName.Replace($root.Path, "."))
  $null = $sb.AppendLine("=" * 40)

  try {
    $content = Get-Content -Raw -Encoding UTF8 $_.FullName -ErrorAction Stop
  } catch {
    $content = Get-Content -Raw -Encoding Default $_.FullName
  }
  
  $null = $sb.AppendLine($content)
}

# 3. Escribir archivo (siempre será el último en listados futuros)
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($out, $sb.ToString(), $utf8NoBom)

Write-Host "Archivo generado: $out" -ForegroundColor Green
Write-Host "Este archivo aparecerá ÚLTIMO en futuros listados" -ForegroundColor Yellow