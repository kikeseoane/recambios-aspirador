$root = Get-Location
$out  = Join-Path $root "todo.txt"

$sb = New-Object System.Text.StringBuilder

$null = $sb.AppendLine("==== ESTRUCTURA DE DIRECTORIOS ====")
$null = $sb.AppendLine("")

# Directorios (sin tree para evitar caracteres OEM)
Get-ChildItem -Recurse -Directory | ForEach-Object {
  $rel = $_.FullName.Replace($root.Path, ".")
  $null = $sb.AppendLine($rel)
}

$null = $sb.AppendLine("")
$null = $sb.AppendLine("==== CONTENIDO DE ARCHIVOS ====")

# Archivos + contenido
Get-ChildItem -Recurse -File | ForEach-Object {
  $null = $sb.AppendLine("")
  $null = $sb.AppendLine("===============================")
  $null = $sb.AppendLine(("ARCHIVO: " + $_.FullName))
  $null = $sb.AppendLine("===============================")

  try {
    $content = Get-Content -Raw -Encoding UTF8 $_.FullName -ErrorAction Stop
  } catch {
    $content = Get-Content -Raw -Encoding Default $_.FullName
  }

  $null = $sb.AppendLine($content)
}

# Escribir SIEMPRE en UTF-8 sin BOM
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($out, $sb.ToString(), $utf8NoBom)
