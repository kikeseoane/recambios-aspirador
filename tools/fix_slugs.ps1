# tools\fix_slugs.ps1
$root = (Resolve-Path ".").Path
$files = Get-ChildItem -Path "$root\content\modelos" -Recurse -Filter "index.md"

foreach ($f in $files) {
  $txt = Get-Content $f.FullName -Raw

  # Solo nos interesa si hay front matter y aparece slug:
  if ($txt -match "(?s)^---\s.*?\s---" -and $txt -match "(?m)^\s*slug\s*:") {

    # Quita cualquier línea slug: ... (en front matter)
    # (si algún día necesitas slug en una página concreta, la vuelves a añadir a mano)
    $new = $txt -replace "(?m)^\s*slug\s*:\s*.*\r?\n", ""

    if ($new -ne $txt) {
      Set-Content -Path $f.FullName -Value $new -NoNewline -Encoding UTF8
      Write-Host "OK  " $f.FullName
    }
  }
}

Write-Host "`nHecho."
