#requires -version 5.1
<#
tools/dump_state.ps1
- Genera un dump "filtrado" del estado relevante del repo Hugo para pegarlo en ChatGPT/IA.
- Excluye artefactos generados: public/, resources/, caches, node_modules/, etc.
- Incluye: estructura de directorios (filtrada), ficheros clave (config/layouts/data/tools),
  y snippets de content (front matter + primeras lineas).
- Al final añade el "PROMPT MAESTRO" EXACTO para que lo copies/pegues tal cual.

USO:
  powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\dump_state.ps1
  powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\dump_state.ps1 -OutFile .\dump_state.txt

NOTA IMPORTANTE (encoding):
- Guarda este archivo como "UTF-8 with BOM" para que no salgan cosas tipo ActÃºa / persuasiÃ³n / â€œ â€.
  En VS Code: "Save with Encoding" -> "UTF-8 with BOM".
#>

param(
  [string]$OutFile = ""
)

$ErrorActionPreference = "Stop"

# --- UTF-8 robusto en consola + fichero (con BOM) ---
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$utf8Bom = New-Object System.Text.UTF8Encoding($true)

# --- Rutas base ---
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = Resolve-Path (Join-Path -Path $ScriptDir -ChildPath "..") | Select-Object -ExpandProperty Path

if ([string]::IsNullOrWhiteSpace($OutFile)) {
  $OutFile = Join-Path -Path $Root -ChildPath "dump_state.txt"
} else {
  if (-not [System.IO.Path]::IsPathRooted($OutFile)) {
    $OutFile = Join-Path -Path $Root -ChildPath $OutFile
  }
}

# --- Exclusiones (carpetas/archivos generados) ---
$ExcludeDirs = @(
  "public",
  "resources",
  "node_modules",
  ".git",
  ".vscode",
  ".idea",
  ".cache",
  "dist",
  "build",
  ".hugo_build.lock",
  "hugo_stats.json",
  ".DS_Store",
  "Thumbs.db"
)

function Is-ExcludedPath {
  param([string]$FullPath)

  $p = $FullPath.Replace("\","/").ToLowerInvariant()

  foreach ($d in $ExcludeDirs) {
    $dd = $d.Replace("\","/").ToLowerInvariant()
    if ($p -match "/$([Regex]::Escape($dd))/") { return $true }
    if ($p.EndsWith("/$dd")) { return $true }
  }

  return $false
}

function RelPath {
  param([string]$FullPath)
  $rp = $FullPath.Substring($Root.Length).TrimStart("\","/")
  if ([string]::IsNullOrWhiteSpace($rp)) { return "." }
  return $rp.Replace("\","/")
}

function Write-Section {
  param($w, [string]$title)
  $w.WriteLine("")
  $w.WriteLine("==== " + $title + " ====")
  $w.WriteLine("")
}

function Safe-ReadAllText {
  param([string]$Path)
  try {
    return [System.IO.File]::ReadAllText($Path, [System.Text.Encoding]::UTF8)
  } catch {
    try {
      return [System.IO.File]::ReadAllText($Path, [System.Text.Encoding]::Default)
    } catch {
      return $null
    }
  }
}

function Write-FileBlock {
  param(
    $w,
    [string]$FullPath,
    [int]$MaxChars = 24000,
    [int]$HeadChars = 14000,
    [int]$TailChars = 6000
  )

  if (-not (Test-Path -LiteralPath $FullPath)) { return }
  if (Is-ExcludedPath -FullPath $FullPath) { return }

  $rel = RelPath $FullPath
  $fi = Get-Item -LiteralPath $FullPath -ErrorAction SilentlyContinue
  if ($null -eq $fi) { return }

  $w.WriteLine("===============================")
  $w.WriteLine("ARCHIVO: " + $rel)
  $w.WriteLine("TAMANO:  " + $fi.Length + " bytes")
  $w.WriteLine("===============================")

  if ($fi.PSIsContainer) {
    $w.WriteLine("[DIRECTORIO]")
    $w.WriteLine("")
    return
  }

  $txt = Safe-ReadAllText -Path $FullPath
  if ($null -eq $txt) {
    $w.WriteLine("[No se pudo leer el archivo como texto]")
    $w.WriteLine("")
    return
  }

  if ($txt.Length -le $MaxChars) {
    $w.WriteLine($txt)
  } else {
    $w.WriteLine("[RECORTADO] Longitud original: " + $txt.Length + " chars")
    $w.WriteLine("")
    $w.WriteLine("----- BEGIN HEAD -----")
    $w.WriteLine($txt.Substring(0, [Math]::Min($HeadChars, $txt.Length)))
    $w.WriteLine("----- END HEAD -----")
    $w.WriteLine("")
    $w.WriteLine("----- BEGIN TAIL -----")
    $w.WriteLine($txt.Substring([Math]::Max(0, $txt.Length - $TailChars)))
    $w.WriteLine("----- END TAIL -----")
  }

  $w.WriteLine("")
}

function Write-Tree {
  param(
    $w,
    [string]$BaseDir,
    [int]$MaxDepth = 6
  )

  $base = Resolve-Path $BaseDir | Select-Object -ExpandProperty Path

  $w.WriteLine("Root: " + (RelPath $base))
  $w.WriteLine("")

  function Write-TreeNode {
    param(
      [string]$Path,
      [int]$Depth,
      [string]$Prefix,
      [bool]$IsLast
    )

    if (Is-ExcludedPath -FullPath $Path) { return }
    if ($Depth -gt $MaxDepth) { return }

    $item = Get-Item -LiteralPath $Path -ErrorAction SilentlyContinue
    if ($null -eq $item) { return }

    $name = Split-Path -Leaf $Path
    if ([string]::IsNullOrWhiteSpace($name)) { $name = "." }

    # Conector
    $connector = "├─ "
    if ($IsLast) { $connector = "└─ " }

    # Sufijo
    $suffix = ""
    if ($item.PSIsContainer) { $suffix = "/" }

    # Línea actual
    $w.WriteLine($Prefix + $connector + $name + $suffix)

    if ($Depth -eq $MaxDepth) { return }
    if (-not $item.PSIsContainer) { return }

    # Hijos (dirs primero, luego files)
    $children = Get-ChildItem -LiteralPath $Path -Force -ErrorAction SilentlyContinue |
      Where-Object { -not (Is-ExcludedPath -FullPath $_.FullName) } |
      Sort-Object @{Expression={$_.PSIsContainer};Descending=$true}, Name

    if ($null -eq $children -or $children.Count -eq 0) { return }

    # Prefijo para hijos
    $childPrefix = $Prefix
    if ($IsLast) {
      $childPrefix += "   "
    } else {
      $childPrefix += "│  "
    }

    for ($i = 0; $i -lt $children.Count; $i++) {
      $c = $children[$i]
      $last = ($i -eq ($children.Count - 1))
      Write-TreeNode -Path $c.FullName -Depth ($Depth + 1) -Prefix $childPrefix -IsLast $last
    }
  }

  # Top children del root
  $topChildren = Get-ChildItem -LiteralPath $base -Force -ErrorAction SilentlyContinue |
    Where-Object { -not (Is-ExcludedPath -FullPath $_.FullName) } |
    Sort-Object @{Expression={$_.PSIsContainer};Descending=$true}, Name

  for ($i = 0; $i -lt $topChildren.Count; $i++) {
    $c = $topChildren[$i]
    $last = ($i -eq ($topChildren.Count - 1))
    Write-TreeNode -Path $c.FullName -Depth 1 -Prefix "" -IsLast $last
  }

  $w.WriteLine("")
}



function Get-ConfigPaths {
  # OJO: nada de comas dentro de Join-Path (evita System.Object[]).
  $candidates = @(
    (Join-Path -Path $Root -ChildPath "hugo.toml")
    (Join-Path -Path $Root -ChildPath "config.toml")
    (Join-Path -Path $Root -ChildPath "hugo.yaml")
    (Join-Path -Path $Root -ChildPath "config.yaml")
    (Join-Path -Path $Root -ChildPath "hugo.json")
    (Join-Path -Path $Root -ChildPath "config.json")
  )
  return $candidates | Where-Object { Test-Path -LiteralPath $_ }
}

function Get-ImportantLayoutPaths {
  $paths = @()

  $p1 = Join-Path -Path $Root -ChildPath "layouts/partials/head/canonical.html"
  if (Test-Path -LiteralPath $p1) { $paths += $p1 }

  $p2 = Join-Path -Path $Root -ChildPath "layouts/modelos/single.html"
  if (Test-Path -LiteralPath $p2) { $paths += $p2 }

  $glob = @(
    "layouts/_default/baseof.html",
    "layouts/_default/single.html",
    "layouts/_default/list.html",
    "layouts/partials/head/*.html",
    "layouts/partials/schema*.html",
    "layouts/partials/breadcrumbs*.html",
    "layouts/partials/*.html",
    "layouts/**/single.html",
    "layouts/**/list.html",
    "layouts/**/terms.html",
    "layouts/**/taxonomy.html"
  )

  foreach ($g in $glob) {
    $full = Join-Path -Path $Root -ChildPath $g
    Get-ChildItem -Path $full -File -ErrorAction SilentlyContinue |
      Where-Object { -not (Is-ExcludedPath -FullPath $_.FullName) } |
      ForEach-Object { $paths += $_.FullName }
  }

  return $paths | Sort-Object -Unique
}

function Get-DataPaths {
  $dataDir = Join-Path -Path $Root -ChildPath "data"
  if (-not (Test-Path -LiteralPath $dataDir)) { return @() }
  return Get-ChildItem -LiteralPath $dataDir -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object { -not (Is-ExcludedPath -FullPath $_.FullName) } |
    Sort-Object FullName |
    Select-Object -ExpandProperty FullName
}

function Get-ToolsPaths {
  $toolsDir = Join-Path -Path $Root -ChildPath "tools"
  if (-not (Test-Path -LiteralPath $toolsDir)) { return @() }
  return Get-ChildItem -LiteralPath $toolsDir -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object { -not (Is-ExcludedPath -FullPath $_.FullName) } |
    Sort-Object FullName |
    Select-Object -ExpandProperty FullName
}

function Get-ContentSamplePaths {
  $contentDir = Join-Path -Path $Root -ChildPath "content"
  if (-not (Test-Path -LiteralPath $contentDir)) { return @() }

  $all = Get-ChildItem -LiteralPath $contentDir -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object {
      -not (Is-ExcludedPath -FullPath $_.FullName) -and
      ($_.Extension -in @(".md",".markdown",".mdx"))
    } |
    Sort-Object FullName

  return $all | Select-Object -First 60 | Select-Object -ExpandProperty FullName
}

function Write-ContentFrontMatterSnippet {
  param($w, [string]$FullPath, [int]$MaxLines = 120)

  if (-not (Test-Path -LiteralPath $FullPath)) { return }
  if (Is-ExcludedPath -FullPath $FullPath) { return }

  $rel = RelPath $FullPath
  $txt = Safe-ReadAllText -Path $FullPath
  if ($null -eq $txt) { return }

  $lines = $txt -split "`r?`n"

  $w.WriteLine("===============================")
  $w.WriteLine("CONTENT (snippet): " + $rel)
  $w.WriteLine("===============================")

  if ($lines.Count -gt 0 -and $lines[0].Trim() -eq "---") {
    $w.WriteLine("[Front matter YAML + primeras lineas]")
    $i = 0
    $fmEnded = $false

    for ($j=0; $j -lt $lines.Count -and $j -lt $MaxLines; $j++) {
      $w.WriteLine($lines[$j])
      if ($j -ne 0 -and $lines[$j].Trim() -eq "---") {
        $fmEnded = $true
        $i = $j + 1
        break
      }
    }

    if ($fmEnded) {
      $w.WriteLine("")
      $w.WriteLine("----- BODY PREVIEW -----")
      $k = 0
      for ($j=$i; $j -lt $lines.Count -and $k -lt 40 -and ($j -lt ($i + 400)); $j++) {
        $w.WriteLine($lines[$j])
        $k++
      }
    }

  } elseif ($lines.Count -gt 0 -and $lines[0].Trim() -eq "+++") {
    $w.WriteLine("[Front matter TOML + primeras lineas]")
    $i = 0
    $fmEnded = $false

    for ($j=0; $j -lt $lines.Count -and $j -lt $MaxLines; $j++) {
      $w.WriteLine($lines[$j])
      if ($j -ne 0 -and $lines[$j].Trim() -eq "+++") {
        $fmEnded = $true
        $i = $j + 1
        break
      }
    }

    if ($fmEnded) {
      $w.WriteLine("")
      $w.WriteLine("----- BODY PREVIEW -----")
      $k = 0
      for ($j=$i; $j -lt $lines.Count -and $k -lt 40 -and ($j -lt ($i + 400)); $j++) {
        $w.WriteLine($lines[$j])
        $k++
      }
    }

  } else {
    $w.WriteLine("[Sin front matter detectable - primeras lineas]")
    for ($j=0; $j -lt $lines.Count -and $j -lt $MaxLines; $j++) {
      $w.WriteLine($lines[$j])
    }
  }

  $w.WriteLine("")
}

# --- Escribir salida ---
$dirOut = Split-Path -Parent $OutFile
if (-not (Test-Path -LiteralPath $dirOut)) {
  New-Item -ItemType Directory -Path $dirOut -Force | Out-Null
}

$w = New-Object System.IO.StreamWriter($OutFile, $false, $utf8Bom)

try {
  $w.WriteLine("# DUMP STATE Hugo repo")
  $w.WriteLine("# Root: " + $Root)
  $w.WriteLine("# Generated: " + (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"))
  $w.WriteLine("")

  Write-Section $w "DIRECTORY TREE (filtrado)"
  Write-Tree $w $Root 6

  Write-Section $w "CONFIG (hugo.toml / config.toml / etc.)"
  $cfgs = Get-ConfigPaths
  if ($cfgs.Count -eq 0) {
    $w.WriteLine("[No se encontro config/hugo.* en root]")
  } else {
    foreach ($c in $cfgs) { Write-FileBlock $w $c 50000 35000 8000 }
  }

  Write-Section $w "LAYOUTS (templates + partials relevantes)"
  $layouts = Get-ImportantLayoutPaths
  if ($layouts.Count -eq 0) {
    $w.WriteLine("[No se encontro layouts/ o no hay archivos matching]")
  } else {
    foreach ($p in $layouts) { Write-FileBlock $w $p 50000 35000 8000 }
  }

  Write-Section $w "DATA (data/**)"
  $datas = Get-DataPaths
  if ($datas.Count -eq 0) {
    $w.WriteLine("[No se encontro data/ o esta vacio]")
  } else {
    foreach ($p in $datas) { Write-FileBlock $w $p 70000 52000 12000 }
  }

  Write-Section $w "TOOLS (tools/**)"
  $tools = Get-ToolsPaths
  if ($tools.Count -eq 0) {
    $w.WriteLine("[No se encontro tools/ o esta vacio]")
  } else {
    foreach ($p in $tools) { Write-FileBlock $w $p 70000 52000 12000 }
  }

  Write-Section $w "CONTENT (muestra de front matter + preview)"
  $samples = Get-ContentSamplePaths
  if ($samples.Count -eq 0) {
    $w.WriteLine("[No se encontro content/ o no hay markdown]")
  } else {
    foreach ($p in $samples) { Write-ContentFrontMatterSnippet $w $p 140 }
  }

  Write-Section $w "NOTAS DE EXCLUSION (artefactos generados)"
  $w.WriteLine("Excluidos: public/, resources/, .hugo_build.lock, hugo_stats.json, .DS_Store, Thumbs.db, .vscode/, node_modules/, caches temporales.")
  $w.WriteLine("")

  Write-Section $w "PROMPT PARA COPIAR Y PEGAR (NO EDITAR)"
  $prompt = @"
Actúa como arquitecto senior SEO + ingeniero de sistemas para Hugo (static site generator) especializado en webs de afiliados escalables, aplicando principios de persuasión avanzada en copywriting y diseño de conversión.

CONTEXTO Y ESTADO ACTUAL

Estoy construyendo una web tipo enciclopedia de marcas y modelos de aspiradores y sus recambios compatibles, fallos comunes y guías.

La web está hecha con Hugo y el objetivo es escalar a miles de modelos sin escribir páginas manuales.

Mi enfoque es data-driven:
- Toda la información vive en un único fichero de datos (YAML o CSV).
- Un script generador (Python) crea los stubs mínimos en /content/ si faltan.
- Hugo renderiza páginas automáticas con plantillas.
- Se busca evitar duplicación de contenido y errores SEO.

Tengo problemas previos de indexación (Search Console):
- URLs duplicadas
- canónicas mal definidas
- páginas no enlazadas correctamente
- algunas páginas detectadas como duplicadas o redirigidas

OBJETIVO PRINCIPAL

Quiero orientar el proyecto al modelo de monetización SEO + afiliados:
1. Capturar búsquedas transaccionales: "batería dyson v11 compatible", "filtro conga 3090", "cargador rowenta x-force".
2. Capturar búsquedas informacionales: "dyson v11 no carga", "conga pierde potencia", etc.
3. Monetizar con Amazon Afiliados + AliExpress Afiliados + Adsense.

APLICA ESTOS PRINCIPIOS DE PERSUASIÓN Y CONVERSIÓN:
1. REPETICIÓN ESTRATÉGICA: Mensajes clave repetidos en puntos de conversión críticos
2. SIMPLIFICACIÓN RADICAL: Reducir opciones complejas a decisiones binarias claras
3. CONTRASTE DRAMÁTICO: Mostrar beneficios vs consecuencias de no actuar
4. TRANSFERENCIA DE AUTORIDAD: Usar marcas reconocidas para construir confianza
5. ORQUESTACIÓN UNIFICADA: Todo el contenido refuerza el mismo mensaje central
6. CONEXIÓN EMOCIONAL: Enfocarse en dolores específicos del usuario
7. COBERTURA COMPLETA: Saturación controlada de todas las intenciones de búsqueda
8. ANTICIPACIÓN DE OBJECIONES: Resolver dudas antes de que surjan
9. CREACIÓN DE URGENCIA CONTROLADA: Mostrar disponibilidad o precios especiales
10. VALIDACIÓN SOCIAL: Usar datos y testimonios para confirmar decisiones
11. NARRATIVA TRANSFORMADORA: Posicionar productos como soluciones "inteligentes"

TAREA COMPLETA

Diseña una arquitectura completa (técnica + SEO + conversión) para que la web sea una máquina escalable y bien indexada, incluyendo:

1. ESTRUCTURA DE URLs definitiva (sin duplicados, limpia, escalable)
2. TIPOS DE PÁGINAS esenciales:
   - Páginas de marca (nivel 1)
   - Páginas de modelo (nivel 2)
   - Páginas de recambio por modelo (nivel 3)
   - Páginas de problemas por modelo (nivel 3)
   - Guías generales comparativas (evergreen)

3. CAMPOS del fichero YAML/CSV maestro para automatización completa
4. SCRIPT Python exacto (qué stubs, qué front matter, qué slugs genera)
5. PLANTILLAS Hugo diseñadas para:
   - Evitar contenido duplicado
   - Hacer cada página única y valiosa
   - Implementar principios de persuasión en secciones clave

6. IMPLEMENTACIÓN TÉCNICA CORRECTA:
   - canonical URLs automáticas
   - sitemap.xml jerárquico
   - robots.txt estratégico
   - schema.org (FAQ, Product, BreadcrumbList, HowTo)
   - breadcrumbs automáticos
   - enlaces internos inteligentes

7. ESTRATEGIA SEO DE ESCALADO:
   - Cómo atacar long-tail keywords masivamente
   - Cómo priorizar modelos/recambios por potencial
   - Cómo generar contenido útil evitando penalizaciones

8. ESTRATEGIA DE MONETIZACIÓN INTEGRADA:
   - Ubicación óptima de enlaces de afiliado
   - Tablas comparativas que guían a la conversión
   - Shortcodes para afiliación sin ensuciar markdown
   - Llamadas a la acción estratégicamente ubicadas

9. CHECKLIST DE VALIDACIÓN:
   - Cómo revisar y eliminar duplicados
   - Cómo comprobar indexación correcta
   - Cómo evitar páginas huérfanas
   - Cómo medir y optimizar conversión

ESPECIFICACIONES TÉCNICAS

- Una sola fuente de verdad (YAML/CSV principal)
- No editar archivos manualmente
- Todo automatizable
- Añadir nuevos modelos = una línea en YAML/CSV
- Preparado para 10,000+ páginas
- Máxima sencillez operativa

FORMATO DE RESPUESTA REQUERIDO

Responde estructurado en estas fases:

FASE 0: DECISIONES CRÍTICAS Y PRINCIPIOS
- Arquitectura de información
- Principios de persuasión aplicados
- Jerarquía de conversión

FASE 1: ESTRUCTURA DE DATOS
- YAML maestro completo con campos
- Relaciones entre entidades
- Metadatos SEO por nivel

FASE 2: GENERACIÓN DE CONTENIDO
- Script Python exacto
- Front matter por tipo de página
- Lógica de slugs y URLs

FASE 3: PLANTILLAS HUGO (con persuasión integrada)
- Templates para cada tipo de página
- Partial para elementos de conversión
- Lógica de enlaces internos

FASE 4: SEO TÉCNICO AVANZADO
- Implementación de schema
- Sitemap y robots
- Canonical y jerarquía

FASE 5: MONETIZACIÓN INTELIGENTE
- Ubicación de enlaces afiliados
- Tablas comparativas persuasivas
- Shortcodes y automatización

FASE 6: ESTRATEGIA DE ESCALADO
- Priorización de contenido
- Expansión progresiva
- Mantenimiento automático

FASE 7: CHECKLIST DE IMPLEMENTACIÓN
- Pasos secuenciales
- Validación técnica
- Medición y optimización

INCLUYE EJEMPLOS CONCRETOS DE:
1. YAML real con datos de ejemplo
2. Front matter real de 3 tipos de páginas
3. URLs reales de la estructura completa
4. Snippets de Hugo templates (list.html / single.html / partials)
5. Snippet de Python para generar stubs
6. Ejemplos de schema JSON-LD con datos dinámicos
7. Ejemplo de tabla comparativa con CTA integrado
8. Estructura de shortcodes para afiliación

CRITERIOS DE CALIDAD MÁXIMA:
- Indexación masiva sin penalización
- Cero duplicados
- Alto CTR en resultados de búsqueda
- Enlazado interno perfecto y estratégico
- Escalabilidad extrema (10k+ páginas)
- Facilidad de mantenimiento
- Conversión optimizada en cada paso

DATOS ACTUALES:
[Incluye aquí cualquier código, estructura o datos que ya tengas desarrollados]

VERIFICACIÓN FINAL REQUERIDA:
1. Checklist final de implementación
2. Errores típicos y cómo evitarlos
3. Recomendación de "qué hacer primero mañana"
4. Métricas clave a monitorizar
5. Plan de expansión en fases

Te acabo de pegar todo lo desarrollado hasta el momento

Nota: El dump está filtrado y no incluye artefactos generados. Excluidos: public/, resources/, .hugo_build.lock, hugo_stats.json, .DS_Store, Thumbs.db, .vscode/, node_modules/, caches temporales.
"@

  $w.WriteLine($prompt)
  $w.WriteLine("")
}
finally {
  $w.Flush()
  $w.Dispose()
}

Write-Host ("OK -> " + $OutFile)
