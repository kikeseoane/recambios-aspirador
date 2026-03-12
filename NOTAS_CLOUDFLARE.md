# Redirect Rules de Cloudflare

Objetivo: dejar una sola version canonica del sitio en `https://recambios-aspirador.com/` y evitar duplicados entre HTTP/HTTPS y `www`/sin `www`.

## Reglas 301

1. HTTP -> HTTPS
   Expresion recomendada:
   `http.request.full_uri starts_with "http://"`

   Accion:
   `Static Redirect`

   URL destino:
   `https://recambios-aspirador.com${http.request.uri.path}`

   Preserve query string:
   `On`

   Status code:
   `301`

2. `www` -> host canonico sin `www`
   Expresion recomendada:
   `http.host eq "www.recambios-aspirador.com"`

   Accion:
   `Static Redirect`

   URL destino:
   `https://recambios-aspirador.com${http.request.uri.path}`

   Preserve query string:
   `On`

   Status code:
   `301`

## Pruebas con PowerShell

Robots en host canonico:

```powershell
iwr -Method Head https://recambios-aspirador.com/robots.txt
```

Sitemap en host canonico:

```powershell
iwr -Method Head https://recambios-aspirador.com/sitemap.xml
```

Robots en `www`:

```powershell
iwr -Method Head https://www.recambios-aspirador.com/robots.txt
```

Sitemap en `www`:

```powershell
iwr -Method Head https://www.recambios-aspirador.com/sitemap.xml
```

HTTP sin `www`:

```powershell
iwr -Method Head http://recambios-aspirador.com/robots.txt
iwr -Method Head http://recambios-aspirador.com/sitemap.xml
```

## Resultado esperado

- `https://recambios-aspirador.com/robots.txt` -> `200`
- `https://recambios-aspirador.com/sitemap.xml` -> `200`
- variantes `http://` y `https://www.` -> `301` hacia `https://recambios-aspirador.com/...`
