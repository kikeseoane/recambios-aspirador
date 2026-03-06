# Procedimiento para regenerar todo el proyecto

## 1️⃣ Si has editado `aspiradores.yaml`

### Validar catálogo

python tools/armageddon_catalog.py --validate

### Generar páginas Hugo

python tools/armageddon_catalog.py --generate-stubs --force

### Buscar enlaces de afiliado

python tools/armageddon_catalog.py --sync-offers --no-cache
--force-lookup

### Probar la web

hugo server -D

### Subir cambios

git add . git commit -m "update catalog" git push

------------------------------------------------------------------------

# 2️⃣ Si has editado `catalog_brands.yaml`, `catalog_parts.yaml` o `catalog_skus.yaml`

Primero hay que reconstruir el catálogo.

### Construir aspiradores.yaml

python tools/build_catalog.py

### Generar todo

python tools/armageddon_catalog.py --all --force --no-cache
--force-lookup

### Probar

hugo server -D

### Subir

git add . git commit -m "rebuild catalog" git push

------------------------------------------------------------------------

# 3️⃣ Comando rápido (todo automático)

python tools/armageddon_catalog.py --all --force --no-cache
--force-lookup

Esto ejecuta: - validate - generate-stubs - sync-offers

------------------------------------------------------------------------

# 4️⃣ Si quieres rehacer solo los enlaces afiliados

python tools/sync_ofertas.py --no-cache

Para un SKU concreto:

python tools/sync_ofertas.py --only-sku dyson-v11-filter

------------------------------------------------------------------------

# 5️⃣ Resultado del proceso

Se actualizan:

-   content/modelos/\*
-   data/ofertas.yaml

Cuando haces:

git push

Cloudflare Pages ejecuta:

hugo --minify

y la web queda publicada.

------------------------------------------------------------------------

# Regla rápida

-   Si cambias aspiradores.yaml → armageddon_catalog
-   Si cambias catalog\_\* → build_catalog primero
-   sync_ofertas → solo enlaces afiliados
