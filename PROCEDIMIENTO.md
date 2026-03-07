FUENTE DE VERDAD
- data/catalog_parts.yaml
- data/catalog_brands.yaml
- data/catalog_skus.yaml

SALIDAS GENERADAS
- data/aspiradores.yaml
- content/*
- data/ofertas.yaml

PIPELINE ACTUAL
1. python tools/build_catalog.py
2. python tools/generar.py --force
3. python tools/sync_ofertas.py --no-cache --force

NOTAS
- build_catalog.py compila aspiradores.yaml
- generar.py crea/actualiza stubs Hugo desde aspiradores.yaml
- sync_ofertas.py actualiza ofertas.yaml desde aspiradores.yaml
- armageddon_catalog.py queda aparcado para futura unificación