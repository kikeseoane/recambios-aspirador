# Datos de Google para Grow

`tools/growth_analytics.py` consulta en modo lectura los últimos 28 días
cerrados de Search Console y GA4. Deja tres días de margen porque Search
Console puede retrasar sus datos. No modifica Google, el catálogo ni las
ofertas.

Hay dos formas de autorizar la consulta: una clave API de Windsor.ai, si GSC y
GA4 ya están conectados allí, o credenciales directas de Google. Grow usa
Windsor.ai cuando encuentra `GROWTH_WINDSOR_API_KEY`; si no, usa Google.

El trabajador local consulta ambas API antes de iniciar Codex. Guarda una copia
sin credenciales dentro de los metadatos Git del checkout aislado y
`tools/growth_audit.py` la incluye en su informe. Esa copia no se publica.

Configura estas variables en el entorno del usuario que ejecuta la tarea
programada de Grow; nunca las incluyas en Git:

```text
GROWTH_GSC_SITE_URL=sc-domain:recambios-aspirador.com
GROWTH_GA4_PROPERTY_ID=529119132
GOOGLE_APPLICATION_CREDENTIALS=C:\ruta\privada\cuenta-servicio.json
```

Para usar las cuentas ya conectadas de Windsor.ai, configura solo
`GROWTH_WINDSOR_API_KEY` en el entorno del usuario de la tarea programada.
Las propiedades de este proyecto ya están fijadas como valores predeterminados:
`sc-domain:recambios-aspirador.com` y GA4 `529119132`. Puedes sobrescribirlas
con `GROWTH_GSC_SITE_URL` y `GROWTH_GA4_PROPERTY_ID`. Obtén la clave en
**Windsor.ai → Account Management → API Access** o en las instrucciones de
**Data Preview → Python/R**, e introdúcela directamente en el PC;
no la pegues en un chat ni la guardes en Git. El trabajador consulta la API
antes de iniciar Codex y no le pasa la clave al proceso Codex.

La cuenta de servicio necesita acceso de lectura a la propiedad de Search
Console y rol Viewer en GA4. El proyecto Google Cloud debe tener habilitadas
Search Console API y Google Analytics Data API. `google-auth` está incluido en
`requirements.txt` para renovar el token automáticamente. También se puede
usar `GROWTH_GOOGLE_ACCESS_TOKEN` con los ámbitos `webmasters.readonly` y
`analytics.readonly`, pero ese token caduca y no sirve como configuración
permanente de la tarea diaria.

`GROWTH_GA4_PROPERTY_ID` es el ID **numérico** de la propiedad, no el ID de
medición `G-8DB4EDP0C7` que aparece en la web. El JSON de la cuenta de servicio
debe permanecer fuera del repositorio y de `.growth-runtime`.

Ejemplos:

```powershell
python tools/growth_analytics.py
python tools/growth_analytics.py --output .growth-runtime/google-analytics.md
python tools/growth_analytics.py --json
```

Cuando falta una variable, la credencial, el paquete opcional o el permiso, el
programa termina correctamente e indica que la fuente no está disponible. No
convierte ese caso en tráfico cero ni inventa métricas. Search Console aporta
páginas, clics, impresiones, CTR y posición; GA4 aporta sesiones, vistas por
página y el evento `affiliate_click` si existe.

Referencias oficiales: [Search Analytics query](https://developers.google.com/webmaster-tools/v1/searchanalytics/query), [autorizacion Search Console](https://developers.google.com/webmaster-tools/v1/how-tos/authorizing), [GA4 runReport](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport) y [inicio rapido GA4](https://developers.google.com/analytics/devguides/reporting/data/v1/quickstart).
