# Datos de Google para Grow

`tools/growth_analytics.py` consulta en modo lectura los últimos 28 días
cerrados de Search Console y GA4. Deja tres días de margen porque Search
Console puede retrasar sus datos. No modifica Google, el catálogo ni las
ofertas.

Hay dos formas de autorizar la consulta: credenciales directas de Google
(cuenta de servicio, la opción permanente y gratuita) o una clave API de
Windsor.ai como alternativa heredada. Grow usa las credenciales de Google
cuando encuentra `GOOGLE_APPLICATION_CREDENTIALS` o
`GROWTH_GOOGLE_ACCESS_TOKEN`; solo recurre a Windsor.ai si no hay nada de
Google configurado.

Configuración permanente automatizada (recomendada, una sola vez):

```powershell
powershell -ExecutionPolicy Bypass -File tools\configurar_google_grow.ps1 -Login
```

Abre el navegador dos veces para autorizar tu cuenta de Google y después crea
el proyecto gratuito, habilita las APIs, crea la cuenta de servicio con su
clave JSON, fija `GOOGLE_APPLICATION_CREDENTIALS`, retira la clave antigua de
Windsor.ai y concede a la cuenta de servicio acceso de lectura en GA4 y Search
Console. Es idempotente: se puede repetir sin romper nada. Requiere
`gcloud` (Google Cloud SDK); si falta: `winget install Google.CloudSDK`.

Configuración manual equivalente:

1. En [Google Cloud Console](https://console.cloud.google.com/) crea un
   proyecto gratuito y habilita **Search Console API** y
   **Google Analytics Data API** (APIs y servicios → Biblioteca).
2. Crea una **cuenta de servicio** (IAM y administración → Cuentas de
   servicio), sin roles de proyecto, y descarga una **clave JSON**.
3. Da acceso de solo lectura al email de la cuenta de servicio
   (`...@...iam.gserviceaccount.com`): en Search Console, propiedad
   `recambios-aspirador.com` → Configuración → Usuarios y permisos →
   Añadir usuario (permiso «Restringido» basta); en GA4, Administrar →
   Gestión de acceso a la propiedad → rol «Lector».
4. Ejecuta `tools/guardar_credenciales_google.cmd`: pide la ruta del JSON
   descargado, lo guarda en `%USERPROFILE%\.growth\`, fija
   `GOOGLE_APPLICATION_CREDENTIALS` para tu usuario y retira la clave antigua
   de Windsor.ai.
5. Comprueba con `python tools/growth_analytics.py` que ambas fuentes
   devuelven datos.

Estas APIs de Google son gratuitas y la clave de la cuenta de servicio no
caduca, así que no hay pruebas ni suscripciones que renovar.

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

Las propiedades de este proyecto ya están fijadas como valores predeterminados
en ambos caminos: `sc-domain:recambios-aspirador.com` y GA4 `529119132`.
Puedes sobrescribirlas con `GROWTH_GSC_SITE_URL` y `GROWTH_GA4_PROPERTY_ID`,
pero normalmente solo hace falta `GOOGLE_APPLICATION_CREDENTIALS`.

Alternativa heredada Windsor.ai (solo si no hay credenciales de Google):
configura `GROWTH_WINDSOR_API_KEY` con `tools/guardar_clave_windsor.cmd`.
Requiere una suscripción activa de Windsor.ai; cuando la prueba o la
suscripción caduca, las consultas fallan, por eso la cuenta de servicio de
Google es la configuración recomendada. El trabajador consulta la API antes de
iniciar Codex y nunca pasa credenciales (ni de Google ni de Windsor) al
proceso Codex.

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
