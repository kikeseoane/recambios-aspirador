# Crecimiento autonomo diario

Windows inicia codex-growth.yml mediante workflow_dispatch diariamente a las
09:00 hora de Madrid (tarea Recambios-Codex-Growth-Daily, script
tools/start_growth_daily.ps1). Recupera el inicio perdido cuando vuelve a estar
disponible el PC; necesita sesion iniciada y red. El huso horario de Windows debe
seguir siendo Madrid. GitHub no tiene un segundo cron para evitar duplicados.
El marcador local daily-dispatch.txt evita repetir el lanzamiento en el mismo dia.
GitHub comparte concurrency.group: sync-ofertas con
la sincronizacion horaria, sin cancelacion y con cola. Ofertas no se ejecuta
mientras Codex trabaja. Los pushes humanos siguen siendo posibles: el publicador
hace fetch/rebase, valida otra vez y nunca fuerza pushes.

Ademas, la referencia Git `automation/growth-hold` mantiene las ofertas pausadas
entre dias. La rutina real la crea antes de solicitar trabajo al PC; el workflow
horario solo comprueba la pausa y omite su job de sincronizacion cuando existe.
No se dejan Actions ejecutandose durante 24 horas ni se modifica main para pausar.
Solo se retira tras terminar y validar/publicar la mejora. Fallos, falta de cuota
y PC apagado conservan la pausa; una prueba dry_run no la crea ni la retira.
Si hay una averia persistente, las ofertas seguiran pausadas hasta resolverla.

La tarea Windows Recambios-Codex-Growth consulta cada cinco minutos las peticiones
activas del workflow en main. No es un runner self-hosted: GitHub no puede mandar
comandos arbitrarios al PC. Ejecuta tools/growth_worker.py del checkout local.
El PC debe estar encendido, con sesion del usuario iniciada y acceso a red.
La peticion contiene run ID, commit exacto y modo prueba, no comandos ejecutables.

Codex trabaja en un clon separado bajo .growth-runtime/, sin tocar los cambios
locales del usuario. Entrega una rama temporal; GitHub controla el alcance,
compila Hugo, comprueba indexacion/CTAs y publica con commit y push a main.
Cloudflare mantiene el despliegue existente. NO se compila ni despliega Hugo local.

El prompt esta en docs/growth-prompt.md. Una mejora pequena por dia: guias,
reviews documentales sin fingir pruebas, comparativas, noticias justificadas,
enlazado o conversion. Prioriza utilidad y enlaces afiliados compatibles.
No hay cuota de articulos ni ingresos garantizados.

## Cuota y modelo

Usa solo la sesion ChatGPT existente, sin API de pago ni copiar credenciales.
Consulta account/rateLimits/read antes de empezar, cada 30 segundos y al terminar.
Ademas de la reserva, admite hasta 10 puntos porcentuales de consumo semanal por
dia (70 % restante -> 60 % restante). Identifica la ventana semanal por su duracion
de 10080 minutos; si no esta disponible, falla de forma segura sin lanzar el modelo.
Guarda el presupuesto en .growth-runtime/daily-budget.json. Reinicios y lanzamientos
manuales no lo reinician. El siguiente presupuesto comienza a las 07:00 UTC del dia
siguiente, antes del inicio diario tanto en verano como en invierno.
Un reinicio de cuota semanal tampoco da otros 10 puntos
en el mismo dia. Se cuenta el consumo observado de la cuenta, incluidas otras sesiones.
Si cualquier ventana tiene 20 % restante o menos, aplaza. Si no puede consultar
la cuota, se detiene y marca fallo. Es un porcentaje de uso del servicio, no un
contador de tokens. Es un control periodico, no atomico: llamadas en curso y
otras sesiones pueden consumir cuota entre comprobaciones.

Selecciona un modelo Luna disponible en model/list para la cuenta, razonamiento
medio; no cambia silenciosamente a una familia mas cara. La comprobacion inicial
encontro gpt-5.6-luna. Reevalua disponibilidad cada dia. Es un punto de partida
para mejoras acotadas, no un optimo demostrado.

Al consumir los 10 puntos, alcanzar la reserva o superar 18 minutos, guarda el parche
local y aplaza, dejando las ofertas pausadas. El siguiente
dia integra ese trabajo sobre main reciente con merge de tres vias. Si hay
conflicto no publica y conserva el checkpoint. Solo lo elimina tras confirmar
que GitHub publico correctamente. Si la cuota semanal sigue baja, vuelve a aplazar.

## Alertas y pruebas

El workflow de crecimiento envia Telegram ante fallos y tras publicar una mejora:
resume el cambio y motivo del registro, archivos afectados y enlace al commit.
No confirma el despliegue de Cloudflare, solo el push validado a main.
Pruebas sin cambios, registros sin mejora y cuota baja son silenciosos.
Si el PC no responde en 27 minutos, GitHub
avisa y termina la Action, pero conserva la pausa de ofertas; vuelve a intentarlo al dia siguiente.
Los logs y checkpoints locales estan en .growth-runtime/, excluidos de Git.
La sincronizacion de ofertas conserva sus avisos existentes.
La auditoria read-only growth-audit.yml puede ejecutarse en paralelo.

workflow_dispatch con dry_run=true prueba coordinacion y cuota sin llamar al modelo.
Los tests de los controles son:
- python -m unittest discover -s tools -p 'test_*growth.py'
- python -m unittest discover -s tools -p 'test_codex_quota.py'

Search Console y ventas AliExpress no estan conectados. Sin ellos, investiga
fuentes publicas actuales y declara limitaciones. affiliate_click mide salidas,
no compras; falta verificar recepcion en GA4. Revisar resultados a 28 dias.

Referencias:
- https://learn.chatgpt.com/docs/app-server
- https://developers.openai.com/api/docs/guides/model-selection
- https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax
