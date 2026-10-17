# Crecimiento autonomo diario

Windows inicia una ejecucion local diariamente a las 13:01 hora de Madrid
(tarea Recambios-Codex-Growth-Daily, script tools/start_growth_daily.ps1).
El trabajador prepara el cambio y solo entonces inicia `codex-growth.yml` para
validarlo y publicarlo. Recupera el inicio perdido cuando vuelve a estar
disponible el PC; necesita sesion iniciada y red. El huso horario de Windows debe
seguir siendo Madrid. GitHub no tiene un segundo cron para evitar duplicados ni
mantiene una Action esperando al equipo.
El marcador local daily-dispatch.txt evita repetir una ejecucion completada en el mismo dia.
Si la reserva de cinco horas baja del 20 %, Windows programa una unica tarea
`Recambios-Codex-Growth-Retry` para un minuto despues del reset exacto comunicado
por Codex. No marca el dia como completado; si vuelve a faltar cuota, actualiza
esa misma tarea para el siguiente reset, sin sondeo periodico.
Ambas tareas Windows permiten hasta dos horas para esperar la reserva de GitHub
y terminar el trabajo local; el agente limita cada mejora a 18 minutos.
GitHub comparte concurrency.group: sync-ofertas con
la sincronizacion horaria, sin cancelacion y con cola. Antes de clonar, el PC
solicita `growth-acquire.yml`, que entra en esa misma cola y crea el hold solo
cuando ya no hay una sincronizacion trabajando. Los siguientes sync ven el hold
y no empiezan. Ofertas no se ejecuta mientras Codex trabaja. Los pushes humanos siguen siendo posibles: el publicador
hace fetch/rebase, valida otra vez y nunca fuerza pushes.

Ademas, la referencia Git `automation/growth-hold` mantiene las ofertas pausadas
durante la mejora. El trabajador local la crea antes de empezar; el workflow
horario solo comprueba la pausa y omite su job de sincronizacion cuando existe.
No se dejan Actions ejecutandose durante 24 horas ni se modifica main para pausar.
Solo se retira tras terminar y validar/publicar una mejora pendiente. Una espera
por cuota o un fallo sin cambios libera la pausa; una mejora parcial la conserva.
Una prueba dry_run no la crea ni la retira.
Si hay una averia persistente, las ofertas seguiran pausadas hasta resolverla.

No hay tarea ni proceso que consulte GitHub cada cinco minutos. El proceso diario
ejecuta `tools/growth_once.py` una sola vez desde el checkout local. Al acabar,
empuja una rama temporal y solicita `workflow_dispatch` con esa rama y el commit
base exacto. GitHub rechaza cualquier resultado cuya base ya no sea `main`, valida
el parche permitido y lo publica. No es un runner self-hosted: GitHub no puede
mandar comandos arbitrarios al PC.

`tools/start_growth_worker.ps1 -Manual` conserva el lanzamiento manual local;
`-DryRun` comprueba el publicador remoto sin llamar a Codex. Sin `-Manual` sale
con código 2 deliberadamente, para que la antigua tarea de cinco minutos no
ejecute trabajo mientras se desactiva. El proceso diario y el manual devuelven
un código distinto de cero si Codex falla o no se puede solicitar la publicación.
El PC debe estar encendido, con sesion del usuario iniciada y acceso a red.
La publicacion recibe una rama temporal, su commit base exacto y el modo prueba,
no comandos ejecutables.

Codex trabaja en un clon separado bajo .growth-runtime/, sin tocar los cambios
locales del usuario. Entrega una rama temporal; GitHub controla el alcance,
compila Hugo, comprueba indexacion/CTAs y publica con commit y push a main.
Cloudflare mantiene el despliegue existente. NO se compila ni despliega Hugo local.
En este Windows el sandbox elevado falla durante su preparacion; el worker usa
el modo nativo `unelevated`, con `workspace-write` limitado al clon de trabajo.

El prompt esta en docs/growth-prompt.md. Una mejora pequena por dia: guias,
reviews documentales sin fingir pruebas, comparativas, noticias justificadas,
enlazado o conversion. Prioriza utilidad y enlaces afiliados compatibles.
No hay cuota de articulos ni ingresos garantizados.

## Cuota y modelo

Usa solo la sesion ChatGPT existente, sin API de pago ni copiar credenciales.
Consulta account/rateLimits/read antes de empezar, cada 30 segundos y al terminar.
No inicia ni continua con menos de 20 % restante en la ventana de cinco horas.
Guarda en `.growth-runtime/daily-budget.json` el gasto observado mientras Grow
esta activo: como maximo 10 puntos por dia y 70 puntos en una misma ventana
semanal (`weekly_resets_at`). Reinicios y lanzamientos manuales no lo reinician.
Al comenzar una ejecucion toma la cuota actual como base; el uso hecho fuera de
Grow entre ejecuciones no consume su presupuesto diario o semanal.
El siguiente presupuesto diario comienza a las 07:00 UTC; un reinicio semanal no
concede una segunda asignacion diaria. Si no se puede consultar la cuota, falla
de forma segura. Es un control periodico, no atomico: una llamada en curso u otra
sesion puede consumir cuota entre comprobaciones.

Selecciona un modelo Luna disponible en model/list para la cuenta, razonamiento
medio; no cambia silenciosamente a una familia mas cara. La comprobacion inicial
encontro gpt-5.6-luna. Reevalua disponibilidad cada dia. Es un punto de partida
para mejoras acotadas, no un optimo demostrado.

Al consumir los 10 puntos o superar 18 minutos, guarda el parche
local y aplaza hasta el dia siguiente. Si alcanza la reserva de cinco horas,
aplaza hasta un minuto despues de su reset. Una mejora parcial mantiene ofertas
pausadas; al reanudar integra el parche sobre main reciente con merge de tres vias. Si hay
conflicto no publica y conserva el checkpoint. Solo lo elimina tras confirmar
que GitHub publico correctamente. Si la cuota semanal sigue baja, vuelve a aplazar.

## Alertas y pruebas

El workflow de crecimiento envia Telegram ante fallos y tras publicar una mejora:
resume el cambio y motivo del registro, archivos afectados y enlace al commit.
No confirma el despliegue de Cloudflare, solo el push validado a main.
Pruebas sin cambios, registros sin mejora y cuota baja son silenciosos.
Si el trabajador local falla, no inicia una Action remota que espere al PC;
guarda el error y conserva el hold cuando hay una mejora pendiente. El siguiente
lanzamiento manual o diario reanuda ese checkpoint. Los logs y checkpoints locales estan en `.growth-runtime/`.
La sincronizacion de ofertas conserva sus avisos existentes.
La auditoria read-only growth-audit.yml puede ejecutarse en paralelo.

`tools/start_growth_worker.ps1 -Manual -DryRun` prueba coordinacion y el
publicador sin llamar al modelo.
Los tests de los controles son:
- python -m unittest discover -s tools -p 'test_*growth.py'
- python -m unittest discover -s tools -p 'test_codex_quota.py'

Grow consulta Search Console y GA4 en modo lectura cuando se configuran los
accesos indicados en `docs/growth-analytics.md`. Sin credenciales o permisos,
la auditoria declara que esas fuentes no estan disponibles y usa evidencia
publica actual. Ventas AliExpress no estan conectadas: `affiliate_click` mide
salidas, no compras. Revisar resultados a 28 dias.

Referencias:
- https://learn.chatgpt.com/docs/app-server
- https://developers.openai.com/api/docs/guides/model-selection
- https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax
