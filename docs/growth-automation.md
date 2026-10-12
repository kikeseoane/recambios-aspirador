# Rutina diaria

El workflow growth-audit.yml audita cobertura cada dia a las 06:30 UTC y guarda
un informe como artefacto de GitHub Actions. No invoca Codex ni cambia el sitio.

## Codex

Esta sesion no dispone de una herramienta para registrar tareas en la app.
La tarea Codex queda pendiente de activacion en Scheduled: cada dia a las 09:00
Europe/Madrid, en un worktree de este proyecto, con este prompt:

> Lee .codex/skills/affiliate-growth/SKILL.md y ejecuta la rutina diaria.
> Investiga recambios nuevos y demanda con fuentes actuales. Prioriza paginas
> con trafico y ofertas compatibles. Implementa una mejora respaldada por datos,
> valida el sitio y entrega el diff y los resultados. No hagas commit ni push en
> esta fase inicial. Informa de datos o accesos ausentes. No ejecutes sync_ofertas.py.

El ordenador debe estar encendido y la app abierta. Tras revisar las primeras
ejecuciones se puede ampliar el prompt con autorizacion de publicacion.
Fuente: https://learn.chatgpt.com/docs/automations

## Medicion

- Search Console: exportaciones recientes con periodo conocido; el acceso
  automatico requiere conectar la propiedad con permisos de lectura.
- GA4: evento affiliate_click con SKU, categoria, modelo y ubicacion.
  Crear dimensiones personalizadas para esos parametros si se necesitan
  desgloses. La recepcion real en GA4 queda pendiente de verificar en DebugView.
- AliExpress: pedidos, comision aprobada y devoluciones. GA4 mide salidas,
  no compras externas. La atribucion por pagina requiere identificadores
  admitidos por afiliacion y su informe; esta conexion no esta implementada.

Compara periodos de 28 dias: clics organicos, tasa de salida afiliada,
pedidos/clics y comision neta descontando automatizacion. No hay ingresos
garantizados. Los datos locales historicos suman 525 clics por dispositivo,
400 desde movil. Los 531 clics por pagina no se suman a los anteriores.

## Prioridades iniciales

Xiaomi G20/G11 y Rowenta X-Force muestran demanda en los CSV historicos.
Investigar primero sus referencias y variantes antes de incorporar mas modelos.
Las ofertas locales incluyen 927 compatible_alto, 296 dudosos y 559
fallback_buy_new. Son clasificaciones automaticas, no verificaciones manuales.

Paginas huerfanas: pendiente de auditoria completa del grafo.
Enlaces recomendados: marca -> modelo -> categorias con oferta aceptada;
problema -> recambio relacionado disponible.
Anchors sugeridos: filtro para Xiaomi G11, bateria para Xiaomi G20 Lite,
comprobaciones si el cepillo Rowenta no gira.
