---
title: "Mi aspirador no enciende: diagnostico paso a paso"
description: "Que hacer cuando tu aspirador escoba no arranca. Bateria, cargador, gatillo, placa y motor: como descartar cada causa."
slug: aspirador-no-enciende
weight: 100
type: guia
guideKey: no-enciende
intent: "diagnostico"
---

## Paso 1: Verifica la carga

El 70% de los casos de "no enciende" son problema de carga:

1. **Conecta el cargador** y observa el LED:
   - LED azul fijo o verde: esta cargando normalmente
   - LED parpadea rapido en rojo: fallo de bateria o temperatura
   - Sin LED: el cargador no llega o esta danado

2. **Espera 3 horas** de carga completa antes de volver a probar

3. **Prueba otro enchufe**: descarta que el problema sea la toma de corriente

## Paso 2: Inspecciona el cargador

- Mira si el cable tiene cortes, dobleces o el conector esta suelto
- Si usas base/dock: comprueba que los contactos metalicos no estan oxidados ni sucios
- Prueba con otro cargador del mismo modelo si tienes acceso

**Diagnostico**: si con otro cargador funciona, el problema es el cargador → sustituir.

## Paso 3: Revisa la bateria

- Retira la bateria y vuelvela a insertar (a veces un mal contacto es todo)
- Revisa los contactos: limpia con un bastoncillo seco si estan sucios
- Si la bateria esta hinchada (abultada), **no la uses** → recicla inmediatamente

**Diagnostico**: si la bateria tiene mas de 3 anos y 0 LED al cargar, probablemente esta agotada → sustituir.

## Paso 4: Comprueba el gatillo o boton

- Pulsa varias veces con firmeza
- En aspiradores con gatillo: escucha si hace "clic" mecanico
- Si el clic no se siente: puede estar roto internamente

**Diagnostico**: si el clic se siente pero no pasa nada, el problema es la placa o motor, no el gatillo.

## Paso 5: Descartar bloqueo de seguridad

Muchos aspiradores no encienden si:
- El deposito no esta correctamente encajado
- El filtro esta fuera de posicion
- El cabezal no esta conectado (en algunos modelos)

Revisa que todas las piezas estan en su sitio hasta escuchar el clic de encaje.

## Paso 6: Problema de placa electronica

Si has descartado todo lo anterior:
- El aspirador no responde a nada: ni LED ni ruido ni vibracion
- Posible fallo en la PCB (placa de control)
- **No reparable en casa** sin conocimientos de electronica

## Arbol de decision

```
No enciende
├── LED del cargador NO se ilumina → Cargador danado → Sustituir cargador
├── LED del cargador OK pero no carga → Bateria agotada → Sustituir bateria
├── Carga OK pero no arranca al pulsar
│   ├── Clic del gatillo NO se siente → Gatillo roto → Reparacion
│   ├── Clic del gatillo OK
│   │   ├── Alguna pieza suelta → Recolocar deposito/filtro/cabezal
│   │   └── Todo en sitio → Placa/motor danado → Valorar sustitucion
└── Bateria hinchada → No usar → Reciclar + sustituir bateria
```

## Cuando no compensa reparar

- Aspiradores de mas de 5 anos con motor danado
- Coste de bateria + cargador supera el 60% del precio de uno nuevo
- Modelo descatalogado sin piezas disponibles

## Siguiente paso

Si has identificado la pieza que falla (bateria o cargador), busca tu modelo en el catalogo para ver opciones compatibles.
