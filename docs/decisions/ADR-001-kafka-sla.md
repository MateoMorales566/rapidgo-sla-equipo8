# ADR-001 - Key y particiones para eventos SLA

## Contexto / problema

Los eventos de un mismo pedido deben procesarse en orden para calcular
correctamente el estado de la entrega y determinar si incumplió el SLA.

## Opciones consideradas

1. Usar `zone` como key y una partición.
2. Usar `order_id` como key y tres particiones.
3. No definir key y dejar el reparto por defecto.

## Decisión tomada

Usar el topic `rapidgo.sla.events`, la key `order_id` y tres particiones.
Kafka mantendrá en la misma partición los eventos de un pedido, preservando el
orden relativo de sus estados; tres particiones permiten paralelismo inicial por
zona y carga sin sobredimensionar la entrega local.

## Evidencia utilizada

La variante exige eventos de pedido, timestamps, zona, distancia y estado. La
identidad del pedido es el elemento que necesita orden; la zona no garantiza
unicidad y una key ausente dificultaría la reconstrucción de estados.

## Consecuencias / trade-offs

- La distribución depende de la cardinalidad y frecuencia de `order_id`.
- Un pedido muy activo puede concentrar carga en una partición.
- Cambiar el número de particiones después puede alterar el reparto de nuevas
  keys; por eso se fija como decisión inicial y se revisará con métricas reales.
