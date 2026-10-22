# Arquitectura específica - Variante 1: SLA de entregas

## Flujo

1. `producer/` genera eventos de pedidos con `order_id` como identificador y
   `event_time` como tiempo del evento.
2. Kafka recibe los eventos en `rapidgo.sla.events`, con tres particiones y
   `order_id` como key.
3. Spark Structured Streaming valida el contrato, calcula la duración de
   entrega y clasifica el cumplimiento del SLA.
4. El Data Lake conserva tres capas: `raw` (evento original), `processed`
   (evento validado y enriquecido) y `curated` (KPIs y alertas).
5. Airflow validará calidad, agregará resultados y producirá reportes en una
   semana posterior.

## Campos mínimos

| Campo | Uso |
|---|---|
| `order_id` | Identificar y particionar cada pedido |
| `zone` | Comparar desempeño por zona |
| `distance_km` | Contextualizar la duración |
| `promised_at` | Fin del SLA prometido |
| `picked_up_at` | Inicio operativo de la entrega |
| `delivered_at` | Fin real de la entrega |
| `status` | Estado actual del pedido |

## Decisiones de diseño

- Todos los timestamps se transportan en UTC con formato ISO-8601.
- El tiempo de entrega se calcula como `delivered_at - picked_up_at`.
- El incumplimiento se calcula comparando `delivered_at` con `promised_at`.
- Los eventos sin `delivered_at` no se consideran entregas finalizadas; pueden
  activar la alerta de detención cuando superen el umbral definido.
- Las capas del Data Lake se mantienen separadas para conservar trazabilidad y
  permitir reprocesamiento.

## Componentes iniciales

La composición preliminar está en [`../docker-compose.yml`](../docker-compose.yml).
El diagrama visual está en [`arquitectura.svg`](arquitectura.svg).
El productor, el job de Spark y el DAG de Airflow se implementarán en las
semanas indicadas por la guía.
