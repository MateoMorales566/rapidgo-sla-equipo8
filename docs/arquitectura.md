# Arquitectura específica - Variante 1: SLA de entregas

## Flujo

1. `producer/` genera eventos `delivery_status` con `team_id=equipo8`, `order_id` como
   identificador y `event_time` como tiempo del evento.
2. Kafka recibe los eventos en `rapidgo.sla.events`, con **3 particiones** y `order_id`
   como key. El topic lo crea el servicio `kafka-init` del compose.
3. Spark Structured Streaming valida el contrato, calcula la duración de la entrega,
   clasifica el cumplimiento y aplica un watermark de 2 minutos.
4. El Data Lake conserva tres capas:
   - `raw`: mensaje original de Kafka (`raw_value`) + topic, partición, offset y timestamp.
   - `processed`: evento parseado, timestamps, `delivery_minutes`, `minutes_late`,
     `sla_status` y `is_late`.
   - `curated/kpis`: agregación por ventana de 15 minutos y zona.
   - `curated/alerts`: `SLA_EXCEEDED` y `DELIVERY_STOPPED`.
5. Cada capa tiene su propio `checkpointLocation` en `datalake/_checkpoints/`.
6. Airflow validará calidad, agregará resultados y producirá reportes en la Semana 3.

## Componentes

| Componente | Archivo | Responsabilidad |
|---|---|---|
| Producer | `producer/producer.py` | Publica en Kafka con key `order_id` |
| Simulador | `producer/events.py` | Genera eventos válidos y lotes reproducibles |
| Reglas | `sla/rules.py` | KPIs y alertas sin Spark/Kafka |
| Contrato | `sla/contract.py` | Validación JSON Schema |
| Reporte | `sla/report.py` | KPIs y alertas de un lote |
| Transformaciones | `spark/transforms.py` | Parseo, enriquecimiento y agregaciones |
| Streaming | `spark/streaming_job.py` | `writeStream` a raw, processed y curated |
| Demo batch | `spark/batch_demo.py` | Mismas transformaciones sin Kafka |
| Infraestructura | `docker-compose.yml` | Kafka, topic, producer y Spark |

## Campos mínimos

| Campo | Uso |
|---|---|
| `team_id` | Identificar los eventos del equipo 8 |
| `order_id` | Identificar y particionar cada pedido |
| `zone` | Comparar desempeño por zona |
| `distance_km` | Contextualizar la duración |
| `promised_at` | Fin del SLA prometido |
| `picked_up_at` | Inicio operativo de la entrega |
| `delivered_at` | Fin real de la entrega (null si no se entregó) |
| `status` | Estado actual del pedido |

## Decisiones de diseño

- Todos los timestamps se transportan en UTC con formato ISO-8601.
- El tiempo de entrega se calcula como `delivered_at - picked_up_at`.
- El incumplimiento se calcula comparando `delivered_at` con `promised_at`.
- Los eventos sin `delivered_at` no se consideran entregas finalizadas; pueden activar
  `DELIVERY_STOPPED` cuando superan el umbral.
- La key `order_id` preserva el orden de los estados de un pedido.
- Las capas del Data Lake se mantienen separadas para conservar trazabilidad y permitir
  reprocesamiento.
- El job usa `local[2]` en el contenedor para que la demo sea reproducible; los servicios
  `spark-master`/`spark-worker` quedan disponibles para una ejecución en clúster.

## Consultas de verificación

```bash
# Topic y particiones
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:29092 \
  --describe --topic rapidgo.sla.events

# Archivos por capa
docker compose exec spark-job find /opt/rapidgo/datalake -name "*.parquet" | head

# Alertas generadas
docker compose exec spark-job ls /opt/rapidgo/datalake/curated/alerts
```
