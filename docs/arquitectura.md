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
5. Cada capa de streaming tiene su propio `checkpointLocation` en
   `datalake/_checkpoints/`.
6. **Airflow** ejecuta un DAG batch bajo demanda (`schedule=None`) sobre `processed`:
   valida fuente, perfila datos, aplica quality gate, calcula KPIs, escribe un reporte
   atómico y publica un resumen. No reinicia Spark ni toca checkpoints.
7. La Semana 4 integrará el quality gate final y el release `v1.0.0`.

```text
producer -> Kafka (3 particiones, key=order_id)
  -> Spark Structured Streaming
       -> raw -> processed -> curated/kpis + curated/alerts
  -> Airflow (batch, schedule=None)
       -> validar_fuente -> perfilar_datos -> validar_calidad
       -> calcular_indicadores -> generar_reporte -> publicar_resultado
  -> reports/sla_semana3_latest.json
```

## Componentes

| Componente | Archivo | Responsabilidad |
|---|---|---|
| Producer | `producer/producer.py` | Publica en Kafka con key `order_id` |
| Simulador | `producer/events.py` | Genera eventos válidos y lotes reproducibles |
| Reglas | `sla/rules.py` | KPIs y alertas sin Spark/Kafka |
| Contrato | `sla/contract.py` | Validación JSON Schema |
| Reporte de lote | `sla/report.py` | KPIs y alertas de un lote JSON |
| Transformaciones | `spark/transforms.py` | Parseo, enriquecimiento y agregaciones |
| Streaming | `spark/streaming_job.py` | `writeStream` a raw, processed y curated |
| Demo batch Spark | `spark/batch_demo.py` | Mismas transformaciones sin Kafka |
| DAG | `airflow/dags/rapidgo_sla_orchestration.py` | Conecta las 6 tareas de Airflow |
| Configuración | `semana3/config.py` | Rutas `RAPIDGO_*` y umbral de calidad |
| Calidad | `semana3/quality.py` | Perfilado Parquet y quality gate |
| Reporting | `semana3/reporting.py` | KPIs, alertas y escritura atómica |
| Orquestación | `semana3/orchestration.py` | Funciones de las tasks y runner local |
| Fixture | `semana3/sample_data.py` | Genera `processed` Parquet para demo/CI |
| Infraestructura | `docker-compose.yml` | Kafka, topic, producer y Spark |
| Airflow | `docker-compose.semana3.yml` + `airflow/Dockerfile` | Airflow 2.9.3 + pyarrow |

## Campos mínimos

| Campo | Uso |
|---|---|
| `team_id` | Identificar los eventos del equipo 8 |
| `event_id` | Detectar duplicados en el quality gate |
| `order_id` | Identificar y particionar cada pedido |
| `zone` | Comparar desempeño por zona |
| `distance_km` | Contextualizar la duración |
| `promised_at` | Fin del SLA prometido |
| `picked_up_at` | Inicio operativo de la entrega |
| `delivered_at` | Fin real de la entrega (null si no se entregó) |
| `status` | Estado actual del pedido |

## Reglas de calidad de la Semana 3

El gate falla si:

- falta una columna requerida (`event_id`, `team_id`, `order_id`, `status`, `zone`,
  `event_time`, `promised_at`);
- no hay filas;
- hay `event_id` duplicados;
- hay filas inválidas (estado, zona, `team_id`, timestamps o `delivery_minutes`).

El umbral de filas inválidas es `0` por defecto y se puede cambiar con
`RAPIDGO_MAX_INVALID_RATIO` (0 a 1). Un fallo deja las tareas aguas abajo en
`upstream_failed`; no se publica un reporte parcial.

## Decisiones de diseño

- Todos los timestamps se transportan en UTC con formato ISO-8601.
- El tiempo de entrega se calcula como `delivered_at - picked_up_at`.
- El incumplimiento se calcula comparando `delivered_at` con `promised_at`.
- Los eventos sin `delivered_at` no se consideran entregas finalizadas; pueden activar
  `DELIVERY_STOPPED` cuando superan el umbral.
- La key `order_id` preserva el orden de los estados de un pedido.
- Las capas del Data Lake se mantienen separadas para conservar trazabilidad y permitir
  reprocesamiento.
- Airflow transporta por XCom solo rutas y conteos; las filas se leen de Parquet.
- El job de Spark usa `local[2]` en el contenedor; `spark-master`/`spark-worker` quedan
  disponibles para una ejecución en clúster.

## Consultas de verificación

```bash
# Topic y particiones
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:29092 \
  --describe --topic rapidgo.sla.events

# Archivos por capa
docker compose exec spark-job find /opt/rapidgo/datalake -name "*.parquet" | head

# DAG y runs
docker compose -f docker-compose.yml -f docker-compose.semana3.yml \
  exec -T airflow airflow dags list
docker compose -f docker-compose.yml -f docker-compose.semana3.yml \
  exec -T airflow airflow dags list-runs -d rapidgo_sla_semana3

# Reporte generado
cat reports/sla_semana3_latest.json
```
