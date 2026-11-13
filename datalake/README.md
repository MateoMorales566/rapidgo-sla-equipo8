# Data Lake - RapidGo SLA

Estructura local de capas tipo Data Lake. Los datos generados no se versionan
(ver `.gitignore`); solo se conservan los `.gitkeep` y este README.

## Capas

| Capa | Ruta | Contenido | Formato |
|---|---|---|---|
| Raw | `datalake/raw/` | Mensaje original de Kafka + topic, partición, offset, timestamp e `ingested_at` | Parquet |
| Processed | `datalake/processed/` | Evento parseado, timestamps, `delivery_minutes`, `minutes_late`, `sla_status`, `is_late`, `event_date` | Parquet |
| Curated KPIs | `datalake/curated/kpis/` | Agregación por ventana de 15 min y zona | Parquet |
| Curated alerts | `datalake/curated/alerts/` | `SLA_EXCEEDED` y `DELIVERY_STOPPED` | Parquet |

`processed` es la fuente del DAG de Airflow de la Semana 3.

## Checkpoints

Cada consulta de streaming usa su propio checkpoint:

```text
datalake/_checkpoints/raw
datalake/_checkpoints/processed
datalake/_checkpoints/curated
```

Los checkpoints permiten reiniciar sin releer offsets ya procesados. Si se eliminan, el
job vuelve a consumir desde `earliest` y puede duplicar datos. **Airflow no lee ni
modifica estos checkpoints**; solo lee `processed`.

## Quality gate de la Semana 3

El DAG `rapidgo_sla_semana3` valida:

- columnas requeridas: `event_id`, `team_id`, `order_id`, `status`, `zone`,
  `event_time`, `promised_at`;
- al menos una fila;
- cero `event_id` duplicados;
- cero filas inválidas (`RAPIDGO_MAX_INVALID_RATIO=0` por defecto).

Si el gate falla, no se publica reporte y las tareas `calcular_indicadores`,
`generar_reporte` y `publicar_resultado` quedan en `upstream_failed`. El detalle está en
[`docs/decisions/ADR-003-orquestacion-y-recuperacion.md`](../docs/decisions/ADR-003-orquestacion-y-recuperacion.md).

## Reprocesamiento

`raw` conserva el JSON original, por lo que se puede reconstruir `processed` y `curated`
sin volver a publicar en Kafka. Para un reprocesamiento completo:

1. Detener `spark-job`.
2. Respaldar o eliminar `processed`, `curated` y sus checkpoints.
3. Volver a ejecutar el job con `startingOffsets=earliest`.

## Datos de demo sin Spark

Para probar el DAG sin levantar Kafka/Spark:

```bash
python -m sla.sample_data \
  --input samples/sla_events_batch.json \
  --output datalake --overwrite
python -m sla.orchestration --lake datalake --reports reports
```

Esto escribe `datalake/processed/part-00000.parquet` con el mismo esquema enriquecido y
luego ejecuta las seis tareas en orden.

## Consulta rápida con PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()
kpis = spark.read.parquet("datalake/curated/kpis")
kpis.orderBy("ventana_inicio").show(50, truncate=False)

alerts = spark.read.parquet("datalake/curated/alerts")
alerts.groupBy("alert_code", "severity").count().show()
```

## Nota sobre Windows

La escritura Parquet local con Spark requiere `winutils.exe` y `HADOOP_HOME`. En
Docker/Linux no se necesita nada adicional. El generador `sla.sample_data` usa
`pyarrow` y sí funciona en Windows, por lo que sirve como fixture para la demo de
Airflow.
