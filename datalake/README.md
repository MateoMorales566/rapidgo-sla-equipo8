# Data Lake - RapidGo SLA

Estructura local de capas tipo Data Lake. Los datos generados no se versionan
(ver `.gitignore`); solo se conservan los `.gitkeep` y este README.

## Capas

| Capa | Ruta | Contenido | Formato |
|---|---|---|---|
| Raw | `datalake/raw/` | Mensaje original de Kafka + topic, partición, offset, timestamp e `ingested_at` | Parquet |
| Processed | `datalake/processed/` | Evento parseado, timestamps, `delivery_minutes`, `minutes_late`, `sla_status`, `is_late` | Parquet |
| Curated KPIs | `datalake/curated/kpis/` | Agregación por ventana de 15 min y zona | Parquet |
| Curated alerts | `datalake/curated/alerts/` | `SLA_EXCEEDED` y `DELIVERY_STOPPED` | Parquet |

## Checkpoints

Cada consulta de streaming usa su propio checkpoint:

```text
datalake/_checkpoints/raw
datalake/_checkpoints/processed
datalake/_checkpoints/curated
```

Los checkpoints permiten reiniciar sin releer offsets ya procesados. Si se eliminan, el
job vuelve a consumir desde `earliest` y puede duplicar datos.

## Reprocesamiento

`raw` conserva el JSON original, por lo que se puede reconstruir `processed` y `curated`
sin volver a publicar en Kafka. Para un reprocesamiento completo:

1. Detener `spark-job`.
2. Respaldar o eliminar `processed`, `curated` y sus checkpoints.
3. Volver a ejecutar el job con `startingOffsets=earliest`.

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

La escritura Parquet local en Windows requiere `winutils.exe` y `HADOOP_HOME`. En
Docker/Linux no se necesita nada adicional. Para pruebas locales sin escribir archivos,
usar `python -m spark.batch_demo` sin `--write`.
