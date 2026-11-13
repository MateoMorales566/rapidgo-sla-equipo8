# ADR-003 - Orquestación batch y recuperación de fallos

## Contexto / problema

La Semana 2 dejó Spark Structured Streaming escribiendo las capas `raw`, `processed` y
`curated`, con un checkpoint por capa. La Semana 3 exige un DAG de Airflow con mínimo 5
tareas, dependencias, retries, XCom, validación de datos, una tarea de calidad, una salida
de reporting y un fallo controlado con recuperación documentada.

El riesgo principal era acoplar Airflow al streaming: si el DAG reiniciara consultas,
borrara checkpoints o compitiera por los mismos offsets, se podrían duplicar datos o
perder trazabilidad.

## Opciones consideradas

1. **Controlar el streaming desde Airflow.** Un DAG que arranca y detiene
   `spark-submit`, o que reinicia consultas con `writeStream`. Da una sola interfaz, pero
   mezcla dos modelos de ejecución y puede tocar checkpoints desde un proceso batch.
2. **DAG batch bajo demanda sobre `processed` (decisión tomada).** Airflow lee el Parquet
   que ya escribió Spark, valida calidad, agrega KPIs y publica un reporte. No reinicia
   Spark ni modifica checkpoints.
3. **Solo streaming, sin Airflow.** Menos componentes, pero incumple el gate de la
   Semana 3 y no deja un punto único para calidad y reporte.

## Decisión tomada

- DAG `rapidgo_sla_semana3` con **6 tareas** encadenadas y `schedule=None`.
- `default_args` con `retries=2` y `retry_delay=30s`. La tarea `validar_calidad` usa
  `retries=0` a propósito: un fallo de calidad no se corrige reintentando, y así el fallo
  controlado termina con las tareas aguas abajo en `upstream_failed`.
- XCom transporta únicamente rutas, conteos y resultados pequeños; las filas se leen
  desde Parquet en cada tarea que las necesita.
- El quality gate exige columnas mínimas, al menos una fila, cero `event_id` duplicados y
  cero filas inválidas (`RAPIDGO_MAX_INVALID_RATIO=0` por defecto).
- El reporte se escribe de forma atómica con archivo temporal + `os.replace`.
- El fallo controlado se activa con `airflow/flags/forzar_fallo_calidad`; la recuperación
  consiste en retirar la bandera y lanzar un run nuevo.

## Evidencia utilizada

Ejecución real en Docker con el lote `samples/sla_events_batch.json`:

```text
Eventos: 120
Pedidos únicos: 30
Pedidos entregados: 25
Tiempo promedio: 36.56 min
Porcentaje fuera de SLA: 12.0 %
Pedidos retrasados: 3
Alertas SLA_EXCEEDED: 3
Alertas DELIVERY_STOPPED: 3
```

- Run exitoso: `manual__2026-11-01T00:00:00-05:00`; las 6 tareas en `success`.
- Run fallido: `manual__2026-11-06T00:00:00-05:00`; `validar_calidad` en `failed` y
  `calcular_indicadores`, `generar_reporte`, `publicar_resultado` en `upstream_failed`.
- Mensaje real del log:
  `RuntimeError: FALLO_CONTROLADO: bandera de calidad activa
  (/opt/airflow/flags/forzar_fallo_calidad). Bórrela y vuelva a ejecutar el DAG.`
- Recuperación: `manual__2026-11-07T00:00:00-05:00` en `success`, con el mismo reporte
  regenerado.

El detalle de comandos y estados está en
[`docs/evidencias.md`](../evidencias.md) y en
[`docs/checkpoints/semana3.md`](../checkpoints/semana3.md).

## Consecuencias / trade-offs

- Ventaja: separación clara entre streaming continuo y control batch; el DAG se puede
  repetir sin duplicar datos y sin tocar los checkpoints de Spark.
- Coste: un componente más (Airflow + pyarrow) y un volumen compartido para `datalake` y
  `reports`.
- El quality gate es estricto: una sola fila inválida bloquea el reporte. Para lotes
  reales con datos sucios habrá que ajustar `RAPIDGO_MAX_INVALID_RATIO` y documentar el
  cambio; el valor por defecto 0 es intencional para la demo.
- El 12 % fuera de SLA supera el objetivo inicial del 10 %. No se cambia el umbral en la
  Semana 3: se documenta como hallazgo de negocio y se calibrará por zona en la Semana 4.
- Las credenciales `admin/admin` y la `FERNET_KEY` del compose son solo para la demo
  local; no deben reutilizarse en producción.
