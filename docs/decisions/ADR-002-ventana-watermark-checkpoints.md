# ADR-002 - Ventana de 15 minutos, watermark de 2 minutos y checkpoints por capa

## Contexto / problema

El job inicial escribía Parquet con `df.write` sobre DataFrames de streaming, no tenía
`checkpointLocation`, no escribía `curated` y no implementaba las alertas. Además, el
lote de evidencia mostró que el 12 % de las entregas queda fuera del SLA y que el
promedio es 36.56 min, por lo que se necesita una agregación por ventana y capacidad de
reprocesar sin duplicar datos.

## Opciones consideradas

1. **Sin watermark ni ventanas**, escribiendo cada evento suelto. Simple, pero no permite
   calcular KPIs por franja ni descartar eventos demasiado tardíos.
2. **Watermark de 2 minutos + ventana fija de 15 minutos + checkpoint por capa**
   (decisión tomada). Permite KPIs por zona/franja y reinicios seguros.
3. **Ventana deslizante de 10 minutos**. Más suave para reportes, pero genera más estado y
   más filas para el volumen actual del proyecto.

## Decisión tomada

- Parsear desde Kafka y conservar el mensaje original en `raw`.
- Aplicar un **watermark de 2 minutos** sobre `event_time`.
- Agregar KPIs en **ventanas fijas de 15 minutos por zona** dentro de `foreachBatch`
  (así `percentile_approx` se puede usar sin las restricciones de streaming).
- Escribir una alerta única por pedido y micro-lote para `SLA_EXCEEDED` y
  `DELIVERY_STOPPED`.
- Usar **un checkpoint por capa**: `raw`, `processed` y `curated`.

## Evidencia utilizada

Ejecución del lote reproducible de 120 eventos (`producer/sla_events_batch.json`):

```text
Pedidos entregados: 25
Tiempo promedio de entrega: 36.56 min (objetivo <= 45 min)
Porcentaje fuera de SLA: 12.00% (objetivo <= 10%)
Pedidos retrasados: 3
Alertas SLA_EXCEEDED: 3
Alertas DELIVERY_STOPPED: 3 (umbral > 30 min)
```

La demo batch de Spark (`python -m spark.batch_demo`) procesó los mismos 120 eventos,
generó las ventanas de 15 minutos y devolvió exactamente 3 alertas de cada tipo. La suite
`pytest` valida los casos normal, límite e inválido de `clasificar_cumplimiento` y las
dos reglas de alerta (38 pruebas en verde).

## Consecuencias / trade-offs

- El 12 % fuera de SLA supera el objetivo inicial del 10 %: **no se cambia el umbral
  todavía**; se documenta como hallazgo y se calibrará por zona con más datos.
- Los eventos con más de 2 minutos de retraso no entran en las agregaciones streaming,
  pero **permanecen en `raw`** y se pueden reprocesar.
- `curated` puede contener una fila de alerta por micro-lote si un pedido sigue detenido;
  el reporte de lote deduplica por el último estado de cada pedido.
- Los checkpoints permiten reiniciar sin releer offsets ya procesados; si se borra un
  checkpoint, el job vuelve a consumir desde `earliest` y puede duplicar archivos.
