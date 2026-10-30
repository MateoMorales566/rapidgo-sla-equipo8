# Evidencias de ejecución - Semana 2

Rama: `feature/semana2-pipeline-ci`  
Fecha: 2026-10-29

## 1. Pruebas y sintaxis

```text
$ python -m pytest -q
......................................                                   [100%]
38 passed in 0.22s

$ python -m compileall -q producer spark sla tests
exit 0
```

## 2. Contrato JSON

```text
$ python -m sla.contract samples/sla_event.json producer/sla_event_sample.json producer/sla_events_batch.json
OK samples/sla_event.json: 1 evento(s) válido(s)
OK producer/sla_event_sample.json: 1 evento(s) válido(s)
OK producer/sla_events_batch.json: 120 evento(s) válido(s)
```

## 3. KPIs y alertas del lote de 120 eventos

Comando:

```text
python -m producer.producer --dry-run --count 120 --interval 300 --seed 8 \
  --start-time 2026-10-29T12:00:00Z --output producer/sla_events_batch.json
python -m sla.report producer/sla_events_batch.json
```

Salida:

```text
Eventos analizados: 120
Pedidos entregados: 25
Tiempo promedio de entrega: 36.56 min (objetivo <= 45 min)
Porcentaje fuera de SLA: 12.00% (objetivo <= 10%)
Pedidos retrasados: 3
Alertas SLA_EXCEEDED: 3
Alertas DELIVERY_STOPPED: 3 (umbral > 30 min)
Eventos inválidos descartados: 0
Reloj de referencia: 2026-10-29 14:18:00+00:00
```

Detalle de las seis alertas:

```text
DELIVERY_STOPPED order-20261029-00012 zona=Norte 60.0 min
DELIVERY_STOPPED order-20261029-00021 zona=Norte 60.0 min
DELIVERY_STOPPED order-20261029-00030 zona=Oeste 60.0 min
SLA_EXCEEDED     order-20261029-00010 zona=Centro 29.0 min
SLA_EXCEEDED     order-20261029-00023 zona=Norte  32.0 min
SLA_EXCEEDED     order-20261029-00024 zona=Centro 20.0 min
```

## 4. Spark batch sobre el mismo lote

Comando (requiere JDK 17 y `requirements-spark.txt`):

```text
python -m spark.batch_demo --input producer/sla_events_batch.json
```

Salida resumida:

```text
Eventos válidos: 120
Reloj de referencia: 2026-10-29 09:18:00
KPIs por ventana de 15 minutos: (tabla con ventana_inicio, ventana_fin, zone,
  eventos, pedidos_entregados, pedidos_en_curso, tiempo_promedio_min,
  p95_entrega_min, pedidos_retrasados, porcentaje_fuera_sla)
Alertas: 3 SLA_EXCEEDED + 3 DELIVERY_STOPPED
```

Ejemplo de ventana:

```text
2026-10-29 08:00:00 | 2026-10-29 08:15:00 | Oeste | 7 | 2 | 5 | 19.5 | 24.0 | 0 | 0.0
2026-10-29 08:15:00 | 2026-10-29 08:30:00 | Este  | 3 | 1 | 2 | 27.0 | 27.0 | 0 | 0.0
```

> La escritura Parquet local en Windows requiere `winutils.exe`; en Linux/Docker la
> escritura en `datalake/curated` funciona sin pasos adicionales. En esta evidencia se
> validaron las transformaciones en batch; el job streaming está preparado para escribir
> las tres capas con `writeStream` y `checkpointLocation`.

## 5. Docker Compose

```text
$ docker compose config --quiet
exit 0
```

El compose define:

- `kafka-init` crea `rapidgo.sla.events` con 3 particiones.
- `producer` publica con key `order_id`.
- `spark-job` ejecuta `spark-submit` con el conector Kafka.

## 6. Comandos para la demo end-to-end

```powershell
docker compose up -d zookeeper kafka kafka-init
docker compose run --rm kafka-init
docker compose up -d producer spark-job

docker compose exec kafka kafka-topics --bootstrap-server localhost:29092 `
  --describe --topic rapidgo.sla.events

docker compose logs -f spark-job
```

Evidencia esperada: 3 particiones en el topic, eventos del producer, archivos Parquet en
`datalake/raw`, `datalake/processed`, `datalake/curated/kpis` y
`datalake/curated/alerts`.
