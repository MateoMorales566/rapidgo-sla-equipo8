# RapidGo SLA - Equipo 8

Proyecto final de Big Data - **Variante 1: SLA de entregas**.

Pipeline end-to-end para capturar eventos de pedidos, calcular el cumplimiento del SLA,
persistir las capas del Data Lake y generar alertas operativas.

## Equipo

| Integrante | Responsabilidad |
|---|---|
| Andrés Morales | Productor de eventos y contrato JSON |
| Julián Pérez | Kafka, Spark Structured Streaming y Data Lake |
| Kevin García | KPIs, alertas, pruebas, CI y documentación |

Handles de GitHub: `MateoMorales566`, `NyawToo`, `SantiagoG1309`.

## Problema de negocio

RapidGo promete un tiempo de entrega por pedido. El equipo necesita saber:

- cuánto tarda realmente una entrega;
- qué porcentaje se sale del SLA;
- qué pedidos se retrasan o quedan detenidos demasiado tiempo.

La solución captura eventos `delivery_status`, los procesa en tiempo real y deja KPIs y
alertas listas para consulta y para la orquestación de la Semana 3.

## Estado de los releases

| Release | Semana | Estado |
|---|---|---|
| `v0.1.0-foundation` | 1 | ✅ Publicado |
| `v0.2.0-streaming` | 2 | ✅ Implementado y validado en CI en la rama `feature/semana2-pipeline-ci`; el tag se crea desde `main` después del merge |
| `v0.3.0-orchestration` | 3 | ✅ Implementado y validado en la rama `feature/semana3-orquestacion`; sin merge a `main` |
| `v1.0.0` | 4 | ⏳ Release final y video |

## Arquitectura

```text
producer/ (confluent-kafka, key=order_id, team_id=equipo8)
  -> Kafka topic rapidgo.sla.events (3 particiones)
    -> Spark Structured Streaming
      -> datalake/raw        (mensaje original + metadatos Kafka)
      -> datalake/processed  (evento parseado y enriquecido)
      -> datalake/curated/
           kpis/             (ventana 15 min por zona)
           alerts/           (SLA_EXCEEDED, DELIVERY_STOPPED)
      -> Airflow (batch bajo demanda: calidad, KPIs y reporte)
           validar_fuente -> perfilar_datos -> validar_calidad
           -> calcular_indicadores -> generar_reporte -> publicar_resultado
      -> reports/sla_semana3_latest.json
```

Diagrama y decisiones: [`docs/arquitectura.md`](docs/arquitectura.md),
[`docs/arquitectura.svg`](docs/arquitectura.svg) y
[`docs/decisions/ADR-001-kafka-sla.md`](docs/decisions/ADR-001-kafka-sla.md).

## Tecnologías y rol

| Tecnología | Rol |
|---|---|
| Docker Compose | Levanta Zookeeper, Kafka, Spark y el producer |
| Apache Kafka | Transporte de eventos, topic de 3 particiones y key `order_id` |
| Spark Structured Streaming | Parseo, enriquecimiento, ventanas, KPIs y alertas |
| Parquet + Data Lake | Persistencia por capas y reprocesamiento |
| Python + `confluent-kafka` | Generación y publicación de eventos |
| Apache Airflow 2.9.3 | Orquestación batch: quality gate, KPIs y reporte (Semana 3) |
| pyarrow | Lectura Parquet en Airflow y fixture de demo |
| pytest | Pruebas de reglas, contrato, generador, calidad y orquestación |
| GitHub Actions | CI en push, PR y ejecución manual |

## Estructura del repositorio

```text
.github/workflows/ci.yml        CI: sintaxis, contrato, pytest, Airflow y compose
.github/workflows/release.yml   Release a partir de tags v*.*.*
airflow/dags/                   DAG rapidgo_sla_semana3
airflow/Dockerfile              Imagen Airflow + pyarrow
datalake/                       Capas raw, processed y curated
docs/                           Arquitectura, KPIs, ADR, checkpoints y evidencias
producer/                       Generador de eventos y cliente Kafka
semana3/                        Paquete de orquestación, calidad, reporting y fixture
sla/                            Reglas puras, contrato y reporte de KPIs
spark/                          Job streaming y demo batch
tests/                          Pruebas automatizadas
```

## Ejecución con Docker

Prerequisitos: Docker Desktop, Git y (opcional) Python 3.12.

```bash
docker compose up -d zookeeper kafka kafka-init
docker compose up -d producer spark-job
docker compose logs -f spark-job
```

Verificar el topic y las particiones:

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:29092 \
  --describe --topic rapidgo.sla.events
```

Verificar las capas:

```bash
docker compose exec spark-job ls -R /opt/rapidgo/datalake
```

Detener todo:

```bash
docker compose down
```

> El job de Spark usa `local[2]` dentro del contenedor `spark-job`. Los servicios
> `spark-master` y `spark-worker` quedan disponibles para la UI y para una demo en clúster.

## Ejecución local (sin Kafka)

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# 1) Generar un lote reproducible de 120 eventos
python -m producer.producer --dry-run --count 120 --interval 300 --seed 8 \
  --start-time 2026-10-29T12:00:00Z --output producer/sla_events_batch.json

# 2) Validar contrato y calcular KPIs/alertas
python -m sla.contract producer/sla_events_batch.json
python -m sla.report producer/sla_events_batch.json
```

Salida esperada del lote de evidencia:

```text
Pedidos entregados: 25
Tiempo promedio de entrega: 36.56 min (objetivo <= 45 min)
Porcentaje fuera de SLA: 12.00% (objetivo <= 10%)
Alertas SLA_EXCEEDED: 3
Alertas DELIVERY_STOPPED: 3
```

Para ejecutar Spark local se necesita **JDK 17** y `requirements-spark.txt`:

```bash
pip install -r requirements-spark.txt
python -m spark.batch_demo --input producer/sla_events_batch.json
```

## Topic, evento, KPIs y alertas

- Topic: `rapidgo.sla.events`, 3 particiones, key `order_id`.
- Contrato: [`docs/contrato_evento.json`](docs/contrato_evento.json).
- Evento de muestra: [`samples/sla_event.json`](samples/sla_event.json).
- KPIs y umbrales: [`docs/kpis_alertas.md`](docs/kpis_alertas.md).

| KPI | Implementación |
|---|---|
| Tiempo promedio de entrega | `sla.rules.calcular_kpis` y `curated/kpis` |
| Porcentaje fuera de SLA | `sla.rules.calcular_kpis` y `curated/kpis` |
| Pedidos retrasados | `sla.rules.calcular_kpis` y `curated/kpis` |

| Alerta | Condición |
|---|---|
| `SLA_EXCEEDED` | `delivered_at > promised_at` |
| `DELIVERY_STOPPED` | pedido no final con más de 30 min desde `promised_at` |

## Pruebas y CI

```bash
python -m pytest -v
python -m compileall -q producer spark sla semana3 airflow/dags tests
python -m sla.contract samples/sla_event.json \
  producer/sla_event_sample.json producer/sla_events_batch.json
docker compose config --quiet
docker compose -f docker-compose.yml -f docker-compose.semana3.yml config --quiet
```

El workflow [`ci.yml`](.github/workflows/ci.yml) se ejecuta en `push`, `pull_request`
hacia `main` y `workflow_dispatch` y tiene tres jobs:

1. `calidad-codigo`: sintaxis, contrato, 67 pruebas y artefacto JUnit.
2. `dag-airflow`: importa el DAG, ejecuta un run exitoso, provoca el fallo controlado,
   verifica los estados `failed`/`upstream_failed` y ejecuta la recuperación.
3. `validar-configuracion`: valida ambos compose, el contrato y la estructura del repo.

## Ejecución con Airflow (Semana 3)

El DAG `rapidgo_sla_semana3` tiene 6 tareas, `schedule=None`, `retries=2` y XCom para
transportar solo rutas y conteos:

```text
validar_fuente -> perfilar_datos -> validar_calidad
-> calcular_indicadores -> generar_reporte -> publicar_resultado
```

Levantar Airflow (Airflow 2.9.3 + pyarrow) junto con el compose base:

```bash
docker compose -f docker-compose.yml -f docker-compose.semana3.yml up -d --build airflow
# UI: http://localhost:8088  (admin / admin, solo demo local)
```

Ejecutar el DAG:

```bash
docker compose -f docker-compose.yml -f docker-compose.semana3.yml \
  exec -T airflow airflow dags test rapidgo_sla_semana3 2026-11-01
```

Sin Airflow también se pueden correr las mismas tareas:

```bash
python -m semana3.sample_data \
  --input producer/sla_events_batch.json --output datalake --overwrite
python -m semana3.orchestration --lake datalake --reports reports
```

Fallo controlado y recuperación:

```bash
# Provoca el fallo del quality gate
touch airflow/flags/forzar_fallo_calidad
docker compose -f docker-compose.yml -f docker-compose.semana3.yml \
  exec -T airflow airflow dags test rapidgo_sla_semana3 2026-11-06

# Recuperación: retirar la bandera y lanzar un run nuevo
rm airflow/flags/forzar_fallo_calidad
docker compose -f docker-compose.yml -f docker-compose.semana3.yml \
  exec -T airflow airflow dags test rapidgo_sla_semana3 2026-11-07
```

Un fallo de `validar_calidad` deja `calcular_indicadores`, `generar_reporte` y
`publicar_resultado` en `upstream_failed`; no se publica un reporte parcial. La tarea de
calidad usa `retries=0` porque reintentar no corrige un dato inválido.

Detalle: [`docs/checkpoints/semana3.md`](docs/checkpoints/semana3.md),
[`docs/evidencias/semana3.md`](docs/evidencias/semana3.md) y
[`docs/decisions/ADR-003-orquestacion-y-recuperacion.md`](docs/decisions/ADR-003-orquestacion-y-recuperacion.md).

## Evidencias

- Checkpoint Semana 3: [`docs/checkpoints/semana3.md`](docs/checkpoints/semana3.md)
- Evidencia Semana 3: [`docs/evidencias/semana3.md`](docs/evidencias/semana3.md)
- ADR-003: [`docs/decisions/ADR-003-orquestacion-y-recuperacion.md`](docs/decisions/ADR-003-orquestacion-y-recuperacion.md)
- Notas de release: [`docs/releases/v0.3.0-orchestration.md`](docs/releases/v0.3.0-orchestration.md)
- Checkpoint Semana 2: [`docs/checkpoints/semana2.md`](docs/checkpoints/semana2.md)
- Evidencia de ejecución: [`docs/evidencias/semana2.md`](docs/evidencias/semana2.md)
- ADR-002: [`docs/decisions/ADR-002-ventana-watermark-checkpoints.md`](docs/decisions/ADR-002-ventana-watermark-checkpoints.md)
- Actions: pestaña **Actions → CI RapidGo** del repositorio.
  - CI en verde (PR): [run 40985893801](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985893801)
  - Fallo controlado: [run 40985829295](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985829295)
  - Ejecución manual: [run 40985938802](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985938802)
- Actividad de transferencia de GitHub Actions: [`docs/actions/actividad_transferencia.md`](docs/actions/actividad_transferencia.md)

## Limitaciones conocidas

- El 12 % fuera de SLA del lote sintético supera el objetivo inicial del 10 %: los umbrales
  siguen siendo provisionales y se calibrarán por zona con más datos.
- El pipeline end-to-end con Docker requiere conexión a internet la primera vez para
  descargar el conector Kafka-Spark desde Maven.
- La escritura Parquet local en Windows requiere `winutils.exe`; en Docker/Linux funciona
  sin pasos adicionales.
- Airflow (Semana 3) ya está implementado; el video final y el release `v1.0.0`
  corresponden a la Semana 4.
- La entrega de la Semana 3 queda en la rama `feature/semana3-orquestacion`: no se hace
  merge a `main` en esta entrega.

## Contribuciones

| Integrante | Responsabilidad | Issues | PRs principales |
|---|---|---|---|
| Andrés Morales | Producer y contrato | #4 | #16 |
| Julián Pérez | Kafka, Spark y Data Lake | #5, #6 | #17 |
| Kevin García | KPIs, alertas, pruebas, CI y Semana 3 | #3, #6, #7, #8, #9 | #18, #19, #21–#24 |
