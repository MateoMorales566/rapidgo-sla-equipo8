# RapidGo SLA - Equipo 8

Proyecto final de Big Data - **Variante 1: SLA de entregas**.

Pipeline end-to-end para capturar eventos de pedidos, calcular el cumplimiento del SLA,
persistir las capas del Data Lake, generar alertas operativas y orquestar un quality gate
batch con Apache Airflow.

## Equipo

| Integrante | Responsabilidad | GitHub |
|---|---|---|
| Andrés Morales | Productor de eventos y contrato JSON | `MateoMorales566` |
| Julián Pérez | Kafka, Spark Structured Streaming y Data Lake | `NyawToo` |
| Kevin García | KPIs, alertas, pruebas, CI y documentación | `SantiagoG1309` |

## Problema de negocio

RapidGo promete un tiempo de entrega por pedido. El equipo necesita saber:

- cuánto tarda realmente una entrega;
- qué porcentaje se sale del SLA;
- qué pedidos se retrasan o quedan detenidos demasiado tiempo.

La solución captura eventos `delivery_status`, los procesa en tiempo real y deja KPIs,
alertas y un reporte batch listos para consulta.

## Estado de los releases

| Release | Semana | Estado |
|---|---|---|
| `v0.1.0-foundation` | 1 | Publicado |
| `v0.2.0-streaming` | 2 | Publicado |
| `v0.3.0-orchestration` | 3 | Publicado e integrado a `main` por el PR #25 |
| `v1.0.0` | 4 | Preparado en este PR; se publica al mergear a `main` |

Notas de cada entrega: [`docs/releases.md`](docs/releases.md).

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
      -> reports/sla_latest.json
```

Diagrama y decisiones: [`docs/arquitectura.png`](docs/arquitectura.png),
[`docs/arquitectura.md`](docs/arquitectura.md) y
[`docs/decisions/ADR-001-kafka-sla.md`](docs/decisions/ADR-001-kafka-sla.md).

## Tecnologías y rol

| Tecnología | Rol |
|---|---|
| Docker Compose | Levanta Zookeeper, Kafka, Spark y Airflow |
| Apache Kafka | Transporte de eventos, topic de 3 particiones y key `order_id` |
| Spark Structured Streaming | Parseo, enriquecimiento, ventanas, KPIs y alertas |
| Parquet + Data Lake | Persistencia por capas y reprocesamiento |
| Python + `confluent-kafka` | Generación y publicación de eventos |
| Apache Airflow 2.9.3 | Orquestación batch: quality gate, KPIs y reporte |
| pyarrow | Lectura Parquet en Airflow y fixture de demo |
| pytest | 70 pruebas de reglas, contrato, generador, calidad y orquestación |
| GitHub Actions | CI en push, PR y ejecución manual; release por tag |

## Estructura del repositorio

```text
.github/workflows/ci.yml        CI: sintaxis, contrato, pruebas, DAG, Quality Gate y compose
.github/workflows/release.yml   Release con Quality Gate previo y artefacto .zip
airflow/dags/                   DAG rapidgo_sla_semana3
airflow/Dockerfile              Imagen Airflow + pyarrow
airflow/flags/                  Bandera del fallo controlado de calidad
datalake/README.md              Contrato de capas y checkpoints
datalake/raw/                   Mensaje original de Kafka en Parquet
datalake/processed/             Evento parseado y enriquecido
datalake/curated/kpis/          Agregación por ventana y zona
datalake/curated/alerts/        SLA_EXCEEDED y DELIVERY_STOPPED
docs/arquitectura.md            Arquitectura y decisiones de diseño
docs/arquitectura.png           Diagrama para informes y presentación
docs/contrato_evento.json       Contrato JSON del evento
docs/kpis_alertas.md            KPIs, umbrales y reglas de alerta
docs/evidencias.md              Evidencias de ejecución de las semanas 2 y 3
docs/releases.md                Notas consolidadas de los releases
docs/checkpoints/               Checkpoints de las semanas 1 a 3
docs/decisions/                 ADR-001, ADR-002 y ADR-003
producer/                       Generador de eventos y cliente Kafka
samples/                        Evento de muestra y lote reproducible de 120 eventos
scripts/quality_gate.py         Quality Gate previo al release v1.0.0
sla/                            Reglas, contrato, reporte, calidad y orquestación
spark/                          Job streaming y demo batch
tests/                          70 pruebas automatizadas
docker-compose.yml              Kafka, Spark y Airflow
requirements.txt                Dependencias base y de CI
requirements-airflow.txt        Airflow 2.9.3 y pyarrow
requirements-spark.txt          PySpark para ejecución local
pytest.ini                      Configuración de pytest (pythonpath)
README.md                       Documentación principal
.gitignore                      Exclusiones de datos y entornos
.dockerignore                   Contexto de build de Docker
```

## Instalación

Prerequisitos: Docker Desktop, Git y (opcional) Python 3.11/3.12.

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt
```

Para Spark local se necesita **JDK 17** y `requirements-spark.txt`. Airflow solo se
ejecuta en Docker o en el job `dag-airflow` del CI.

## Ejecución con Docker

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

Airflow (UI en <http://localhost:8088>, `admin` / `admin` solo para demo local):

```bash
docker compose up -d --build airflow
docker compose exec -T airflow airflow dags list
```

El DAG necesita la capa `datalake/processed`; ejecútalo después de que Spark haya
procesado eventos o genera el fixture con `python -m sla.sample_data` (ver más abajo).

Detener todo:

```bash
docker compose down
```

> El job de Spark usa `local[2]` dentro del contenedor `spark-job`. Los servicios
> `spark-master` y `spark-worker` quedan disponibles para la UI y para una demo en clúster.

## Ejecución local (sin Kafka)

```bash
# 1) Generar un lote reproducible de 120 eventos en reports/
python -m producer.producer --dry-run --count 120 --interval 300 --seed 8 \
  --start-time 2026-10-29T12:00:00Z --output reports/sla_events_batch.json

# 2) Validar el contrato del evento de muestra y del lote versionado
python -m sla.contract samples/sla_event.json samples/sla_events_batch.json

# 3) Calcular KPIs y alertas
python -m sla.reporting samples/sla_events_batch.json

# 4) Ejecutar la demo batch de Spark (requiere JDK 17)
python -m spark.batch_demo --input samples/sla_events_batch.json
```

Salida esperada del lote de evidencia:

```text
Pedidos entregados: 25
Tiempo promedio de entrega: 36.56 min (objetivo <= 45 min)
Porcentaje fuera de SLA: 12.00% (objetivo <= 10%)
Alertas SLA_EXCEEDED: 3
Alertas DELIVERY_STOPPED: 3
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

## Ejecución con Airflow

El DAG `rapidgo_sla_semana3` tiene 6 tareas, `schedule=None`, `retries=2` y XCom para
transportar solo rutas y conteos:

```text
validar_fuente -> perfilar_datos -> validar_calidad
-> calcular_indicadores -> generar_reporte -> publicar_resultado
```

Ejecutar el DAG:

```bash
docker compose exec -T airflow airflow dags test rapidgo_sla_semana3 2026-11-01
```

Sin Airflow también se pueden correr las mismas tareas:

```bash
python -m sla.sample_data \
  --input samples/sla_events_batch.json --output datalake --overwrite
python -m sla.orchestration --lake datalake --reports reports
```

Fallo controlado y recuperación:

```bash
# Provoca el fallo del quality gate
touch airflow/flags/forzar_fallo_calidad
docker compose exec -T airflow airflow dags test rapidgo_sla_semana3 2026-11-06

# Recuperación: retirar la bandera y lanzar un run nuevo
rm airflow/flags/forzar_fallo_calidad
docker compose exec -T airflow airflow dags test rapidgo_sla_semana3 2026-11-07
```

Un fallo de `validar_calidad` deja `calcular_indicadores`, `generar_reporte` y
`publicar_resultado` en `upstream_failed`; no se publica un reporte parcial. La tarea de
calidad usa `retries=0` porque reintentar no corrige un dato inválido.

Detalle: [`docs/checkpoints/semana3.md`](docs/checkpoints/semana3.md),
[`docs/evidencias.md`](docs/evidencias.md) y
[`docs/decisions/ADR-003-orquestacion-y-recuperacion.md`](docs/decisions/ADR-003-orquestacion-y-recuperacion.md).

## Pruebas, Quality Gate y CI

```bash
python -m pytest -q
python -m compileall -q producer spark sla airflow/dags tests scripts
python -m sla.contract samples/sla_event.json samples/sla_events_batch.json
python scripts/quality_gate.py
docker compose config --quiet
```

El workflow [`ci.yml`](.github/workflows/ci.yml) se ejecuta en `push`,
`pull_request` hacia `main` y `workflow_dispatch` con tres jobs:

1. `calidad-codigo`: sintaxis, contrato, 70 pruebas, Quality Gate y artefacto JUnit.
2. `dag-airflow`: importa el DAG, ejecuta un run exitoso, provoca el fallo controlado,
   verifica los estados `failed`/`upstream_failed` y ejecuta la recuperación.
3. `validar-configuracion`: valida el compose, el contrato, la estructura del repositorio
   y empaqueta el artefacto de release.

El **Quality Gate** (`scripts/quality_gate.py`) exige README completo, compose, pruebas,
paquete `sla/`, DAG con al menos 5 tareas, tres ADR y los checkpoints semanales. Si falla,
el CI y el workflow de release se detienen y no se publica `v1.0.0`.

[`release.yml`](.github/workflows/release.yml) valida el Quality Gate, empaqueta el
repositorio y publica el GitHub Release a partir de un tag `v*.*.*`.

## Resultados

| Métrica | Valor |
|---|---|
| Eventos del lote reproducible | 120 |
| Pedidos únicos | 30 |
| Pedidos entregados | 25 |
| Tiempo promedio de entrega | 36.56 min |
| Porcentaje fuera de SLA | 12.00 % |
| Pedidos retrasados | 3 |
| Alertas `SLA_EXCEEDED` | 3 |
| Alertas `DELIVERY_STOPPED` | 3 |
| Filas inválidas / duplicadas en `processed` | 0 / 0 |

## Evidencias

- Checkpoints: [`semana1`](docs/checkpoints/semana1.md),
  [`semana2`](docs/checkpoints/semana2.md) y
  [`semana3`](docs/checkpoints/semana3.md).
- Evidencias de ejecución: [`docs/evidencias.md`](docs/evidencias.md).
- ADR: [`ADR-001`](docs/decisions/ADR-001-kafka-sla.md),
  [`ADR-002`](docs/decisions/ADR-002-ventana-watermark-checkpoints.md) y
  [`ADR-003`](docs/decisions/ADR-003-orquestacion-y-recuperacion.md).
- Notas de release: [`docs/releases.md`](docs/releases.md).
- Release `v0.3.0-orchestration`:
  [releases/tag/v0.3.0-orchestration](https://github.com/MateoMorales566/rapidgo-sla-equipo8/releases/tag/v0.3.0-orchestration)
- Actions: pestaña **Actions -> CI RapidGo** del repositorio.
  - Semana 3 en verde (PR #25): [run 42147681397](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/42147681397)
  - CI en verde Semana 2: [run 40985893801](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985893801)
  - Fallo controlado: [run 40985829295](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985829295)
  - Ejecución manual: [run 40985938802](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985938802)

## Limitaciones conocidas

- El 12 % fuera de SLA del lote sintético supera el objetivo inicial del 10 %: los umbrales
  siguen siendo provisionales y se calibrarán por zona con más datos.
- El pipeline end-to-end con Docker requiere conexión a internet la primera vez para
  descargar el conector Kafka-Spark desde Maven.
- La escritura Parquet local en Windows requiere `winutils.exe`; en Docker/Linux funciona
  sin pasos adicionales.
- El video final no se versiona en el repositorio; se enlaza desde el release `v1.0.0`.

## Responsables y contribuciones

| Integrante | Responsabilidad | Issues asignados | PRs |
|---|---|---|---|
| Andrés Morales | Producer y contrato | #1, #4, #7, #13 | #15, #17, #23 |
| Julián Pérez | Kafka, Spark y Data Lake | #2, #5, #9 | #14, #18, #22, #24, #26, #27, #28 |
| Kevin García | KPIs, alertas, pruebas, CI y Semanas 3-4 | #3, #6, #8, #10, #11, #12 | #16, #19, #20, #21, #25, #29 |

La autoría de los commits dentro de cada PR puede incluir aportes de otros integrantes;
el detalle queda en el historial de cada rama. Antes del release final cada integrante
debe ejecutar al menos una vez el workflow con `workflow_dispatch` y dejar el run visible
en Actions.
