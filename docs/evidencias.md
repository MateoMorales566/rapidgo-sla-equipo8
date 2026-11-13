# Evidencias de ejecución - Semanas 2 y 3

Documento consolidado con las salidas reales del pipeline. Los conteos de pruebas
corresponden al momento de cada entrega; la suite actual se ejecuta en el CI y en
`python -m pytest -q`.

## Semana 2 - Pipeline Kafka -> Spark -> Data Lake

Rama: `feature/semana2-pipeline-ci`
Fecha: 2026-10-29

### 1. Pruebas y sintaxis

```text
$ python -m pytest -q
......................................                                   [100%]
38 passed in 0.22s

$ python -m compileall -q producer spark sla tests
exit 0
```

### 2. Contrato JSON

```text
$ python -m sla.contract samples/sla_event.json samples/sla_events_batch.json
OK samples/sla_event.json: 1 evento(s) válido(s)
OK samples/sla_events_batch.json: 120 evento(s) válido(s)
```

### 3. KPIs y alertas del lote de 120 eventos

Comandos:

```text
python -m producer.producer --dry-run --count 120 --interval 300 --seed 8 \
  --start-time 2026-10-29T12:00:00Z --output samples/sla_events_batch.json
python -m sla.reporting samples/sla_events_batch.json
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

### 4. Spark batch sobre el mismo lote

Comando (requiere JDK 17 y `requirements-spark.txt`):

```text
python -m spark.batch_demo --input samples/sla_events_batch.json
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
> validaron las transformaciones en batch; el job streaming escribe las tres capas con
> `writeStream` y `checkpointLocation`.

### 5. Docker Compose

```text
$ docker compose config --quiet
exit 0
```

El compose define:

- `kafka-init` crea `rapidgo.sla.events` con 3 particiones.
- `producer` publica con key `order_id`.
- `spark-job` ejecuta `spark-submit` con el conector Kafka.
- `airflow` ejecuta el DAG batch de calidad y reporte sobre `processed`.

### 6. Comandos para la demo end-to-end

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

### 7. CI en GitHub Actions

El workflow `CI RapidGo` (`.github/workflows/ci.yml`) se ejecuta en `push`,
`pull_request` hacia `main` y `workflow_dispatch`.

| Escenario | Commit | Run |
|---|---|---|
| Push inicial en verde | `1c93efe` | [40985753336](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985753336) |
| PR en verde | `1c93efe` | [40985764861](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985764861) |
| Fallo controlado (prueba rota a propósito) | `6f10a18` | [40985829295](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985829295) |
| Corrección, CI en verde | `e6cc5ce` | [40985893801](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985893801) |
| Ejecución manual `workflow_dispatch` | `e6cc5ce` | [40985938802](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985938802) |

Línea esencial del fallo controlado:

```text
E       AssertionError: assert 'CUMPLE' == 'FUERA_SLA'
FAILED tests/test_sla.py::test_clasificar_cumplimiento_limite - AssertionError
```

Solo falló el step **Ejecutar pruebas**; `Instalar dependencias`, `Validar sintaxis
Python` y `Validar contrato JSON` pasaron, lo que confirma que el fallo era de la regla y
no del entorno.

## Semana 3 - Airflow, calidad y resiliencia

Rama: `feature/semana3-orquestacion`
Fecha de ejecución: 2026-11-05
Entorno: Docker Desktop + Airflow 2.9.3 + `pyarrow==16.1.0`

### 8. Pruebas y sintaxis

```text
$ python -m pytest -q
...................................................................      [100%]
67 passed in 5.00s

$ python -m compileall -q producer spark sla airflow/dags tests scripts
exit 0
```

Cobertura nueva de la Semana 3: quality gate (columnas, filas, duplicados, umbral),
deduplicación por `event_time`, escritura atómica, fallo controlado, recuperación,
generador de capa `processed` y estructura estática del DAG.

### 9. Data Lake de prueba

```text
$ python -m sla.sample_data \
    --input samples/sla_events_batch.json \
    --output datalake --overwrite
Eventos leídos: 120; escritos: 120; descartados: 0
Parquet: datalake/processed/part-00000.parquet (8844 bytes)
Rango temporal: 2026-10-29T12:01:00.000Z -> 2026-10-29T14:18:00.000Z
```

El generador usa las mismas reglas que `spark/transforms.py` (`delivery_minutes`,
`minutes_late`, `sla_status`, `is_late`) para que el DAG tenga un fixture realista sin
depender de Spark en el contenedor de Airflow.

### 10. Runner local (sin Airflow)

```text
$ python -m sla.orchestration \
    --lake reports/sample-datalake --reports reports/sample-reports
Reporte SLA listo: {"report": "reports\\sample-reports\\sla_latest.json",
"size_bytes": 1306, "pedidos_unicos": 30, "tiempo_promedio_min": 36.56,
"porcentaje_fuera_sla": 12.0, "alertas": {"SLA_EXCEEDED": 3, "DELIVERY_STOPPED": 3}}
```

Fallo controlado del runner:

```text
$ python -m sla.orchestration --force-failure
ERROR: FALLO_CONTROLADO: bandera de calidad activa (...). Bórrela y vuelva a ejecutar el DAG.
exit 2
```

### 11. Airflow: DAG importado y visible

```text
$ docker compose up -d --build airflow
$ docker compose exec -T airflow airflow dags list
dag_id              | fileloc                                        | owners  | is_paused
rapidgo_sla_semana3 | /opt/airflow/dags/rapidgo_sla_orchestration.py | equipo8 | True
```

Interfaz: <http://localhost:8088> (usuario `admin`, contraseña `admin` para la demo).

### 12. Run exitoso

```text
$ docker compose exec -T airflow \
    airflow dags test rapidgo_sla_semana3 2026-11-01
...
Marking task as SUCCESS. task_id=validar_fuente
Marking task as SUCCESS. task_id=perfilar_datos
Marking task as SUCCESS. task_id=validar_calidad
Marking task as SUCCESS. task_id=calcular_indicadores
Marking task as SUCCESS. task_id=generar_reporte
Marking task as SUCCESS. task_id=publicar_resultado
DagRun Finished: run_id=manual__2026-11-01T00:00:00-05:00, state=success
```

Salida de `calcular_indicadores` (XCom real):

```text
{'eventos_totales': 120, 'pedidos_unicos': 30, 'pedidos_entregados': 25,
 'pedidos_en_curso': 3, 'pedidos_cancelados': 2, 'tiempo_promedio_min': 36.56,
 'porcentaje_fuera_sla': 12.0, 'pedidos_retrasados': 3,
 'alertas_sla_excedido': 3, 'alertas_delivery_stopped': 3,
 'reloj_referencia': '2026-10-29T14:18:00.000Z'}
```

Reporte generado en `reports/sla_latest.json` (1250 bytes):

```json
{
  "metrics": {
    "eventos_totales": 120,
    "pedidos_unicos": 30,
    "pedidos_entregados": 25,
    "pedidos_en_curso": 3,
    "pedidos_cancelados": 2,
    "tiempo_promedio_min": 36.56,
    "porcentaje_fuera_sla": 12.0,
    "pedidos_retrasados": 3,
    "alertas_sla_excedido": 3,
    "alertas_delivery_stopped": 3
  },
  "quality": {
    "passed": true,
    "checks": {
      "columnas_requeridas": true,
      "filas_presentes": true,
      "sin_duplicados": true,
      "invalidos_dentro_de_umbral": true
    }
  }
}
```

### 13. Fallo controlado

```text
$ touch airflow/flags/forzar_fallo_calidad
$ docker compose exec -T airflow \
    airflow dags test rapidgo_sla_semana3 2026-11-06
...
RuntimeError: FALLO_CONTROLADO: bandera de calidad activa
(/opt/airflow/flags/forzar_fallo_calidad). Bórrela y vuelva a ejecutar el DAG.
DagRun Finished: run_id=manual__2026-11-06T00:00:00-05:00, state=failed
exit 1
```

Estados de las tareas del run fallido:

```text
$ docker compose exec -T airflow airflow tasks states-for-dag-run \
    rapidgo_sla_semana3 manual__2026-11-06T00:00:00-05:00

task_id              | state
validar_fuente       | success
perfilar_datos       | success
validar_calidad      | failed
calcular_indicadores | upstream_failed
generar_reporte      | upstream_failed
publicar_resultado   | upstream_failed
```

### 14. Recuperación

```text
$ rm airflow/flags/forzar_fallo_calidad
$ docker compose exec -T airflow \
    airflow dags test rapidgo_sla_semana3 2026-11-07
...
Reporte SLA listo: {"report": "/opt/airflow/reports/sla_latest.json",
"size_bytes": 1250, "pedidos_unicos": 30, "tiempo_promedio_min": 36.56,
"porcentaje_fuera_sla": 12.0, "alertas": {"SLA_EXCEEDED": 3, "DELIVERY_STOPPED": 3}}
DagRun Finished: run_id=manual__2026-11-07T00:00:00-05:00, state=success
```

No se borraron Parquet, reportes ni checkpoints. El cambio que resolvió el problema fue
retirar la bandera y lanzar un run nuevo.

### 15. Docker Compose

```text
$ docker compose config --quiet
exit 0
```

### 16. GitHub Actions

El workflow `CI RapidGo` tiene tres jobs:

1. `calidad-codigo`: sintaxis, contrato, pruebas, Quality Gate de Semana 4 y artefacto
   JUnit.
2. `dag-airflow`: instala Airflow, importa el DAG, ejecuta un run exitoso, provoca el
   fallo controlado, verifica los estados `failed`/`upstream_failed` y ejecuta la
   recuperación. Publica reporte y logs como artefacto.
3. `validar-configuracion`: valida el compose completo, el contrato JSON, la estructura
   mínima del repositorio y empaqueta el artefacto de release.

Los enlaces de la ejecución de la rama:

- PR mergeado: [#25](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/25) (06/11/2026)
- CI completo en verde: [run 42147681397](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/42147681397)
- Job del DAG (importación, fallo controlado y recuperación): [job 127663010836](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/42147681397/job/127663010836)
- Release: [v0.3.0-orchestration](https://github.com/MateoMorales566/rapidgo-sla-equipo8/releases/tag/v0.3.0-orchestration)
- Workflow de release: [run 42148261466](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/42148261466)

## Semana 4 - Integración, Quality Gate y release

Rama: `feature/optimizacion-estructura`  
Fecha: 2026-11-12

### 17. Estructura final y verificaciones

```text
$ python -m pytest -q
......................................................................   [100%]
70 passed

$ python -m compileall -q producer spark sla airflow/dags tests scripts
exit 0

$ python -m sla.contract samples/sla_event.json samples/sla_events_batch.json
OK samples/sla_event.json: 1 evento(s) válido(s)
OK samples/sla_events_batch.json: 120 evento(s) válido(s)

$ python scripts/quality_gate.py
QUALITY GATE: OK

$ docker compose config --quiet
exit 0
```

El Quality Gate exige README completo, los servicios `kafka`, `spark-job` y `airflow` en
el compose, el paquete `sla/`, los tres ADR, los tres checkpoints y un DAG con al menos 5
tareas. También se empaqueta `reports/rapidgo-sla-equipo8.zip` como artefacto de release.

### 18. CI y PR

- PR de integración: [#30](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/30)
- CI en `push`: [run 43349700354](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/43349700354)
- CI en `pull_request`: [run 43349705279](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/43349705279)
- Ejecución manual `workflow_dispatch`: [run 43349871595](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/43349871595)

Los tres jobs terminaron en `success`: `calidad-codigo`, `dag-airflow` y
`validar-configuracion`.

### 19. Pipeline completo con Docker sobre `main`

Ejecución de cierre sobre el merge `fdb10fd` (2026-11-12), con el stack completo levantado
desde cero.

```text
$ docker compose up -d zookeeper kafka kafka-init
$ docker compose exec kafka kafka-topics --bootstrap-server localhost:29092 \
    --describe --topic rapidgo.sla.events
Topic: rapidgo.sla.events   PartitionCount: 3   ReplicationFactor: 1

$ docker compose up -d --no-deps spark-job
spark-job-1 | Consultas activas: raw, processed, curated
spark-job-1 | [curated] batch=1 kpis=2 alertas=0

$ python -m producer.producer --count 60 --interval 0.05 --seed 8
$ docker compose exec -T airflow airflow dags test rapidgo_sla_semana3
[2026-11-12 22:00:14] {dagrun.py:901} DagRun Finished:
  dag_id=rapidgo_sla_semana3, state=success, run_duration=2.49
Reporte SLA listo: {"report": "/opt/airflow/reports/sla_latest.json",
"size_bytes": 1794, "pedidos_unicos": 53, "tiempo_promedio_min": 35.29,
"porcentaje_fuera_sla": 18.18,
"alertas": {"SLA_EXCEEDED": 6, "DELIVERY_STOPPED": 3}}
```

Resultado del reporte (`reports/sla_latest.json`):

| Métrica | Valor |
|---|---|
| Eventos procesados | 180 |
| Pedidos únicos | 53 |
| Pedidos entregados | 33 |
| Tiempo promedio | 35.29 min |
| Porcentaje fuera de SLA | 18.18 % |
| Alertas `SLA_EXCEEDED` | 6 |
| Alertas `DELIVERY_STOPPED` | 3 |
| Filas inválidas / duplicadas | 0 / 0 |

Las tres capas quedaron escritas en Parquet: `raw`, `processed` y `curated/{kpis,alerts}`,
con checkpoints en `datalake/_checkpoints/`.

### 20. Cierre del release

- Tag y release `v1.0.0` sobre `main` (merge `fdb10fd`).
- Issue [#12](https://github.com/MateoMorales566/rapidgo-sla-equipo8/issues/12) cerrado con
  el release.
- Issue [#11](https://github.com/MateoMorales566/rapidgo-sla-equipo8/issues/11) cerrado por
  el PR #30.
- Issue [#10](https://github.com/MateoMorales566/rapidgo-sla-equipo8/issues/10) cerrado por
  el PR #29.
- Pendiente fuera del repositorio: video final y el `workflow_dispatch` manual de los otros
  dos integrantes.

