# Evidencias de ejecución - Semana 3

Rama: `feature/semana3-orquestacion`  
Fecha de ejecución: 2026-11-05  
Entorno: Docker Desktop + Airflow 2.9.3 + `pyarrow==16.1.0`

## 1. Pruebas y sintaxis

```text
$ python -m pytest -q
...................................................................      [100%]
67 passed in 5.00s

$ python -m compileall -q producer spark sla semana3 airflow/dags tests
exit 0
```

Cobertura nueva de la Semana 3: quality gate (columnas, filas, duplicados, umbral),
deduplicación por `event_time`, escritura atómica, fallo controlado, recuperación,
generador de capa `processed` y estructura estática del DAG.

## 2. Data Lake de prueba

```text
$ python -m semana3.sample_data \
    --input producer/sla_events_batch.json \
    --output datalake --overwrite
Eventos leídos: 120; escritos: 120; descartados: 0
Parquet: datalake/processed/part-00000.parquet (8844 bytes)
Rango temporal: 2026-10-29T12:01:00.000Z -> 2026-10-29T14:18:00.000Z
```

El generador usa las mismas reglas que `spark/transforms.py` (`delivery_minutes`,
`minutes_late`, `sla_status`, `is_late`) para que el DAG tenga un fixture realista sin
depender de Spark en el contenedor de Airflow.

## 3. Runner local (sin Airflow)

```text
$ python -m semana3.orchestration \
    --lake reports/sample-datalake --reports reports/sample-reports
Reporte SLA listo: {"report": "reports\\sample-reports\\sla_semana3_latest.json",
"size_bytes": 1306, "pedidos_unicos": 30, "tiempo_promedio_min": 36.56,
"porcentaje_fuera_sla": 12.0, "alertas": {"SLA_EXCEEDED": 3, "DELIVERY_STOPPED": 3}}
```

Fallo controlado del runner:

```text
$ python -m semana3.orchestration --force-failure
ERROR: FALLO_CONTROLADO: bandera de calidad activa (...). Bórrela y vuelva a ejecutar el DAG.
exit 2
```

## 4. Airflow: DAG importado y visible

```text
$ docker compose -f docker-compose.yml -f docker-compose.semana3.yml up -d --build airflow
$ docker compose ... exec -T airflow airflow dags list
dag_id              | fileloc                                        | owners  | is_paused
rapidgo_sla_semana3 | /opt/airflow/dags/rapidgo_sla_orchestration.py | equipo8 | True
```

Interfaz: <http://localhost:8088> (usuario `admin`, contraseña `admin` para la demo).

## 5. Run exitoso

```text
$ docker compose ... exec -T airflow \
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

Reporte generado en `reports/sla_semana3_latest.json` (1250 bytes):

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

## 6. Fallo controlado

```text
$ touch airflow/flags/forzar_fallo_calidad
$ docker compose ... exec -T airflow \
    airflow dags test rapidgo_sla_semana3 2026-11-06
...
RuntimeError: FALLO_CONTROLADO: bandera de calidad activa
(/opt/airflow/flags/forzar_fallo_calidad). Bórrela y vuelva a ejecutar el DAG.
DagRun Finished: run_id=manual__2026-11-06T00:00:00-05:00, state=failed
exit 1
```

Estados de las tareas del run fallido:

```text
$ docker compose ... exec -T airflow airflow tasks states-for-dag-run \
    rapidgo_sla_semana3 manual__2026-11-06T00:00:00-05:00

task_id              | state
validar_fuente       | success
perfilar_datos       | success
validar_calidad      | failed
calcular_indicadores | upstream_failed
generar_reporte      | upstream_failed
publicar_resultado   | upstream_failed
```

## 7. Recuperación

```text
$ rm airflow/flags/forzar_fallo_calidad
$ docker compose ... exec -T airflow \
    airflow dags test rapidgo_sla_semana3 2026-11-07
...
Reporte SLA listo: {"report": "/opt/airflow/reports/sla_semana3_latest.json",
"size_bytes": 1250, "pedidos_unicos": 30, "tiempo_promedio_min": 36.56,
"porcentaje_fuera_sla": 12.0, "alertas": {"SLA_EXCEEDED": 3, "DELIVERY_STOPPED": 3}}
DagRun Finished: run_id=manual__2026-11-07T00:00:00-05:00, state=success
```

No se borraron Parquet, reportes ni checkpoints. El cambio que resolvió el problema fue
retirar la bandera y lanzar un run nuevo.

## 8. Docker Compose

```text
$ docker compose config --quiet
exit 0
$ docker compose -f docker-compose.yml -f docker-compose.semana3.yml config --quiet
exit 0
```

## 9. GitHub Actions

El workflow `CI RapidGo` tiene tres jobs:

1. `calidad-codigo`: sintaxis, contrato, 67 pruebas y artefacto JUnit.
2. `dag-airflow`: instala Airflow, importa el DAG, ejecuta un run exitoso, provoca el
   fallo controlado, verifica los estados `failed`/`upstream_failed` y ejecuta la
   recuperación. Publica reporte y logs como artefacto.
3. `validar-configuracion`: valida el compose base y el overlay de Semana 3, el contrato
   JSON y la estructura mínima del repositorio.

Los enlaces de la ejecución de la rama se agregan al PR y al README cuando el workflow
termine.
