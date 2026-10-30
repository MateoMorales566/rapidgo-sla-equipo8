# Checkpoint Semana 2

## 1. Qué quedó funcionando

Se completó el núcleo del pipeline de la variante 1 (SLA de entregas):

- **Producer real** con `confluent-kafka`: publica en `rapidgo.sla.events` con key `order_id` y `team_id=equipo8`.
- **Topic con 3 particiones** creado por el servicio `kafka-init` del `docker-compose.yml`.
- **Spark Structured Streaming** con tres consultas: `raw`, `processed` y `curated` (KPIs + alertas).
- **Tres transformaciones**: parseo del JSON, timestamps de negocio y cálculo de duración/clasificación SLA.
- **Agregación por ventana** de 15 minutos y zona en `curated/kpis`.
- **Dos alertas implementadas**: `SLA_EXCEEDED` y `DELIVERY_STOPPED` en `curated/alerts`.
- **Parquet y checkpoint por capa** en `datalake/{raw,processed,curated}` y `datalake/_checkpoints`.
- **38 pruebas pytest** (reglas, contrato, generador y reporte) y **CI** con `push`, `pull_request` y `workflow_dispatch`.

## 2. Evidencia real de ejecución

| Verificación | Resultado |
|---|---|
| `python -m pytest -q` | `38 passed` |
| `python -m compileall producer spark sla tests` | exit 0 |
| `python -m sla.contract ...` | 120 eventos válidos |
| `python -m sla.report producer/sla_events_batch.json` | 25 entregas, promedio **36.56 min**, **12.00 %** fuera de SLA, 3 alertas `SLA_EXCEEDED`, 3 `DELIVERY_STOPPED` |
| `python -m spark.batch_demo ...` | 120 eventos, ventanas de 15 min, 6 alertas únicas |
| `docker compose config --quiet` | exit 0 |

El detalle de comandos y salidas está en [`docs/evidencias/semana2.md`](../evidencias/semana2.md).

## 3. Decisión técnica tomada

Usar **watermark de 2 minutos**, **ventana fija de 15 minutos** y **un checkpoint por capa**. La decisión se tomó después de observar que el lote de 120 eventos produce suficiente dispersión (36.56 min de promedio y 12 % fuera de SLA) como para necesitar agregación por ventana y reprocesamiento desde `raw`. Está documentada en [`ADR-002`](../decisions/ADR-002-ventana-watermark-checkpoints.md).

## 4. Problema real y solución

Al ejecutar `pytest` desde la raíz, **5 de 6 pruebas fallaban** por la ruta relativa `../samples/sla_event.json`. Se reemplazó por `Path(__file__).resolve().parents[1]`, se agregó `pytest.ini` con `pythonpath = .` y se amplió la suite a 38 pruebas. También se corrigió `requirements.txt`: `apache-spark` no existe en PyPI y faltaba `pyspark`, por lo que cualquier CI habría fallado al instalar dependencias.

- Issue: [#4 Producer](https://github.com/MateoMorales566/rapidgo-sla-equipo8/issues/4), [#5 Kafka/Spark](https://github.com/MateoMorales566/rapidgo-sla-equipo8/issues/5), [#6 Data Lake](https://github.com/MateoMorales566/rapidgo-sla-equipo8/issues/6)
- Pull Request: [#19 feature/semana2-pipeline-ci](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/19)
- Actions: se enlaza la ejecución del PR #19 en la pestaña **Actions → CI RapidGo**.

## 5. Plan de la Semana 3

- DAG de Airflow con mínimo 5 tareas, retries y XCom.
- Validaciones de calidad sobre `curated`.
- Fallo controlado y recuperación documentada.
- `ADR-003` y release `v0.3.0-orchestration`.
