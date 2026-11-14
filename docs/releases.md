# Notas de release - RapidGo SLA

Historial consolidado de las cuatro entregas del proyecto. El detalle operativo está en
[`docs/evidencias.md`](evidencias.md) y en los checkpoints semanales.

## v0.1.0-foundation (Semana 1)

### Resumen

Primera entrega: la base de diseño del pipeline. Define qué eventos se capturan, cómo se
transportan, qué se mide y cómo se decide el cumplimiento del SLA.

### Contenido

- **Arquitectura** de la variante SLA en `docs/arquitectura.md` y `docs/arquitectura.png`.
- **Contrato JSON** del evento `delivery_status` en `docs/contrato_evento.json`, con un
  evento de muestra en `samples/sla_event.json`.
- **Tres KPIs** con umbrales iniciales: tiempo promedio de entrega, porcentaje fuera de
  SLA y pedidos retrasados (`docs/kpis_alertas.md`).
- **Dos reglas de alerta**: `SLA_EXCEEDED` y `DELIVERY_STOPPED`
  (`docs/kpis_alertas.md`).
- **Decisión de Kafka**: topic `rapidgo.sla.events`, key `order_id` y 3 particiones,
  justificada en `docs/decisions/ADR-001-kafka-sla.md`.
- **Estructura inicial del repositorio** y `docker-compose.yml` preliminar con Kafka,
  Spark y el producer.
- **Checkpoint de Semana 1** en `docs/checkpoints/semana1.md`.

### Evidencia

```text
Tag:    v0.1.0-foundation
Commit: 138338c (merge del PR #15, 22/10/2026)
```

En este tag `producer/`, `spark/` y `tests/` solo contienen `.gitkeep`: la implementación
funcional empieza en `v0.2.0-streaming`.

### Cómo verificar

```bash
git show 138338c --stat
git show 138338c:docs/contrato_evento.json
git show 138338c:docs/kpis_alertas.md
```

### Limitaciones

- El `docker-compose.yml` es preliminar; todavía no levanta el pipeline completo.
- Los umbrales son valores de diseño y se recalibran con datos reales en la Semana 2.
- No hay pruebas automatizadas ni CI en esta entrega.

### Enlaces

- Checkpoint: [`docs/checkpoints/semana1.md`](checkpoints/semana1.md)
- Arquitectura: [`docs/arquitectura.md`](arquitectura.md)
- Contrato: [`docs/contrato_evento.json`](contrato_evento.json)
- ADR-001: [`docs/decisions/ADR-001-kafka-sla.md`](decisions/ADR-001-kafka-sla.md)

## v0.2.0-streaming (Semana 2)

### Resumen

Pipeline funcional de la variante 1 (SLA de entregas): Kafka -> Spark Structured
Streaming -> Data Lake, con tres transformaciones, agregación por ventana, dos alertas y
CI en GitHub Actions.

### Cambios principales

- Producer real con `confluent-kafka`, key `order_id` y `team_id=equipo8`.
- Topic `rapidgo.sla.events` con 3 particiones creado por `kafka-init`.
- Spark Structured Streaming con capas `raw`, `processed` y `curated`.
- Checkpoint independiente por capa y watermark de 2 minutos.
- KPIs por ventana de 15 minutos y zona; alertas `SLA_EXCEEDED` y `DELIVERY_STOPPED`.
- 38 pruebas pytest en el tag (la suite creció a 70 en la entrega final): reglas,
  contrato, generador y reporte.
- CI `ci.yml` con `push`, `pull_request` y `workflow_dispatch`.
- `release.yml` para publicar releases a partir de tags `v*.*.*`.
- ADR-002 y checkpoint de Semana 2 con evidencia real.
- Contrato actualizado: `team_id` obligatorio y timestamps de pickup/delivery nullables.

### Evidencia

```text
38 passed (tag v0.2.0)
120 eventos válidos
Tiempo promedio: 36.56 min
% fuera de SLA: 12.00%
SLA_EXCEEDED: 3
DELIVERY_STOPPED: 3
docker compose config --quiet: OK
```

Detalle en `docs/evidencias.md`.

### Cómo verificar

```bash
python -m pytest -v
python -m sla.contract samples/sla_event.json samples/sla_events_batch.json
docker compose config --quiet
```

### Limitaciones

- El 12 % fuera de SLA supera el objetivo inicial del 10 %; queda documentado como
  hallazgo para calibrar con más datos.
- Airflow y el release final `v1.0.0` corresponden a las semanas 3 y 4.

### Tag y publicación

El tag `v0.2.0-streaming` se creó el 30/10/2026 sobre el merge del PR #19 y el workflow
`Release RapidGo` publicó el GitHub Release a partir de ese tag.

## v0.3.0-orchestration (Semana 3)

### Resumen

Airflow orquesta la validación de calidad, el cálculo de KPIs y la publicación del
reporte sobre la capa `processed` que deja Spark Structured Streaming.

### Cambios principales

- **DAG `rapidgo_sla_semana3`** con 6 tareas, dependencias, `retries=2`, XCom y
  `schedule=None`.
- **Quality gate** sobre Parquet: columnas requeridas, filas presentes, sin `event_id`
  duplicados y sin filas inválidas.
- **Reporte atómico** `reports/sla_latest.json` con métricas, calidad y fuentes.
- **Fallo controlado** con la bandera `airflow/flags/forzar_fallo_calidad` y recuperación
  documentada.
- **Airflow 2.9.3 + pyarrow** integrado en `docker-compose.yml`, con Dockerfile propio.
- **Runner local** `python -m sla.orchestration` para ejecutar las mismas tareas sin
  Airflow.
- **Generador de fixture** `python -m sla.sample_data` para demos y CI.
- **CI ampliado**: importación real del DAG, run exitoso, fallo controlado, recuperación y
  artefactos de reporte/logs.
- **67 pruebas pytest** en el tag y documentación ADR-003/checkpoint/evidencias.

### Evidencia

```text
DAG: rapidgo_sla_semana3 (6 tasks)
Run exitoso:  manual__2026-11-01T00:00:00-05:00
Run fallido:  manual__2026-11-06T00:00:00-05:00
Recuperación: manual__2026-11-07T00:00:00-05:00
Eventos: 120
Pedidos únicos: 30
Tiempo promedio: 36.56 min
Fuera de SLA: 12.0 %
SLA_EXCEEDED: 3
DELIVERY_STOPPED: 3
pytest: 67 passed
```

### Cómo verificar

```bash
python -m pytest -v
python -m compileall -q producer spark sla airflow/dags tests scripts
python -m sla.sample_data --input samples/sla_events_batch.json \
  --output reports/sample-datalake --overwrite
python -m sla.orchestration --lake reports/sample-datalake \
  --reports reports/sample-reports
docker compose config --quiet
```

Airflow local:

```bash
docker compose up -d --build airflow
# http://localhost:8088  (admin / admin)
```

### Enlaces

- Integración: PR [#25](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/25)
  mergeado a `main` el 06/11/2026.
- Commit del tag: `9b396de`.
- CI en verde en `main`: [run 42149252424](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/42149252424)

### Limitaciones

- El 12 % fuera de SLA supera el objetivo inicial del 10 %; queda como hallazgo para
  revisar en la Semana 4.
- Las credenciales y la `FERNET_KEY` del compose son solo para demo local.

## v1.0.0 (Semana 4)

### Resumen

Entrega final: repositorio unificado y ejecutable de punta a punta. Consolida el paquete
`sla/`, un solo `docker-compose.yml`, el Quality Gate previo al release y el README
definitivo con instalación, arquitectura, ejecución, resultados y responsables.

### Contenido

- Paquete único `sla/` con reglas, contrato, reporte, calidad, orquestación y fixture.
- `docker-compose.yml` con Kafka, Spark y Airflow; sin overlays.
- `scripts/quality_gate.py` y job de Quality Gate en CI/release.
- `release.yml` con quality gate previo, empaquetado `.zip` y publicación del release.
- `docs/evidencias.md` y `docs/releases.md` consolidados.
- 70 pruebas automatizadas y CI en verde antes del tag.
- Video final del equipo: [Proyecto Final Big Data SLA - RapidGo Equipo 8](https://youtu.be/LwqG7DeIxgM) (10:47).
- `requirements.txt` unificado en cuatro grupos (base, pruebas, Airflow y Spark) en el
  PR [#34](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/34), después del tag.

### Cómo verificar

```bash
python -m pytest -q
python -m compileall -q producer spark sla airflow/dags tests scripts
python scripts/quality_gate.py
docker compose config --quiet
```

### Ejecución de cierre (2026-11-12)

Stack completo con Docker sobre `main` (`fdb10fd`):

```text
Topic rapidgo.sla.events: 3 particiones
Spark streaming: raw, processed y curated activos
Airflow DAG rapidgo_sla_semana3: state=success, run_duration=2.49 s
Reporte reports/sla_latest.json: 180 eventos, 53 pedidos, 35.29 min,
18.18 % fuera de SLA, 6 SLA_EXCEEDED, 3 DELIVERY_STOPPED
Calidad: 0 filas inválidas, 0 duplicados
```

| Verificación | Resultado |
|---|---|
| `pytest -q` | 70 passed |
| `compileall` | OK |
| Contrato JSON | 121 eventos válidos |
| `quality_gate.py` | OK |
| `docker compose config --quiet` | OK |
| CI en `main` | success |

### Estado

Publicado. Tag y GitHub Release `v1.0.0` creados sobre el merge `c8292c4` (PR #37,
2026-11-13) con el ZIP del repositorio adjunto por `release.yml`. El tag se movió a ese
merge para que el artefacto incluya el enlace del video; después, `main` solo avanzó con el
PR #38 (registro documental).

El video final no forma parte del repositorio: está publicado en YouTube
([https://youtu.be/LwqG7DeIxgM](https://youtu.be/LwqG7DeIxgM)) con la participación de los
tres integrantes y enlazado desde este release y desde el README.
