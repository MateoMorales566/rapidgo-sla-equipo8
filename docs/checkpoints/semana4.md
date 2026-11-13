# Checkpoint Semana 4

## 1. Qué quedó cerrado

La entrega final quedó integrada en un solo flujo: paquete `sla/` unificado, un único
`docker-compose.yml`, un `requirements.txt` con todos los grupos y el Quality Gate como
paso obligatorio antes del release.

```text
producer -> Kafka -> Spark Structured Streaming -> datalake/{raw,processed,curated}
         -> Airflow (calidad, KPIs y reporte) -> reports/sla_latest.json
```

El repositorio quedó ejecutable de punta a punta con Docker y también en local sin Kafka
(contrato, reporte, fixture y demo batch de Spark). El detalle de cada verificación está en
[`docs/evidencias.md`](../evidencias.md).

## 2. Integración y consolidación

| PR | Contenido | Merge |
|---|---|---|
| [#29](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/29) | Integración del pipeline y Quality Gate | `11bb340` |
| [#30](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/30) | Paquete `sla/`, compose único y preparación del release | `fdb10fd` |
| [#31](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/31) | Evidencias y notas del release `v1.0.0` | `28851eb` |
| [#32](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/32) | Python 3.12 alineado con el CI para publicar el artefacto | `68b141d` |
| [#33](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/33) | Enlace al tablero Kanban | `234cad3` |
| [#34](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/34) | Unificación de `requirements.txt` | `6d1f284` |

El PR #30 eliminó `semana3/` y `docker-compose.semana3.yml`, consolidó
`docs/evidencias.md` y `docs/releases.md` y agregó `scripts/quality_gate.py`. El PR #34
dejó un solo archivo de dependencias con cuatro bloques (base, pruebas, Airflow y Spark) y
cada contexto instala el grupo que necesita.

## 3. Verificación local (2026-11-13, sobre `main` `6d1f284`)

| Comprobación | Resultado |
|---|---|
| `python -m pytest -q` | 70 passed |
| `python -m compileall -q producer spark sla airflow/dags tests scripts` | exit 0 |
| `python -m sla.contract samples/sla_event.json samples/sla_events_batch.json` | 1 + 120 eventos válidos |
| `python scripts/quality_gate.py` | QUALITY GATE: OK |
| `docker compose config --quiet` | exit 0 |
| Enlaces internos de la documentación | 0 rotos |
| `python -m sla.reporting samples/sla_events_batch.json` | 120 eventos, 25 entregas, 36.56 min, 12.00 % fuera de SLA |

## 4. Release

```text
Tag:      v1.0.0
Commit:   234cad3 (2026-11-12)
Release:  https://github.com/MateoMorales566/rapidgo-sla-equipo8/releases/tag/v1.0.0
Artefacto: rapidgo-sla-equipo8.zip (111 KB)
Publicado: 2026-11-13
```

El ZIP se generó desde el contenido del tag, no desde `main`. Como `main` avanzó después
con el PR #34 (`6d1f284`), el artefacto publicado no incluye la unificación de dependencias;
el tag y el ZIP siguen correspondiendo a la misma versión.

## 5. Estado de la entrega

| Requisito | Estado |
|---|---|
| Release final `v1.0.0` publicado y con artefacto | ✅ |
| Repositorio ejecutable según el README | ✅ |
| CI en verde sobre `main` (`6d1f284`) | ✅ |
| Tablero Kanban enlazado y 13 issues cerrados | ✅ |
| Documentación por semanas y ADR | ✅ |
| Video final de hasta 10 minutos | ❌ Pendiente |

## 6. Pendiente: video final

El video es el único entregable abierto. No se versiona en el repositorio.

1. Grabar la demo (máximo 10 minutos): problema, arquitectura, Docker/Kafka/Spark/Data
   Lake/Airflow con el fallo controlado y la recuperación, recorrido por GitHub y
   resultados.
2. Subirlo a YouTube (público o no listado).
3. Editar el release `v1.0.0` y agregar el enlace en las notas.
4. Reemplazar el estado "pendiente" del README por el enlace y la fecha de grabación.

El guion y el plan de grabación se guardan fuera del repositorio, junto con el resto del
material de la Semana 4.

## 7. Trazabilidad

- Issues: 13 de 13 cerrados; el tablero público es
  [RapidGo - Equipo 8 - SLA de entregas](https://github.com/users/MateoMorales566/projects/1).
- CI final: [run 43457040585](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/43457040585)
  sobre `6d1f284`.
- Requirements unificados: [run 43456837016](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/43456837016).
- Release: [run 43357711364](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/43357711364).
- Evidencias: [`docs/evidencias.md`](../evidencias.md) y
  [`docs/releases.md`](../releases.md).
