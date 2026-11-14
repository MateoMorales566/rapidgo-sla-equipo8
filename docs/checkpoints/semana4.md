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
| [#35](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/35) | Cierre documental y guía de publicación del video | `4a53d7b` |
| [#36](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/36) | Verificación en clon limpio y evidencias del cierre | `23a4095` |
| [#37](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/37) | Enlace del video final en README, release y evidencias | `c8292c4` |

El PR #30 eliminó `semana3/` y `docker-compose.semana3.yml`, consolidó
`docs/evidencias.md` y `docs/releases.md` y agregó `scripts/quality_gate.py`. El PR #34
dejó un solo archivo de dependencias con cuatro bloques (base, pruebas, Airflow y Spark) y
cada contexto instala el grupo que necesita. El PR #35 cerró el README y este checkpoint, el
#36 verificó la entrega en un clon limpio y el #37 enlazó el video final.

## 3. Verificación local (2026-11-13, sobre `main`)

| Comprobación | Resultado |
|---|---|
| `python -m pytest -q` | 70 passed |
| `python -m compileall -q producer spark sla airflow/dags tests scripts` | exit 0 |
| `python -m sla.contract samples/sla_event.json samples/sla_events_batch.json` | 1 + 120 eventos válidos |
| `python scripts/quality_gate.py` | QUALITY GATE: OK |
| `docker compose config --quiet` | exit 0 |
| Enlaces internos de la documentación | 0 rotos |
| `python -m sla.reporting samples/sla_events_batch.json` | 120 eventos, 25 entregas, 36.56 min, 12.00 % fuera de SLA |

## 4. Verificación en clon limpio

Se clonó el repositorio en una carpeta temporal y se siguió el README sin estado previo:

```text
Clon:           4a53d7b
Python:         3.13.14 (venv nuevo)
Instalación:    grupo base de requirements.txt (pyarrow 26.0.1)
pytest -q:      70 passed
producer:       120 eventos generados en reports/sla_events_batch.json
sla.contract:   1 + 120 eventos válidos
sla.reporting:  25 entregas, 36.56 min, 12.00 % fuera de SLA
compose config: exit 0
```

La instalación completa (`pip install -r requirements.txt`) exige Python 3.11 o 3.12 por
Airflow 2.9.3; en 3.13 se documenta el grupo base, que es el que usó esta verificación.

## 5. Release

```text
Tag:      v1.0.0
Commit:   c8292c4 (2026-11-13)
Release:  https://github.com/MateoMorales566/rapidgo-sla-equipo8/releases/tag/v1.0.0
Artefacto: rapidgo-sla-equipo8.zip
Publicado: 2026-11-13
```

El tag se movió al merge del PR #37 para que el ZIP publicado incluya el enlace del video
en el README y en la documentación. Después del tag, `main` solo avanzó con el PR #38
(registro documental del cierre).

## 6. Estado de la entrega

| Requisito | Estado |
|---|---|
| Release final `v1.0.0` publicado y con artefacto | ✅ |
| Repositorio ejecutable según el README, verificado en clon limpio | ✅ |
| CI en verde sobre el commit del tag y sobre `main` | ✅ |
| Tablero Kanban con las 13 tarjetas en DONE | ✅ |
| Documentación por semanas y ADR | ✅ |
| Video final publicado y enlazado | ✅ |

## 7. Video final

Grabado el 2026-11-13 y publicado en YouTube:

**[Proyecto Final Big Data SLA - RapidGo Equipo 8](https://youtu.be/LwqG7DeIxgM)**

- Duración: 10:47.
- Participantes: Andrés Morales, Julián Pérez y Kevin García.
- Contenido: problema de negocio, arquitectura y ADR, demo del pipeline con Docker (Kafka,
  Spark Structured Streaming, Data Lake y Airflow con el fallo controlado y la
  recuperación), recorrido por GitHub y resultados del lote de 120 eventos.
- Enlazado en el README y en las notas del release `v1.0.0`. No se versiona en el
  repositorio por tamaño.

## 8. Trazabilidad

- Issues: 13 de 13 cerrados; el tablero público es
  [RapidGo - Equipo 8 - SLA de entregas](https://github.com/users/MateoMorales566/projects/1)
  y las 13 tarjetas están en la columna DONE.
- CI sobre el commit del tag `c8292c4`: [run 43538679992](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/43538679992).
- CI de la PR #37: [run 43538541536](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/43538541536).
- CI sobre `main` en el merge del PR #35: [run 43459069626](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/43459069626)
  sobre `4a53d7b`.
- Ejecución manual de `NyawToo`: [run 43460938070](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/43460938070).
- Ejecución manual de `SantiagoG1309`: [run 43357845590](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/43357845590).
- Revisiones cruzadas: PRs #29, #30 y #34 aprobados por `NyawToo`.
- Release: [run 43357711364](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/43357711364).
- Evidencias: [`docs/evidencias.md`](../evidencias.md) y
  [`docs/releases.md`](../releases.md).
