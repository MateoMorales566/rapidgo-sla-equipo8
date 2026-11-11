# Checkpoint Semana 3

## 1. Qué quedó funcionando

DAG `rapidgo_sla_semana3` con **6 tareas**, dependencias, XCom, retries y un quality
gate real sobre la capa `processed`:

```text
validar_fuente -> perfilar_datos -> validar_calidad
-> calcular_indicadores -> generar_reporte -> publicar_resultado
```

Airflow corre en `docker-compose.semana3.yml` (Airflow 2.9.3 + pyarrow). El DAG es
manual (`schedule=None`) y no toca los checkpoints de Spark.

## 2. Valor real observado

Ejecución con el lote reproducible de 120 eventos y 30 pedidos:

| Métrica | Valor |
|---|---|
| Eventos procesados | 120 |
| Pedidos únicos | 30 |
| Pedidos entregados | 25 |
| Tiempo promedio | **36.56 min** |
| Fuera de SLA | **12.0 %** |
| Pedidos retrasados | 3 |
| Alertas `SLA_EXCEEDED` | 3 |
| Alertas `DELIVERY_STOPPED` | 3 |
| Filas inválidas / duplicadas | 0 / 0 |

Run exitoso: `manual__2026-11-01T00:00:00-05:00` (6 tareas en `success`).
Reporte: `reports/sla_semana3_latest.json` (1250 bytes).

## 3. Decisión técnica

Airflow **no reemplaza** a Spark Structured Streaming: se usa un DAG batch bajo demanda
que lee `processed`, valida calidad, calcula KPIs y publica un reporte atómico. Se
transportan por XCom solo rutas y conteos, nunca filas. Justificación en
[`ADR-003`](../decisions/ADR-003-orquestacion-y-recuperacion.md).

## 4. Fallo controlado y recuperación

Se creó `airflow/flags/forzar_fallo_calidad` y se ejecutó el DAG. La tarea
`validar_calidad` falló con el mensaje real:

```text
RuntimeError: FALLO_CONTROLADO: bandera de calidad activa
(/opt/airflow/flags/forzar_fallo_calidad). Bórrela y vuelva a ejecutar el DAG.
```

Run fallido: `manual__2026-11-06T00:00:00-05:00` (`failed`). Tareas afectadas:

| Tarea | Estado |
|---|---|
| `validar_fuente` | `success` |
| `perfilar_datos` | `success` |
| `validar_calidad` | `failed` |
| `calcular_indicadores` | `upstream_failed` |
| `generar_reporte` | `upstream_failed` |
| `publicar_resultado` | `upstream_failed` |

Se eliminó la bandera y se lanzó un run nuevo: `manual__2026-11-07T00:00:00-05:00`
terminó en `success` y regeneró el reporte. El cambio que resolvió el problema fue
retirar la bandera; no se borraron Parquet ni checkpoints.

## 5. Trazabilidad

- Issues Semana 3: [#7 DAG](https://github.com/MateoMorales566/rapidgo-sla-equipo8/issues/7), [#8 calidad](https://github.com/MateoMorales566/rapidgo-sla-equipo8/issues/8), [#9 fallo y recuperación](https://github.com/MateoMorales566/rapidgo-sla-equipo8/issues/9).
- Pull Request de la rama `feature/semana3-orquestacion`: [#25](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/25), mergeado a `main` el 06/11/2026.
- CI `CI RapidGo`: job `dag-airflow` ejecuta importación, run exitoso, fallo controlado y recuperación. Run en verde: [42147681397](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/42147681397).
- Evidencia detallada: [`docs/evidencias/semana3.md`](../evidencias/semana3.md).
- Release `v0.3.0-orchestration`: publicado sobre el tag `9b396de`: [releases/tag/v0.3.0-orchestration](https://github.com/MateoMorales566/rapidgo-sla-equipo8/releases/tag/v0.3.0-orchestration).
