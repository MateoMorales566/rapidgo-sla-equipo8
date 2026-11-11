# Actividad de transferencia - GitHub Actions (variante SLA)

Esta actividad traduce la guía de GitHub Actions al proyecto RapidGo, variante 1
(SLA de entregas). No se copió el caso StockGuard: se usó su mecanismo para definir
checks propios del equipo.

## 1. Regla de negocio propia

```python
clasificar_cumplimiento(tiempo_real_min, tiempo_objetivo_min=45, tolerancia_min=0)
```

Devuelve `CUMPLE` si el tiempo real es menor o igual al objetivo más la tolerancia; en
caso contrario devuelve `FUERA_SLA`. Rechaza tiempos negativos y objetivos inválidos.
Está en `sla/rules.py` y se prueba sin Kafka ni Spark.

## 2. Casos de prueba (normal, límite, inválido/alerta)

| Caso | Entrada | Resultado esperado | Test |
|---|---|---|---|
| Normal | 30 min / 45 min | `CUMPLE` | `test_clasificar_cumplimiento_normal` |
| Límite | 45 min / 45 min | `CUMPLE` | `test_clasificar_cumplimiento_limite` |
| Límite fuera | 45.01 min / 45 min | `FUERA_SLA` | `test_clasificar_cumplimiento_limite` |
| Inválido | -1 min / 45 min | `ValueError` | `test_clasificar_cumplimiento_invalido` |
| Alerta SLA | `delivered_at > promised_at` | `SLA_EXCEEDED` | `test_evaluar_sla_exceeded_activa_alerta` |
| Alerta detención | 45 min desde `promised_at` | `DELIVERY_STOPPED` | `test_evaluar_delivery_stopped_activa_alerta` |

La suite completa tiene **38 pruebas** en `tests/`.

## 3. Carpetas que valida `compileall`

```bash
python -m compileall -q producer spark sla tests
```

Incluye el producer, los jobs de Spark, las reglas puras y las pruebas. No se valida
`airflow/` porque el DAG corresponde a la Semana 3.

## 4. Archivos y configuración que se comprueban

- `docs/contrato_evento.json` (JSON válido y esquema).
- `samples/sla_event.json` y `producer/sla_events_batch.json` (eventos válidos).
- `docker-compose.yml` (`docker compose config --quiet`).
- Existencia de `README.md`, `requirements.txt`, `.github/workflows`, `producer`,
  `spark`, `tests` y las tres capas del Data Lake.

## 5. Condición que impide un merge

El check **`Sintaxis, contrato y pruebas`** debe estar en verde. Si falla, no se hace
merge a `main`: el PR queda bloqueado hasta corregir el contrato, la sintaxis o las
pruebas. Se recomienda activar branch protection en `main` con los checks obligatorios.

## 6. Preguntas de análisis

### 1. Ejecución exitosa y ejecución fallida

- **Exitosa:** [run 40985893801](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985893801)
  (commit `e6cc5ce`), disparada por el PR #19. Todos los steps terminaron en verde.
- **Fallida intencionalmente:** [run 40985829295](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985829295)
  (commit `6f10a18`). Falló el step **Ejecutar pruebas**; los steps anteriores
  (`Instalar dependencias`, `Validar sintaxis`, `Validar contrato`) pasaron.
- **Corregida:** [run 40985938802](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985938802),
  ejecución manual con `workflow_dispatch` después de restaurar la prueba.
- **Manual:** [run 40985938802](https://github.com/MateoMorales566/rapidgo-sla-equipo8/actions/runs/40985938802),
  iniciada por `SantiagoG1309` desde **Actions → CI RapidGo → Run workflow**.

### 2. ¿Por qué `pytest` local no reemplaza Actions?

Porque Actions ejecuta en un **runner limpio** (Ubuntu, Python 3.12) e instala las
dependencias desde `requirements.txt`. Eso detecta errores que el equipo no ve en su
máquina: rutas relativas, paquetes inexistentes (el caso de `apache-spark`) y diferencias
entre Windows y Linux. Además, el resultado queda asociado al PR y al commit.

### 3. ¿Qué riesgo tiene hacer merge sin esperar los checks?

Que un contrato roto, una dependencia inexistente o una regla mal calculada llegue a
`main` y contamine el release. Sin checks, el error se descubre en la demo o en la
sustentación, cuando ya no hay tiempo de corregirlo ni historial claro de la causa.

### 4. Línea esencial del error de la ejecución fallida

```text
E       AssertionError: assert 'CUMPLE' == 'FUERA_SLA'
FAILED tests/test_sla.py::test_clasificar_cumplimiento_limite - AssertionError
```

El diagnóstico fue inmediato: el caso límite (45/45) debía ser `CUMPLE`, pero la prueba
modificada esperaba `FUERA_SLA`. El step de pruebas fue el único que falló, lo que
confirmó que la sintaxis, el contrato y las dependencias estaban bien.

### 5. Diferencia entre `uses:` y `run:`

- `uses:` reutiliza una acción publicada (por ejemplo `actions/checkout@v7` descarga el
  repositorio en el runner).
- `run:` ejecuta comandos de shell dentro del runner (por ejemplo `pytest -v`,
  `python -m compileall` o `docker compose config --quiet`).

### 6. ¿Qué aporta `${{ github.actor }}` en `run-name`?

Identifica quién disparó la ejecución. En este repositorio el run se llama
**“CI ejecutado por SantiagoG1309”**, lo que sirve como evidencia de participación
individual y permite reconstruir quién ejecutó el workflow manualmente.

### 7. Si el workflow pasa pero Docker falla, ¿Actions no sirve?

Actions verifica lo que el equipo le pidió verificar: sintaxis, contrato, pruebas y
configuración del compose. **No levanta Kafka ni Spark en cada push**, así que no prueba
el runtime completo. Un fallo de Docker puede venir de imágenes, red, permisos o
conectores; para eso está la demo manual y, en el futuro, un workflow programado de
integración.

### 8. Tres checks propuestos para la variante SLA

1. **Contrato JSON:** evita que un evento sin `team_id`, con estado inválido o con
   timestamp mal formado entre al pipeline.
2. **Reglas de SLA:** evita que `clasificar_cumplimiento` cambie el límite de 45 min o
   que las alertas dejen de dispararse.
3. **Docker Compose + estructura:** evita que el topic, las capas o el producer
   desaparezcan del repositorio antes de la demo.

### 9. ¿Cuál implementar primero?

El de **contrato y reglas**, porque son la lógica de negocio y se ejecutan en segundos.
Sin ellos, el resto de checks puede estar en verde mientras los KPIs son incorrectos.

### 10. ¿Qué prueba sería demasiado costosa o frágil para cada push?

Levantar **Kafka + Spark + Airflow** y esperar varios minutos de streaming en cada push.
Es lento, consume recursos y depende de imágenes/red. Se valida con una demo manual
documentada, con `docker compose config` en CI y con la demo batch de Spark
(`python -m spark.batch_demo`) sobre un lote reproducible.

## 7. Evidencias conservadas

- Ejecución exitosa: run 40985893801.
- Ejecución fallida intencional: run 40985829295.
- Ejecución corregida: run 40985938802.
- PR con checks: [#19](https://github.com/MateoMorales566/rapidgo-sla-equipo8/pull/19).
- Registro de la falla y corrección en `docs/evidencias/semana2.md`.
