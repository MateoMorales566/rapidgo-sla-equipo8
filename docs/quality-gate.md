# Quality Gate - Semana 4 (Variante 1: SLA de entregas)

Equipo 8 | Rama: `andres` | Issue: #10 | Fecha de ejecución: 2026-11-11
Entorno: Docker Desktop (Windows), Kafka 7.6.1 (Confluent), Spark 3.5.3, Airflow 2.9.3.

## 1. Qué valida el gate

| Validación | Comando | Resultado |
|---|---|---|
| Sintaxis | `py -m compileall producer spark tests airflow scripts` | OK, sin errores |
| Pruebas | `py -m pytest -q` | 70 passed in 6.11s |
| Docker Compose | `docker compose config --quiet` | OK, sin errores |
| Estructura / docs / DAG | `py scripts\quality_gate.py` | `QUALITY GATE: OK` |

`scripts/quality_gate.py` verifica: archivos y carpetas obligatorios, checkpoints de las semanas 1 a 3,
palabras clave del README, al menos 3 ADR en `docs/decisions/` y un DAG con 5 o más tasks
(el DAG del equipo tiene 6 `PythonOperator`). Si algo falla, termina con código 1 y el release no se crea.

## 2. Ejecución del pipeline completo (stack levantado con Docker)

Servicios en estado `Up`: zookeeper, kafka (healthy), producer, spark-master, spark-worker, spark-job y airflow.
`kafka-init` termina con `Exited` por diseño, porque solo crea el topic.

### Kafka
- Topic: `rapidgo.sla.events`, key = `order_id`, 3 particiones, replication factor 1.
- Offsets finales por partición: P0 = 128, P1 = 124, P2 = 139.
- **Total de eventos publicados: 391** (128 + 124 + 139).
- Log del producer: `Publicando en kafka:29092 topic=rapidgo.sla.events key=order_id team=equipo8`.

### Spark Structured Streaming
- El job `spark-job` procesa micro-lotes de forma continua. En el log aparecen los lotes 45 a 67.
- Cada lote curated calculó entre 2 y 4 KPIs.
- Se dispararon alertas en 3 de esos 23 lotes (lotes 51, 52 y 61), con 1 alerta cada uno;
  en el resto `alertas=0`.

### Data Lake (archivos Parquet)

| Capa | Archivos Parquet |
|---|---|
| raw | 249 |
| processed | 245 |
| curated | 268 |
| **Total** | **762** |

## 3. KPIs y alertas del reporte batch

Fuente: reporte generado por el DAG (`reports/sla_semana3_latest.json`), documentado en
`docs/evidencias/semana3.md`.

| KPI | Valor |
|---|---|
| Eventos totales | 120 |
| Pedidos únicos | 30 |
| Pedidos entregados / en curso / cancelados | 25 / 3 / 2 |
| Tiempo promedio de entrega | 36.56 min |
| % fuera de SLA | 12.0 % |
| Pedidos retrasados | 3 |

Alertas del reporte:
- `SLA_EXCEEDED` (SLA excedido): 3
- `DELIVERY_STOPPED` (entrega detenida): 3

Quality gate de datos dentro del DAG: columnas requeridas, filas presentes, sin duplicados e
inválidos dentro del umbral, los cuatro en `true`.

## 4. Orquestación con Airflow

- DAG: `rapidgo_sla_semana3`, 6 tasks (`PythonOperator`), `schedule=None`, sin errores de importación
  (`Has import errors: false`).
- Flujo: `validar_fuente` -> `perfilar_datos` -> `validar_calidad` -> `calcular_indicadores`
  -> `generar_reporte` -> `publicar_resultado`.
- Ejecución manual del DAG `rapidgo_sla_semana3`:
  - Run ID: `manual__2026-11-12T04:18:30.524871+00:00` (equivale al 2026-11-11 23:18:30, hora de Bogotá).
  - Estado: **success**, con las 6 tareas en verde.
  - Inicio 23:18:31, fin 23:19:19, duración 00:00:47.
  - Tipo de run: manual, disparado externamente (Externally triggered: True).
  
- Mecanismos de resiliencia ya validados en la Semana 3: `retries=2` por defecto, `retries=0` en
  `validar_calidad`, XCom entre tareas y fallo controlado `FALLO_CONTROLADO` con recuperación
  (ver `docs/evidencias/semana3.md`).

## 5. Evidencia en GitHub

| Evidencia | Enlace |
|---|---|
| PR del issue #10 | Pendiente: agregar el enlace después de crear el PR |
| Action en verde (CI RapidGo) | Pendiente: agregar tras el merge |
| Ejecución manual (workflow_dispatch) - Mateo | Pendiente |
| Ejecución manual (workflow_dispatch) - integrante 2 | Pendiente |
| Ejecución manual (workflow_dispatch) - integrante 3 | Pendiente |

## 6. Problemas encontrados y cómo se resolvieron

1. **Imagen `bitnami/spark:3.5.3` no encontrada.** Al ejecutar `docker compose up -d --build`, el daemon
   respondió `failed to resolve reference "docker.io/bitnami/spark:3.5.3": not found`, y Kafka y
   Zookeeper quedaron `Interrupted`. Solución: cambiar la imagen a `bitnamilegacy/spark:3.5.3` en los
   3 servicios de Spark de `docker-compose.yml` (`spark-master`, `spark-worker`, `spark-job`).
   Las rutas internas `/opt/bitnami/...` no cambian. Tras el cambio, el stack levantó completo.
2. **`python` y `pytest` no reconocidos en Windows.** El sistema redirigía `python` a la Microsoft Store.
   Se usó el lanzador `py` y `py -m pytest`. Los workflows en GitHub (Ubuntu) siguen usando `python`.
3. **Aviso transitorio del producer.** Al iniciar, el log mostró `Failed to acquire idempotence PID from
   broker ... Coordinator load in progress: retrying`. Es un reintento automático mientras el coordinador
   de Kafka termina de cargar; el producer continuó publicando (391 eventos en el topic).

## 7. Limitaciones conocidas

- `bitnamilegacy/spark` es un repositorio congelado que ya no recibe actualizaciones de seguridad;
  como trabajo futuro, migrar a la imagen `apache/spark`.
- Airflow corre con SQLite y `SequentialExecutor` (solo para demostración); en producción se
  recomienda Postgres y un executor paralelo.
- La contraseña de Airflow (`admin`/`admin`) es solo para la demo local.
