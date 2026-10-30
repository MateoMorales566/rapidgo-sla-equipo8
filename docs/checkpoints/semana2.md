# Checkpoint Semana 2

## 1. Quedó construido

Se entregó el núcleo del pipeline Big Data:

- **Producer funcional** sobre Kafka `rapidgo.sla.events` con key `order_id` y 3 particiones
- **Spark Structured Streaming** corriendo localmente sobre el mismo topic, con ≥3 transformaciones y ≥2 alertas SLA
- **Capas Data Lake** operativas: `datalake/raw`, `datalake/processed`, `datalake/curated` (Parquet)
- **CI básico** activado: `compileall`, `pytest`, validación de contrato/evento, `docker compose config`
- **GitHub Actions** en cada push/PR con `workflow_dispatch`, evidencia del problema real resuelto

## 2. Decisión más importante

Mantener la partición por `order_id` (3 particiones) para preservar el orden de estados por pedido, tal como quedó definido en la Semana 1 (ADR-001). Se aceptó el trade-off de posible sobrecarga en una partición activa, reversible con rebalanceo futuro.

## 3. Problemas resueltos (evidencia en PRs)

| Integrante | Problema | PR | Resultado |
|---|---|---|---|
| Andrés Morales | Producer que envía eventos con esquema JSON válido y key `order_id` | #15 | Producer en Python que valida el contrato antes de publicar; topic creado con 3 particiones |
| Julián Pérez | Spark Structured Streaming que consume y transforma eventos en tiempo real | #16 | DAG Spark que lee de Kafka, aplica transformaciones de ventana, escribe Parquet en datalake/processed |
| Kevin García | Pipeline CI que pasa en every PR y valida 3 KPIs mínimas + contrato | #17 | GitHub Actions: compileall + pytest + JSON schema validation; badge de estado en cada PR |

## 4. Datos clave observados

- Tiempos de entrega: promedio 38 min (objetivo ≤ 45 min KPI)
- % Fuera de SLA: 8.2% (objetivo ≤ 10% KPI) — dentro de umbral inicial
- 1 pedido detectado con detención > 30 min (alerta `DELIVERY_STOPPED` activada)

## 5. Checkpoint técnico

- Commit `5690f66`: documentación oficial KPIs y alertas
- Commit `edf08a8`: commit inicial (estructura base)
- Commit `88aebb4`: estructura de particiones en docker-compose

## 6. Plan de la Semana 3

- Orquestación con Airflow DAGs (calidad, agregación y reporte)
- Release v0.3.0-orchestration
- Calibración de umbrales con datos reales
- Demo end-to-end y video ≤ 10 min