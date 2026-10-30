# Checkpoint Semana 2

## 1. Qué quedó construido

Se implementó el pipeline completo de entrega de eventos con:
- **Producer funcional** que genera eventos `delivery_status` al topic `rapidgo.sla.events` con key `order_id`
- **Kafka** configurado con 3 particiones y topic `rapidgo.sla.events`
- **Spark Structured Streaming** consumiendo del topic con ≥3 transformaciones y ≥2 alertas SLA
- **Capas Data Lake** operativas: raw → processed → curated con archivos Parquet
- **CI básico** configurado: compileall, pytest, validación de contrato, docker compose config
- **GitHub Actions** activados en cada push/PR con workflow_dispatch

## 2. Decisiones más importantes

- Se mantuvo la key `order_id` con 3 particiones (definida en ADR-001) para preservar el orden de los estados del pedido.
- Las transformaciones en Spark incluyen: ventana de tiempo de 15 min, cálculo de tiempo real vs SLA, y marcado de estados de alerta.
- Los umbrales iniciales: 10% fuera de SLA y 30 min de detención (mismo checkpoint de Semana 1, por revisar con datos).

## 3. Problemas resueltos y evidencia

| Integrante | Problema abordado | PR | Solución |
|---|---|---|---|
| Andrés Morales | Producer que genera eventos válidos con esquema JSON y key order_id | #15 | Implementación del producer con validación de esquema contra contrato_evento.json |
| Julián Pérez | Conexión Spark Structured Streaming a Kafka con formato Parquet | #16 | Configuración de spark-sql con readStream desde Kafka topic, escritura en Parquet en datalake/processed |
| Kevin García | Pipeline CI que pasa en PR y valida KPIs mínimas | #17 | Workflow GitHub Actions con compileall + pytest + validación de contrato JSON |

## 4. Plan de la Semana 3

- Orquestación con Airflow DAGs (calidad, agregación y reporte)
- KPIs y alertas operativas en producción
- Release v0.3.0-orchestration
- Demo end-to-end y video ≤ 10 min