# RapidGo SLA - Variantes 1-10

Proyecto final de Big Data para RapidGo, variante 1: **SLA de entregas**.

## Equipo

| Integrante | Responsabilidad inicial |
|---|---|
| Andrés Morales | Productor de eventos y definición del contrato |
| Julián Pérez | Arquitectura, Kafka y Data Lake |
| Kevin García | KPIs, alertas y documentación |

## Problema de negocio

RapidGo necesita identificar retrasos e incumplimientos de los tiempos prometidos de entrega. La solución capturará eventos de pedidos, calculará el tiempo real de entrega frente al SLA y generará alertas cuando el SLA sea excedido o una entrega permanezca detenida demasiado tiempo.

## Entregas y releases

| Release | Semana | Estado |
|---|---|---|
| `v0.1.0-foundation` | 1 | ✅ Estructura base, arquitectura, contrato, KPIs, checkpoint |
| `v0.2.0-streaming` | 2 | 🚧 Pipeline Kafka/Spark, CI básico, PRs con Actions (en desarrollo) |
| `v0.3.0-orchestration` | 3 | ⏳ Por iniciar - Orquestación Airflow |
| `v1.0.0` | 4 | ⏳ Por iniciar - Release final y demo |

## Alcance de la Semana 1 (`v0.1.0-foundation`)

Base de diseño y estructura inicial. Incluye:

- Arquitectura específica de SLA;
- Contrato JSON y evento de ejemplo;
- Tres KPIs y dos reglas de alerta con umbrales iniciales;
- Decisión de topic, key y particiones;
- Estructura inicial del proyecto y Docker Compose preliminar;
- ADR-001 y checkpoint de Semana 1.

La implementación funcional de Kafka, Spark, Airflow, pruebas automatizadas y CI corresponde a las semanas posteriores.

## Alcance de la Semana 2 (`v0.2.0-streaming`)

Desarrollo del núcleo del pipeline. Incluye:

- Producer funcional sobre Kafka con key `order_id` y 3 particiones;
- Spark Structured Streaming procesando eventos en tiempo real;
- ≥3 transformaciones y ≥2 alertas SLA operativas;
- Persistencia en Data Lake con capas Raw/Processed/Curated (Parquet);
- CI básico activado: compileall, pytest, contrato/evento, docker compose config;
- GitHub Actions en cada push/PR con workflow_dispatch;
- 1 PR sustancial por integrante con evidencia del problema real resuelto.

## Arquitectura

```text
producer/ -> Kafka topic rapidgo.sla.events (key: order_id, 3 particiones)
  -> Spark Structured Streaming
    -> datalake/raw -> datalake/processed -> datalake/curated
    -> Airflow (calidad, agregación y reporte; Semana 3)
    -> KPIs y alertas SLA
```

La arquitectura detallada y sus responsabilidades están en [`docs/arquitectura.md`](docs/arquitectura.md). El diagrama visual está en [`docs/arquitectura.svg`](docs/arquitectura.svg).

## Contrato y métricas

- Contrato: [`docs/contrato_evento.json`](docs/contrato_evento.json)
- Evento de muestra: [`samples/sla_event.json`](samples/sla_event.json)
- KPIs y alertas: [`docs/kpis_alertas.md`](docs/kpis_alertas.md)
- Decisión de Kafka: [`docs/decisions/ADR-001-kafka-sla.md`](docs/decisions/ADR-001-kafka-sla.md)
- Checkpoint Semana 1: [`docs/checkpoints/semana1.md`](docs/checkpoints/semana1.md)
- Checkpoint Semana 2: [`docs/checkpoints/semana2.md`](docs/checkpoints/semana2.md)

## Ejecución prevista

El archivo [`docker-compose.yml`](docker-compose.yml) es preliminar y prepara Kafka y Spark para las siguientes semanas. La implementación del productor y del procesamiento se incorporará después de la entrega base.

Cada release debe contar una parte de la historia del proyecto.