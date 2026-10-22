# RapidGo SLA - Semana 1

Proyecto final de Big Data para RapidGo, variante 1: **SLA de entregas**.

## Equipo

| Integrante | Responsabilidad inicial |
|---|---|
| Andrés Morales | Productor de eventos y definición del contrato |
| Julián Pérez | Arquitectura, Kafka y Data Lake |
| Kevin García | KPIs, alertas y documentación |

## Problema de negocio

RapidGo necesita identificar retrasos e incumplimientos de los tiempos prometidos
de entrega. La solución capturará eventos de pedidos, calculará el tiempo real de
entrega frente al SLA y generará alertas cuando el SLA sea excedido o una entrega
permanezca detenida demasiado tiempo.

## Alcance de esta entrega

Esta es la base de diseño de la Semana 1 (`v0.1.0-foundation`). Incluye:

- arquitectura específica de SLA;
- contrato JSON y evento de ejemplo;
- tres KPIs y dos reglas de alerta con umbrales iniciales;
- decisión de topic, key y particiones;
- estructura inicial del proyecto y Docker Compose preliminar;
- ADR-001 y checkpoint de Semana 1.

La implementación funcional de Kafka, Spark, Airflow, pruebas automatizadas y CI
corresponde a las semanas posteriores.

## Arquitectura

```text
producer/
  -> Kafka topic rapidgo.sla.events (key: order_id, 3 particiones)
  -> Spark Structured Streaming
  -> datalake/raw -> datalake/processed -> datalake/curated
  -> Airflow (calidad, agregación y reporte; Semana 3)
  -> KPIs y alertas SLA
```

La arquitectura detallada y sus responsabilidades están en
[`docs/arquitectura.md`](docs/arquitectura.md).
El diagrama visual está en [`docs/arquitectura.svg`](docs/arquitectura.svg).

## Contrato y métricas

- Contrato: [`docs/contrato_evento.json`](docs/contrato_evento.json)
- Evento de muestra: [`samples/sla_event.json`](samples/sla_event.json)
- KPIs y alertas: [`docs/kpis_alertas.md`](docs/kpis_alertas.md)
- Decisión de Kafka: [`docs/decisions/ADR-001-kafka-sla.md`](docs/decisions/ADR-001-kafka-sla.md)

## Ejecución prevista

El archivo [`docker-compose.yml`](docker-compose.yml) es preliminar y prepara
Kafka y Spark para las siguientes semanas. La implementación del productor y del
procesamiento se incorporará después de esta entrega.
