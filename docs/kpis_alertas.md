# KPIs y alertas - Variante SLA

Los umbrales son valores iniciales de diseño. Deben ajustarse después de observar datos
reales y documentar la evidencia en la siguiente entrega.

## KPIs mínimos

| KPI | Definición | Umbral/objetivo inicial | Implementación |
|---|---|---|---|
| Tiempo promedio de entrega | Promedio de `delivered_at - picked_up_at` en pedidos entregados | Reportar en minutos; objetivo <= 45 min | `sla.rules.calcular_kpis`, `curated/kpis` |
| Porcentaje fuera de SLA | `pedidos con delivered_at > promised_at / pedidos entregados * 100` | Objetivo <= 10% por ventana | `sla.rules.calcular_kpis`, `curated/kpis` |
| Pedidos retrasados | Conteo de pedidos entregados después de `promised_at` | Reportar por ventana y zona | `sla.rules.calcular_kpis`, `curated/kpis` |

La agregación de Spark añade `p95_entrega_min` y `pedidos_en_curso` por ventana de 15
minutos y zona.

## Reglas de alerta

| Código | Condición | Acción | Implementación |
|---|---|---|---|
| `SLA_EXCEEDED` | `delivered_at > promised_at` | Registrar alerta con pedido, zona y minutos de retraso | `sla.rules.evaluar_sla_exceeded`, `curated/alerts` |
| `DELIVERY_STOPPED` | Pedido no entregado durante más de 30 minutos después de `promised_at` | Registrar alerta de detención y conservar el último estado | `sla.rules.evaluar_delivery_stopped`, `curated/alerts` |

Ambas reglas existen en dos lugares con la misma semántica:

- `sla/rules.py`: versión pura, usada por `pytest` y por el reporte de lotes.
- `spark/transforms.py`: versión Spark, usada por el job streaming y la demo batch.

## Justificación inicial

El objetivo de 10% permite detectar degradación sin alertar por casos aislados, mientras
que 30 minutos representa una detención operacional relevante después del compromiso al
cliente. Ambos valores deben revisarse por zona y distribución real de tiempos.

## Calibración observada (Semana 2)

Con el lote reproducible de 120 eventos:

```text
Pedidos entregados: 25
Tiempo promedio de entrega: 36.56 min (objetivo <= 45 min)
Porcentaje fuera de SLA: 12.00% (objetivo <= 10%)
Pedidos retrasados: 3
Alertas SLA_EXCEEDED: 3
Alertas DELIVERY_STOPPED: 3
```

El promedio cumple el objetivo, pero el **12 % fuera de SLA supera el 10 %**. No se cambia
el umbral todavía: se documenta el hallazgo y se propone analizarlo por zona con un lote
más grande en la Semana 3.
