# KPIs y alertas - Variante SLA

Los umbrales son valores iniciales de diseño. Deben ajustarse después de
observar datos reales y documentar la evidencia en la siguiente entrega.

## KPIs mínimos

| KPI | Definición | Umbral/objetivo inicial |
|---|---|---|
| Tiempo promedio de entrega | Promedio de `delivered_at - picked_up_at` en pedidos entregados | Reportar en minutos; objetivo <= 45 min |
| Porcentaje fuera de SLA | `pedidos con delivered_at > promised_at / pedidos entregados * 100` | Objetivo <= 10% por ventana |
| Pedidos retrasados | Conteo de pedidos entregados después de `promised_at` | Reportar por ventana y zona |

## Reglas de alerta

| Código | Condición | Acción |
|---|---|---|
| `SLA_EXCEEDED` | `delivered_at > promised_at` | Registrar alerta con pedido, zona y minutos de retraso |
| `DELIVERY_STOPPED` | Pedido no entregado durante más de 30 minutos después de `promised_at` | Registrar alerta de detención y conservar el último estado |

## Justificación inicial

El objetivo de 10% permite detectar degradación sin alertar por casos aislados,
mientras que 30 minutos representa una detención operacional relevante después
del compromiso al cliente. Ambos valores deben revisarse por zona y distribución
real de tiempos.
