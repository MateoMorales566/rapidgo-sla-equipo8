# Checkpoint Semana 1

## 1. Qué quedó diseñado

Se definió la arquitectura RapidGo para la variante 1 (SLA de entregas), el
contrato JSON, un evento de muestra, tres KPIs, dos alertas y la composición
preliminar de Kafka y Spark.

## 2. Decisión más importante

Los eventos usarán el topic `rapidgo.sla.events`, `order_id` como key y tres
particiones. Esto conserva el orden de los estados de un pedido y deja
paralelismo inicial para la futura implementación.

## 3. Riesgo principal

Los umbrales iniciales (10% fuera de SLA y 30 minutos de detención) pueden no
representar todas las zonas o franjas horarias. Se deben recalibrar con datos y
logs reales durante la implementación.

## 4. Plan de la Semana 2

Implementar el productor, crear el topic con la configuración definida,
consumir y validar eventos con Spark Structured Streaming, persistir las capas
Raw/Processed/Curated y agregar pruebas del contrato y de las reglas de alerta.
