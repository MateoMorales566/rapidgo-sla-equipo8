"""Orquestación batch de la Semana 3 para la variante SLA de RapidGo.

El paquete no importa Airflow: el DAG solo conecta estas funciones. Así las
reglas se pueden probar con pytest y ejecutar desde la línea de comandos sin
levantar el scheduler.
"""

from __future__ import annotations

__all__ = ["config", "orchestration", "quality", "reporting", "sample_data"]
