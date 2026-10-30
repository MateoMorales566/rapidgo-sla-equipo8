"""Reglas de negocio de la variante 1 (SLA de entregas).

Este módulo es intencionalmente puro: no importa Kafka, Spark ni Airflow.
Así las reglas se pueden probar con pytest en cualquier runner de GitHub
Actions sin levantar infraestructura.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

TEAM_ID = "equipo8"

# Umbrales iniciales documentados en docs/kpis_alertas.md.
SLA_TARGET_MINUTES = 45.0
OUTSIDE_SLA_THRESHOLD_PCT = 10.0
STOPPED_THRESHOLD_MINUTES = 30.0

VALID_STATUSES = ("created", "picked_up", "in_transit", "delivered", "cancelled")
FINAL_STATUSES = frozenset({"delivered", "cancelled"})


def _as_number(value: Any, name: str) -> float:
    """Convierte a float y rechaza bool/None/texto para evitar comparaciones raras."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} debe ser numérico, se recibió {value!r}")
    return float(value)


def parse_timestamp(value: Any) -> datetime | None:
    """Convierte un timestamp ISO-8601 (con o sin Z) a datetime UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    else:
        raise ValueError(f"Timestamp no soportado: {value!r}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_iso(value: datetime) -> str:
    """Serializa un datetime a ISO-8601 UTC con milisegundos y sufijo Z."""
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def clasificar_cumplimiento(
    tiempo_real_min: float,
    tiempo_objetivo_min: float = SLA_TARGET_MINUTES,
    tolerancia_min: float = 0.0,
) -> str:
    """Clasifica una entrega como CUMPLE o FUERA_SLA.

    Es la regla de negocio que se prueba en CI sin necesidad de Kafka.
    Un tiempo real igual al objetivo se considera dentro del SLA.
    """
    real = _as_number(tiempo_real_min, "tiempo_real_min")
    objetivo = _as_number(tiempo_objetivo_min, "tiempo_objetivo_min")
    tolerancia = _as_number(tolerancia_min, "tolerancia_min")

    if real < 0:
        raise ValueError("El tiempo real no puede ser negativo")
    if objetivo <= 0:
        raise ValueError("El tiempo objetivo debe ser mayor que cero")
    if tolerancia < 0:
        raise ValueError("La tolerancia no puede ser negativa")

    return "CUMPLE" if real <= objetivo + tolerancia else "FUERA_SLA"


def calcular_tiempo_entrega(picked_up_at: Any, delivered_at: Any) -> float | None:
    """Devuelve la duración de la entrega en minutos, o None si falta un timestamp."""
    inicio = parse_timestamp(picked_up_at)
    fin = parse_timestamp(delivered_at)
    if inicio is None or fin is None:
        return None
    if fin < inicio:
        raise ValueError("delivered_at no puede ser anterior a picked_up_at")
    return round((fin - inicio).total_seconds() / 60.0, 2)


def evaluar_sla_exceeded(event: Mapping[str, Any]) -> dict[str, Any] | None:
    """Regla SLA_EXCEEDED: delivered_at > promised_at."""
    delivered = parse_timestamp(event.get("delivered_at"))
    promised = parse_timestamp(event.get("promised_at"))
    if delivered is None or promised is None:
        return None

    minutes_late = (delivered - promised).total_seconds() / 60.0
    if minutes_late <= 0:
        return None

    return {
        "alert_code": "SLA_EXCEEDED",
        "order_id": event.get("order_id"),
        "zone": event.get("zone"),
        "status": event.get("status"),
        "minutes": round(minutes_late, 2),
        "minutes_late": round(minutes_late, 2),
        "promised_at": to_iso(promised),
        "delivered_at": to_iso(delivered),
        "severity": "ALTA" if minutes_late >= 15 else "MEDIA",
    }


def evaluar_delivery_stopped(
    event: Mapping[str, Any],
    now: Any = None,
) -> dict[str, Any] | None:
    """Regla DELIVERY_STOPPED: pedido sin entregar > 30 min después de promised_at."""
    if event.get("status") in FINAL_STATUSES:
        return None

    promised = parse_timestamp(event.get("promised_at"))
    if promised is None:
        return None

    reference = parse_timestamp(now) if now is not None else datetime.now(timezone.utc)
    if reference is None:
        return None

    minutes_stopped = (reference - promised).total_seconds() / 60.0
    if minutes_stopped <= STOPPED_THRESHOLD_MINUTES:
        return None

    return {
        "alert_code": "DELIVERY_STOPPED",
        "order_id": event.get("order_id"),
        "zone": event.get("zone"),
        "status": event.get("status"),
        "minutes": round(minutes_stopped, 2),
        "minutes_since_promised": round(minutes_stopped, 2),
        "promised_at": to_iso(promised),
        "reference_at": to_iso(reference),
        "severity": "ALTA" if minutes_stopped >= STOPPED_THRESHOLD_MINUTES * 2 else "MEDIA",
    }


def latest_events(events: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Conserva el último estado conocido de cada pedido."""
    latest: dict[str, tuple[datetime, Mapping[str, Any]]] = {}
    for event in events:
        order_id = str(event.get("order_id"))
        moment = parse_timestamp(event.get("event_time")) or datetime.min.replace(tzinfo=timezone.utc)
        if order_id not in latest or moment >= latest[order_id][0]:
            latest[order_id] = (moment, event)
    return [item[1] for item in latest.values()]


def evaluar_alertas(
    events: Iterable[Mapping[str, Any]],
    now: Any = None,
) -> list[dict[str, Any]]:
    """Aplica ambas reglas al último estado de cada pedido y deduplica."""
    alerts: dict[tuple[str, Any], dict[str, Any]] = {}
    for event in latest_events(events):
        for alert in (evaluar_sla_exceeded(event), evaluar_delivery_stopped(event, now=now)):
            if alert is None:
                continue
            key = (alert["alert_code"], alert["order_id"])
            if key not in alerts or alert["minutes"] > alerts[key]["minutes"]:
                alerts[key] = alert

    return sorted(alerts.values(), key=lambda item: (item["alert_code"], str(item["order_id"])))


def calcular_kpis(
    events: Iterable[Mapping[str, Any]],
    now: Any = None,
) -> dict[str, Any]:
    """Calcula los tres KPIs mínimos de la variante y resume las alertas."""
    events = list(events)
    delivered = [
        event
        for event in events
        if event.get("status") == "delivered"
        and event.get("delivered_at")
        and event.get("picked_up_at")
    ]

    durations: list[float] = []
    late = 0
    invalid = 0

    for event in delivered:
        try:
            duration = calcular_tiempo_entrega(
                event.get("picked_up_at"), event.get("delivered_at")
            )
        except (ValueError, TypeError):
            invalid += 1
            continue
        if duration is None:
            invalid += 1
            continue

        durations.append(duration)
        promised = parse_timestamp(event.get("promised_at"))
        if promised is not None and parse_timestamp(event.get("delivered_at")) > promised:
            late += 1

    alerts = evaluar_alertas(events, now=now)
    total_delivered = len(durations)

    return {
        "eventos": len(events),
        "pedidos_entregados": total_delivered,
        "tiempo_promedio_min": round(sum(durations) / total_delivered, 2) if total_delivered else None,
        "porcentaje_fuera_sla": round(100.0 * late / total_delivered, 2) if total_delivered else 0.0,
        "pedidos_retrasados": late,
        "alertas_sla_excedido": sum(
            1 for alert in alerts if alert["alert_code"] == "SLA_EXCEEDED"
        ),
        "alertas_delivery_stopped": sum(
            1 for alert in alerts if alert["alert_code"] == "DELIVERY_STOPPED"
        ),
        "eventos_invalidos": invalid,
        "umbral_sla_min": SLA_TARGET_MINUTES,
        "umbral_fuera_sla_pct": OUTSIDE_SLA_THRESHOLD_PCT,
        "umbral_detencion_min": STOPPED_THRESHOLD_MINUTES,
    }


def reference_time(events: Iterable[Mapping[str, Any]]) -> datetime:
    """Usa el máximo event_time como reloj de referencia para analizar lotes históricos."""
    timestamps = [parse_timestamp(event.get("event_time")) for event in events]
    valid = [ts for ts in timestamps if ts is not None]
    return max(valid) if valid else datetime.now(timezone.utc)


__all__ = [
    "FINAL_STATUSES",
    "OUTSIDE_SLA_THRESHOLD_PCT",
    "SLA_TARGET_MINUTES",
    "STOPPED_THRESHOLD_MINUTES",
    "TEAM_ID",
    "VALID_STATUSES",
    "calcular_kpis",
    "calcular_tiempo_entrega",
    "clasificar_cumplimiento",
    "evaluar_alertas",
    "evaluar_delivery_stopped",
    "evaluar_sla_exceeded",
    "parse_timestamp",
    "reference_time",
    "to_iso",
]
