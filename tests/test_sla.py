"""Pruebas de las reglas de negocio de SLA (sin Kafka ni Spark)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from sla.rules import (
    calcular_kpis,
    calcular_tiempo_entrega,
    clasificar_cumplimiento,
    evaluar_alertas,
    evaluar_delivery_stopped,
    evaluar_sla_exceeded,
    reference_time,
)

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "samples" / "sla_event.json"


def load_sample() -> dict:
    with SAMPLE.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def iso(minutes: int) -> str:
    base = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)
    return (base + timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")


def test_event_has_required_fields():
    event = load_sample()
    required = [
        "event_id",
        "team_id",
        "event_type",
        "event_time",
        "order_id",
        "zone",
        "distance_km",
        "promised_at",
        "picked_up_at",
        "delivered_at",
        "status",
    ]
    for field in required:
        assert field in event, f"Campo faltante: {field}"


def test_event_status_valid():
    assert load_sample()["status"] in {
        "created",
        "picked_up",
        "in_transit",
        "delivered",
        "cancelled",
    }


def test_distance_positive():
    assert load_sample()["distance_km"] > 0


def test_event_time_es_iso_parseable():
    datetime.fromisoformat(load_sample()["event_time"].replace("Z", "+00:00"))


def test_clasificar_cumplimiento_normal():
    assert clasificar_cumplimiento(30, 45) == "CUMPLE"


def test_clasificar_cumplimiento_limite():
    assert clasificar_cumplimiento(45, 45) == "FUERA_SLA"  # fallo intencional temporal
    assert clasificar_cumplimiento(45.01, 45) == "FUERA_SLA"


def test_clasificar_cumplimiento_fuera_sla():
    assert clasificar_cumplimiento(60, 45) == "FUERA_SLA"


@pytest.mark.parametrize(
    "real,objetivo",
    [(-1, 45), (10, 0), (10, -5)],
)
def test_clasificar_cumplimiento_invalido(real, objetivo):
    with pytest.raises(ValueError):
        clasificar_cumplimiento(real, objetivo)


def test_calcular_tiempo_entrega_sample():
    event = load_sample()
    assert calcular_tiempo_entrega(event["picked_up_at"], event["delivered_at"]) == 35.0


def test_calcular_tiempo_entrega_invalido():
    with pytest.raises(ValueError):
        calcular_tiempo_entrega("2026-10-29T14:00:00Z", "2026-10-29T13:00:00Z")


def test_calcular_tiempo_entrega_sin_timestamp():
    assert calcular_tiempo_entrega(None, "2026-10-29T14:00:00Z") is None


def test_evaluar_sla_exceeded_activa_alerta():
    event = {
        "order_id": "order-1",
        "zone": "Norte",
        "status": "delivered",
        "promised_at": "2026-10-29T13:00:00Z",
        "delivered_at": "2026-10-29T13:30:00Z",
    }
    alert = evaluar_sla_exceeded(event)
    assert alert is not None
    assert alert["alert_code"] == "SLA_EXCEEDED"
    assert alert["minutes_late"] == 30.0
    assert alert["severity"] == "ALTA"


def test_evaluar_sla_exceeded_no_activa_si_llega_a_tiempo():
    event = {
        "order_id": "order-2",
        "zone": "Sur",
        "status": "delivered",
        "promised_at": "2026-10-29T13:30:00Z",
        "delivered_at": "2026-10-29T13:30:00Z",
    }
    assert evaluar_sla_exceeded(event) is None


def test_evaluar_delivery_stopped_activa_alerta():
    event = {
        "order_id": "order-3",
        "zone": "Centro",
        "status": "in_transit",
        "promised_at": "2026-10-29T12:00:00Z",
    }
    alert = evaluar_delivery_stopped(event, now="2026-10-29T12:45:00Z")
    assert alert is not None
    assert alert["alert_code"] == "DELIVERY_STOPPED"
    assert alert["minutes_since_promised"] == 45.0


def test_evaluar_delivery_stopped_limite_exacto():
    event = {
        "order_id": "order-4",
        "zone": "Centro",
        "status": "picked_up",
        "promised_at": "2026-10-29T12:00:00Z",
    }
    assert evaluar_delivery_stopped(event, now="2026-10-29T12:30:00Z") is None


def test_evaluar_delivery_stopped_no_activa_si_esta_entregado():
    event = {
        "order_id": "order-5",
        "zone": "Centro",
        "status": "delivered",
        "promised_at": "2026-10-29T12:00:00Z",
    }
    assert evaluar_delivery_stopped(event, now="2026-10-29T14:00:00Z") is None


def test_alertas_usan_el_ultimo_estado_del_pedido():
    events = [
        {
            "order_id": "order-6",
            "zone": "Sur",
            "status": "in_transit",
            "event_time": iso(10),
            "promised_at": iso(0),
            "delivered_at": None,
        },
        {
            "order_id": "order-6",
            "zone": "Sur",
            "status": "delivered",
            "event_time": iso(20),
            "promised_at": iso(30),
            "delivered_at": iso(20),
        },
    ]
    assert evaluar_alertas(events, now=iso(120)) == []


def test_calcular_kpis():
    events = [
        {
            "order_id": f"order-{index}",
            "zone": "Norte",
            "status": "delivered",
            "event_time": iso(index),
            "promised_at": iso(index + 40),
            "picked_up_at": iso(index),
            "delivered_at": iso(index + (30 if index < 5 else 60)),
        }
        for index in range(10)
    ]
    kpis = calcular_kpis(events, now=iso(100))
    assert kpis["pedidos_entregados"] == 10
    assert kpis["tiempo_promedio_min"] == 45.0
    assert kpis["pedidos_retrasados"] == 5
    assert kpis["porcentaje_fuera_sla"] == 50.0
    assert kpis["alertas_sla_excedido"] == 5


def test_reference_time_usa_el_maximo_event_time():
    events = [{"event_time": iso(0)}, {"event_time": iso(90)}, {"event_time": iso(30)}]
    assert reference_time(events) == datetime(2026, 10, 29, 13, 30, tzinfo=timezone.utc)
