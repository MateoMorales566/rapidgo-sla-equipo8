"""Pruebas del generador de eventos (sin broker Kafka)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from producer.events import OrderSimulator, event_key
from sla.contract import load_schema, validate_event

START = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)


def test_batch_genera_eventos_validos():
    schema = load_schema()
    events = OrderSimulator(seed=7).generate_batch(120, start=START)
    assert len(events) == 120
    for event in events:
        validate_event(event, schema=schema)


def test_batch_es_reproducible_con_la_misma_semilla():
    first = OrderSimulator(seed=11).generate_batch(20, start=START)
    second = OrderSimulator(seed=11).generate_batch(20, start=START)
    assert [event["order_id"] for event in first] == [event["order_id"] for event in second]
    assert [event["status"] for event in first] == [event["status"] for event in second]


def test_eventos_identificables_por_equipo():
    events = OrderSimulator(seed=3).generate_batch(20, start=START)
    assert {event["team_id"] for event in events} == {"equipo8"}


def test_key_de_kafka_es_el_order_id():
    event = OrderSimulator(seed=3).generate_batch(1, start=START)[0]
    assert event_key(event) == event["order_id"]


def test_tiempos_coherentes_en_entregas():
    events = OrderSimulator(seed=5).generate_batch(120, start=START)
    delivered = [event for event in events if event["status"] == "delivered"]
    assert delivered, "El lote debe incluir entregas"
    for event in delivered:
        picked = datetime.fromisoformat(event["picked_up_at"].replace("Z", "+00:00"))
        delivered_at = datetime.fromisoformat(event["delivered_at"].replace("Z", "+00:00"))
        assert delivered_at >= picked


def test_eventos_no_entregados_no_tienen_delivered_at():
    events = OrderSimulator(seed=9).generate_batch(80, start=START)
    for event in events:
        if event["status"] != "delivered":
            assert event["delivered_at"] is None


def test_modo_en_vivo_avanza_estados():
    simulator = OrderSimulator(seed=4)
    clock = START
    events = []
    for _ in range(40):
        events.append(simulator.next_event(clock))
        clock += timedelta(minutes=1)
    validate_event(events[0], schema=load_schema())
    assert {"created", "picked_up", "in_transit", "delivered"}.intersection(
        {event["status"] for event in events}
    )
