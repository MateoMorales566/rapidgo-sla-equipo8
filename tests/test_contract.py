"""Pruebas del contrato JSON de eventos SLA."""

from __future__ import annotations

import copy
from pathlib import Path

import pytest

from sla.contract import is_valid, load_events, load_schema, validate_event, validate_file

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "samples" / "sla_event.json"
BATCH = ROOT / "producer" / "sla_events_batch.json"


@pytest.fixture()
def schema():
    return load_schema()


@pytest.fixture()
def sample():
    return load_events(SAMPLE)[0]


def test_sample_valido(schema, sample):
    validate_event(sample, schema=schema)


def test_lote_completo_valido(schema):
    assert validate_file(BATCH, schema=schema) == 120


def test_campo_requerido_faltante(schema, sample):
    event = copy.deepcopy(sample)
    del event["order_id"]
    with pytest.raises(Exception):
        validate_event(event, schema=schema)


def test_campo_adicional_rechazado(schema, sample):
    event = copy.deepcopy(sample)
    event["campo_extra"] = "no permitido"
    with pytest.raises(Exception):
        validate_event(event, schema=schema)


def test_status_invalido(schema, sample):
    event = copy.deepcopy(sample)
    event["status"] = "perdido"
    with pytest.raises(Exception):
        validate_event(event, schema=schema)


def test_timestamp_invalido(schema, sample):
    event = copy.deepcopy(sample)
    event["event_time"] = "29-10-2026 12:00"
    with pytest.raises(Exception):
        validate_event(event, schema=schema)


def test_equipo_incorrecto(schema, sample):
    event = copy.deepcopy(sample)
    event["team_id"] = "equipo1"
    with pytest.raises(Exception):
        validate_event(event, schema=schema)


def test_distancia_cero_invalida(schema, sample):
    event = copy.deepcopy(sample)
    event["distance_km"] = 0
    assert not is_valid(event, schema=schema)
