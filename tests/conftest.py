"""Fixtures compartidas por las pruebas del pipeline y de la Semana 3."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sla.sample_data import build_processed_rows, write_processed


@pytest.fixture
def sample_events() -> list[dict]:
    """Cuatro pedidos: a tiempo, tarde, detenido y cancelado."""

    base = {
        "team_id": "equipo8",
        "event_type": "delivery_status",
    }
    return [
        {
            **base,
            "event_id": "evt-1",
            "event_time": "2026-11-01T08:50:00Z",
            "order_id": "order-1",
            "zone": "Centro",
            "distance_km": 5.0,
            "promised_at": "2026-11-01T09:00:00Z",
            "picked_up_at": "2026-11-01T08:20:00Z",
            "delivered_at": "2026-11-01T08:50:00Z",
            "status": "delivered",
        },
        {
            **base,
            "event_id": "evt-2",
            "event_time": "2026-11-01T09:30:00Z",
            "order_id": "order-2",
            "zone": "Norte",
            "distance_km": 9.5,
            "promised_at": "2026-11-01T09:00:00Z",
            "picked_up_at": "2026-11-01T08:00:00Z",
            "delivered_at": "2026-11-01T09:30:00Z",
            "status": "delivered",
        },
        {
            **base,
            "event_id": "evt-3",
            "event_time": "2026-11-01T10:30:00Z",
            "order_id": "order-3",
            "zone": "Sur",
            "distance_km": 7.0,
            "promised_at": "2026-11-01T09:00:00Z",
            "picked_up_at": "2026-11-01T08:45:00Z",
            "delivered_at": None,
            "status": "in_transit",
        },
        {
            **base,
            "event_id": "evt-4",
            "event_time": "2026-11-01T09:05:00Z",
            "order_id": "order-4",
            "zone": "Oeste",
            "distance_km": 3.0,
            "promised_at": "2026-11-01T09:00:00Z",
            "picked_up_at": None,
            "delivered_at": None,
            "status": "cancelled",
        },
    ]


@pytest.fixture
def sample_lake(tmp_path, sample_events) -> Path:
    """Data Lake temporal con una capa processed válida."""

    rows, _ = build_processed_rows(sample_events)
    write_processed(rows, tmp_path, overwrite=True)
    return tmp_path
