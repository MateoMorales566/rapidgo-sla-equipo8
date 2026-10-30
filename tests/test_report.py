"""Pruebas del reporte de KPIs y alertas."""

from __future__ import annotations

from pathlib import Path

from sla.contract import load_events
from sla.report import build_report, format_report

ROOT = Path(__file__).resolve().parents[1]
BATCH = ROOT / "producer" / "sla_events_batch.json"


def test_reporte_incluye_kpis_y_alertas():
    report = build_report(load_events(BATCH))
    assert report["kpis"]["eventos"] == 120
    assert report["kpis"]["pedidos_entregados"] > 0
    assert report["kpis"]["tiempo_promedio_min"] > 0
    assert 0 <= report["kpis"]["porcentaje_fuera_sla"] <= 100


def test_formato_legible():
    text = format_report(build_report(load_events(BATCH)))
    assert "Tiempo promedio de entrega" in text
    assert "Porcentaje fuera de SLA" in text
    assert "DELIVERY_STOPPED" in text
