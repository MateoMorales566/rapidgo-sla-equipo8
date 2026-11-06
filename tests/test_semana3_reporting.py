"""Pruebas de KPIs, deduplicación y escritura atómica del reporte."""

from __future__ import annotations

import json

from semana3.reporting import (
    build_report,
    read_report,
    save_report,
    summarize_records,
    summarize_sla,
)


def test_deduplicates_by_latest_event_time_not_file_order():
    rows = [
        # Llega primero el evento final, pero es el más reciente.
        {
            "order_id": "1",
            "event_time": "2026-01-02",
            "status": "delivered",
            "delivery_minutes": 50,
            "is_late": True,
        },
        # Llega después un estado anterior: no debe reemplazar al entregado.
        {"order_id": "1", "event_time": "2026-01-01", "status": "in_transit"},
        {
            "order_id": "2",
            "event_time": "2026-01-02",
            "status": "delivered",
            "delivery_minutes": 30,
            "is_late": False,
        },
    ]

    result = summarize_records(rows)

    assert result["pedidos_unicos"] == 2
    assert result["pedidos_entregados"] == 2
    assert result["tiempo_promedio_min"] == 40.0
    assert result["porcentaje_fuera_sla"] == 50.0
    assert result["pedidos_retrasados"] == 1


def test_summarize_sla_from_processed_parquet(sample_lake):
    result = summarize_sla([sample_lake / "processed"])

    assert result["pedidos_unicos"] == 4
    assert result["pedidos_entregados"] == 2
    assert result["pedidos_en_curso"] == 1
    assert result["pedidos_cancelados"] == 1
    assert result["tiempo_promedio_min"] == 60.0
    assert result["porcentaje_fuera_sla"] == 50.0
    assert result["alertas_sla_excedido"] == 1
    assert result["alertas_delivery_stopped"] == 1


def test_report_is_written_atomically(tmp_path):
    report = build_report(
        metrics={"pedidos_unicos": 3},
        quality={"passed": True},
        source_files=["a.parquet"],
    )
    output = save_report(report, tmp_path)

    assert output["size_bytes"] > 0
    assert read_report(output["path"])["metrics"]["pedidos_unicos"] == 3
    assert not list(tmp_path.glob("*.tmp"))


def test_report_content_is_json_serializable(tmp_path):
    output = save_report(
        build_report(metrics={"a": 1}, quality={"passed": True}), tmp_path
    )
    payload = json.loads(open(output["path"], encoding="utf-8").read())

    assert payload["metrics"] == {"a": 1}
    assert "generated_at" in payload
