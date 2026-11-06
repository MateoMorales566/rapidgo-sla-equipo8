"""Pruebas del generador de capa processed para demos y CI."""

from __future__ import annotations

import json

import pytest

from semana3.sample_data import (
    build_processed_rows,
    load_events,
    main,
    write_processed,
)


def test_build_processed_rows_calculates_sla(sample_events):
    rows, skipped = build_processed_rows(sample_events)

    assert skipped == 0
    by_order = {row["order_id"]: row for row in rows}
    assert by_order["order-1"]["delivery_minutes"] == 30.0
    assert by_order["order-1"]["sla_status"] == "CUMPLE"
    assert by_order["order-2"]["delivery_minutes"] == 90.0
    assert by_order["order-2"]["sla_status"] == "FUERA_SLA"
    assert by_order["order-2"]["is_late"] is True
    assert by_order["order-3"]["sla_status"] == "PENDIENTE"
    assert by_order["order-4"]["sla_status"] == "CANCELADO"


def test_build_processed_rows_skips_invalid_events(sample_events):
    invalid = dict(sample_events[0])
    invalid["event_id"] = "evt-otro-equipo"
    invalid["team_id"] = "equipo7"
    invalid_status = dict(sample_events[0])
    invalid_status["event_id"] = "evt-estado"
    invalid_status["status"] = "unknown"

    rows, skipped = build_processed_rows(sample_events + [invalid, invalid_status])

    assert len(rows) == len(sample_events)
    assert skipped == 2


def test_write_processed_refuses_overwrite(sample_events, tmp_path):
    rows, _ = build_processed_rows(sample_events)
    write_processed(rows, tmp_path, overwrite=False)

    with pytest.raises(FileExistsError):
        write_processed(rows, tmp_path, overwrite=False)

    assert write_processed(rows, tmp_path, overwrite=True)["rows"] == len(rows)


def test_load_events_rejects_non_list(tmp_path):
    path = tmp_path / "events.json"
    path.write_text(json.dumps({"event_id": "x"}), encoding="utf-8")

    with pytest.raises(ValueError, match="lista"):
        load_events(path)


def test_cli_writes_sample_lake(tmp_path, sample_events):
    source = tmp_path / "events.json"
    source.write_text(json.dumps(sample_events), encoding="utf-8")
    output = tmp_path / "lake"

    assert main(["--input", str(source), "--output", str(output)]) == 0
    assert (output / "processed" / "part-00000.parquet").is_file()
