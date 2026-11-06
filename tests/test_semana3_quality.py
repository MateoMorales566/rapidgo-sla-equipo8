"""Pruebas del perfilado y del quality gate de la capa processed."""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest

from semana3.quality import (
    QualityGateError,
    evaluate_quality,
    inspect_parquet,
    resolve_parquet_paths,
)
from semana3.sample_data import build_processed_rows, write_processed


def good_profile() -> dict:
    return {
        "columns": [
            "event_id",
            "team_id",
            "order_id",
            "status",
            "zone",
            "event_time",
            "promised_at",
        ],
        "total_rows": 1,
        "invalid_count": 0,
        "duplicate_event_ids": 0,
        "sample_problems": [],
    }


def test_inspect_parquet_profiles_real_file(sample_lake):
    profile = inspect_parquet([sample_lake / "processed"])

    assert profile["files"] == 1
    assert profile["total_rows"] == 4
    assert profile["invalid_count"] == 0
    assert profile["duplicate_event_ids"] == 0
    assert profile["min_event_time"].startswith("2026-11-01T08:50:00")
    assert profile["max_event_time"].startswith("2026-11-01T10:30:00")
    assert "order_id" in profile["columns"]


def test_quality_passes_with_valid_lake(sample_lake):
    profile = inspect_parquet([sample_lake / "processed"])
    result = evaluate_quality(profile)

    assert result["passed"] is True
    assert result["checks"] == {
        "columnas_requeridas": True,
        "filas_presentes": True,
        "sin_duplicados": True,
        "invalidos_dentro_de_umbral": True,
    }


def test_quality_rejects_missing_column():
    profile = good_profile()
    profile["columns"].remove("zone")

    with pytest.raises(QualityGateError, match="QUALITY_GATE_FAILED.*zone"):
        evaluate_quality(profile)


def test_quality_rejects_no_rows():
    profile = good_profile()
    profile["total_rows"] = 0

    with pytest.raises(QualityGateError, match="QUALITY_GATE_FAILED"):
        evaluate_quality(profile)


def test_quality_rejects_invalid_rows_by_default():
    profile = good_profile()
    profile["invalid_count"] = 1
    profile["sample_problems"] = ["Fila 0: status inválido"]

    with pytest.raises(QualityGateError, match="QUALITY_GATE_FAILED"):
        evaluate_quality(profile)


def test_quality_allows_configured_invalid_ratio():
    profile = good_profile()
    profile["total_rows"] = 100
    profile["invalid_count"] = 2

    assert evaluate_quality(profile, max_invalid_ratio_override=0.02)["passed"] is True
    with pytest.raises(QualityGateError):
        evaluate_quality(profile, max_invalid_ratio_override=0.01)


def test_quality_rejects_duplicate_event_ids():
    profile = good_profile()
    profile["duplicate_event_ids"] = 1

    with pytest.raises(QualityGateError, match="duplicados"):
        evaluate_quality(profile)


def test_inspect_detects_duplicated_event_ids(tmp_path, sample_events):
    duplicated = [sample_events[0], dict(sample_events[0])]
    rows, skipped = build_processed_rows(duplicated)
    assert skipped == 0
    write_processed(rows, tmp_path, overwrite=True)

    profile = inspect_parquet([tmp_path / "processed"])
    assert profile["total_rows"] == 2
    assert profile["duplicate_event_ids"] == 1
    assert any("duplicado" in problem for problem in profile["sample_problems"])


def test_resolve_parquet_paths_ignores_checkpoints(sample_lake):
    checkpoint = sample_lake / "processed" / "_checkpoints" / "part.parquet"
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_bytes(b"no es un parquet real")

    found = resolve_parquet_paths([sample_lake / "processed"])

    assert len(found) == 1
    assert "_checkpoints" not in found[0].parts


def test_parquet_has_expected_enriched_columns(sample_lake):
    table = pq.read_table(sample_lake / "processed" / "part-00000.parquet")
    assert {"delivery_minutes", "minutes_late", "sla_status", "is_late"} <= set(
        table.column_names
    )
