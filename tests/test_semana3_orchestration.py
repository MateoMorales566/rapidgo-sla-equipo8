"""Pruebas de las tareas de orquestación sin levantar Airflow."""

from __future__ import annotations

from pathlib import Path

import pytest

from semana3.orchestration import (
    TASK_IDS,
    _FakeTI,
    quality_gate,
    run_pipeline,
    validate_source,
)
from semana3.quality import inspect_parquet


def test_validate_source_requires_processed(tmp_path, monkeypatch):
    monkeypatch.setenv("RAPIDGO_DATALAKE", str(tmp_path))

    with pytest.raises(FileNotFoundError, match="processed"):
        validate_source()


def test_validate_source_returns_paths(sample_lake, monkeypatch):
    monkeypatch.setenv("RAPIDGO_DATALAKE", str(sample_lake))

    source = validate_source()

    assert source["discovered_files"] == 1
    assert len(source["processed_paths"]) == 1
    assert source["processed_bytes"] > 0


def test_quality_gate_controlled_failure_and_recovery(
    tmp_path, sample_lake, monkeypatch
):
    flags = tmp_path / "flags"
    flags.mkdir()
    monkeypatch.setenv("RAPIDGO_FLAGS", str(flags))

    ti = _FakeTI()
    ti.values["perfilar_datos"] = inspect_parquet([sample_lake / "processed"])

    (flags / "forzar_fallo_calidad").write_text("fallo", encoding="utf-8")
    with pytest.raises(RuntimeError, match="FALLO_CONTROLADO"):
        quality_gate(ti)

    (flags / "forzar_fallo_calidad").unlink()
    assert quality_gate(ti)["passed"] is True


def test_run_pipeline_end_to_end(tmp_path, sample_lake, monkeypatch):
    monkeypatch.setenv("RAPIDGO_DATALAKE", str(sample_lake))
    monkeypatch.setenv("RAPIDGO_REPORTS", str(tmp_path / "reports"))
    monkeypatch.setenv("RAPIDGO_FLAGS", str(tmp_path / "flags"))

    result = run_pipeline()

    assert result["tasks"] == list(TASK_IDS)
    assert result["summary"]["pedidos_unicos"] == 4
    assert result["summary"]["tiempo_promedio_min"] == 60.0
    assert result["report"]["quality"]["passed"] is True
    assert Path(result["summary"]["report"]).is_file()


def test_run_pipeline_recovers_after_forced_failure(tmp_path, sample_lake, monkeypatch):
    monkeypatch.setenv("RAPIDGO_DATALAKE", str(sample_lake))
    monkeypatch.setenv("RAPIDGO_REPORTS", str(tmp_path / "reports"))
    monkeypatch.setenv("RAPIDGO_FLAGS", str(tmp_path / "flags"))

    with pytest.raises(RuntimeError, match="FALLO_CONTROLADO"):
        run_pipeline(force_failure=True)

    result = run_pipeline()
    assert result["report"]["quality"]["passed"] is True
