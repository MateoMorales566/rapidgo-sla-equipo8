"""Rutas y parámetros compartidos por las tareas de la Semana 3.

Las rutas se leen en cada llamada para que los tests y el DAG puedan usar
directorios temporales sin reiniciar el proceso. En Docker se sobreescriben
con ``RAPIDGO_DATALAKE``, ``RAPIDGO_REPORTS`` y ``RAPIDGO_FLAGS``.
"""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_DATALAKE = ROOT / "datalake"
DEFAULT_REPORTS = ROOT / "reports"
DEFAULT_FLAGS = ROOT / "airflow" / "flags"

QUALITY_FLAG_NAME = "forzar_fallo_calidad"


def _path_from_env(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    return Path(value).expanduser() if value else default


def datalake_path() -> Path:
    """Raíz del Data Lake (raw / processed / curated)."""

    return _path_from_env("RAPIDGO_DATALAKE", DEFAULT_DATALAKE)


def reports_path() -> Path:
    """Directorio donde el DAG escribe el reporte JSON."""

    return _path_from_env("RAPIDGO_REPORTS", DEFAULT_REPORTS)


def flags_path() -> Path:
    """Directorio de banderas usadas para el fallo controlado."""

    return _path_from_env("RAPIDGO_FLAGS", DEFAULT_FLAGS)


def quality_failure_flag() -> Path:
    """Bandera que fuerza el fallo de la tarea ``validar_calidad``."""

    return flags_path() / QUALITY_FLAG_NAME


def max_invalid_ratio() -> float:
    """Porcentaje máximo de filas inválidas aceptado por el quality gate."""

    raw = os.environ.get("RAPIDGO_MAX_INVALID_RATIO", "0")
    try:
        value = float(raw)
    except ValueError as error:
        raise ValueError(
            f"RAPIDGO_MAX_INVALID_RATIO debe ser numérico, se recibió {raw!r}"
        ) from error
    if value < 0 or value > 1:
        raise ValueError("RAPIDGO_MAX_INVALID_RATIO debe estar entre 0 y 1")
    return value
