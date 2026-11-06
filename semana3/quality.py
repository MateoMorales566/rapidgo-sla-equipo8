"""Perfilado y quality gate de la capa ``processed`` del Data Lake.

La lectura se hace con ``pyarrow`` para no cargar Spark dentro de Airflow. El
gate es deliberadamente estricto: columnas requeridas, al menos una fila, sin
``event_id`` duplicados y sin filas inválidas. El umbral de filas inválidas se
puede relajar con ``RAPIDGO_MAX_INVALID_RATIO`` (0 por defecto).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Sequence

from sla.rules import TEAM_ID, VALID_STATUSES, parse_timestamp

from semana3.config import max_invalid_ratio

REQUIRED_COLUMNS = frozenset(
    {
        "event_id",
        "team_id",
        "order_id",
        "status",
        "zone",
        "event_time",
        "promised_at",
    }
)

OPTIONAL_TIMESTAMPS = ("picked_up_at", "delivered_at")
MAX_SAMPLE_PROBLEMS = 10


class QualityGateError(ValueError):
    """El lote no cumple las condiciones mínimas para publicar resultados."""


def resolve_parquet_paths(paths: Iterable[str | Path]) -> list[Path]:
    """Acepta archivos o directorios y devuelve Parquet sin checkpoints."""

    if isinstance(paths, (str, Path)):
        paths = [paths]

    found: list[Path] = []
    for raw in paths:
        candidate = Path(raw)
        if candidate.is_dir():
            found.extend(sorted(candidate.rglob("*.parquet")))
        elif candidate.is_file():
            if candidate.suffix != ".parquet":
                raise ValueError(f"No es un archivo Parquet: {candidate}")
            found.append(candidate)
        else:
            raise FileNotFoundError(f"No existe la ruta: {candidate}")

    unique: list[Path] = []
    seen: set[Path] = set()
    for path in found:
        resolved = path.resolve()
        if "_checkpoints" in resolved.parts or resolved in seen:
            continue
        seen.add(resolved)
        unique.append(path)
    return unique


def _valid_timestamp(value: Any) -> bool:
    if value is None:
        return False
    try:
        return parse_timestamp(value) is not None
    except (TypeError, ValueError):
        return False


def _row_problems(row: dict[str, Any]) -> list[str]:
    """Devuelve los problemas de una fila; vacío significa fila válida."""

    problems: list[str] = []

    if not str(row.get("event_id") or "").strip():
        problems.append("event_id vacío")
    if not str(row.get("order_id") or "").strip():
        problems.append("order_id vacío")
    if str(row.get("team_id") or "") != TEAM_ID:
        problems.append(f"team_id distinto de {TEAM_ID}")
    if row.get("status") not in VALID_STATUSES:
        problems.append(f"status inválido: {row.get('status')!r}")
    if not str(row.get("zone") or "").strip():
        problems.append("zone vacía")

    if not _valid_timestamp(row.get("event_time")):
        problems.append("event_time inválido")
    if not _valid_timestamp(row.get("promised_at")):
        problems.append("promised_at inválido")

    for column in OPTIONAL_TIMESTAMPS:
        value = row.get(column)
        if value is not None and not _valid_timestamp(value):
            problems.append(f"{column} inválido")

    if row.get("status") == "delivered":
        delivery = row.get("delivery_minutes")
        if delivery is None:
            problems.append("delivery_minutes ausente para pedido entregado")
        else:
            try:
                if float(delivery) < 0:
                    problems.append("delivery_minutes negativo")
            except (TypeError, ValueError):
                problems.append("delivery_minutes no numérico")

    return problems


def inspect_parquet(paths: Iterable[str | Path]) -> dict[str, Any]:
    """Perfila los Parquet y cuenta filas inválidas sin detener la lectura."""

    import pyarrow.parquet as pq

    files = resolve_parquet_paths(paths)
    if not files:
        raise ValueError("No se recibieron archivos Parquet")

    total_rows = 0
    total_bytes = 0
    columns: set[str] | None = None
    schemas: set[tuple[str, ...]] = set()
    invalid_count = 0
    duplicate_event_ids = 0
    sample_problems: list[str] = []
    event_ids: set[str] = set()
    min_event_time = None
    max_event_time = None

    for path in files:
        table = pq.read_table(path)
        available = set(table.column_names)
        columns = available if columns is None else columns & available
        schemas.add(tuple(sorted(available)))
        total_rows += table.num_rows
        total_bytes += path.stat().st_size

        for index, row in enumerate(table.to_pylist()):
            problems = _row_problems(row)

            event_id = str(row.get("event_id") or "").strip()
            if event_id:
                if event_id in event_ids:
                    duplicate_event_ids += 1
                    problems.append(f"event_id duplicado: {event_id}")
                event_ids.add(event_id)

            if problems:
                invalid_count += 1
                if len(sample_problems) < MAX_SAMPLE_PROBLEMS:
                    sample_problems.append(
                        f"{path.name} fila {index}: " + "; ".join(problems)
                    )

            moment = None
            try:
                moment = parse_timestamp(row.get("event_time"))
            except (TypeError, ValueError):
                moment = None
            if moment is not None:
                min_event_time = moment if min_event_time is None else min(min_event_time, moment)
                max_event_time = moment if max_event_time is None else max(max_event_time, moment)

    return {
        "files": len(files),
        "columns": sorted(columns or []),
        "schemas": [list(schema) for schema in sorted(schemas)],
        "total_rows": total_rows,
        "total_bytes": total_bytes,
        "invalid_count": invalid_count,
        "duplicate_event_ids": duplicate_event_ids,
        "sample_problems": sample_problems,
        "min_event_time": min_event_time.isoformat() if min_event_time else None,
        "max_event_time": max_event_time.isoformat() if max_event_time else None,
    }


def evaluate_quality(
    profile: dict[str, Any],
    max_invalid_ratio_override: float | None = None,
) -> dict[str, Any]:
    """Evalúa el perfil y lanza ``QualityGateError`` si no es publicable."""

    total = int(profile.get("total_rows") or 0)
    invalid = int(profile.get("invalid_count") or 0)
    duplicates = int(profile.get("duplicate_event_ids") or 0)
    columns = set(profile.get("columns") or [])
    missing = sorted(REQUIRED_COLUMNS - columns)
    threshold = (
        max_invalid_ratio()
        if max_invalid_ratio_override is None
        else float(max_invalid_ratio_override)
    )
    invalid_ratio = (invalid / total) if total else 1.0

    checks = {
        "columnas_requeridas": not missing,
        "filas_presentes": total > 0,
        "sin_duplicados": duplicates == 0,
        "invalidos_dentro_de_umbral": invalid_ratio <= threshold,
    }

    problems: list[str] = []
    if missing:
        problems.append("Columnas faltantes: " + ", ".join(missing))
    if total <= 0:
        problems.append("No hay filas procesadas")
    if duplicates:
        problems.append(f"event_id duplicados: {duplicates}")
    if invalid_ratio > threshold:
        problems.append(
            f"Filas inválidas: {invalid}/{total} "
            f"({invalid_ratio:.2%}) supera el umbral {threshold:.2%}"
        )

    if problems:
        detail = problems + list(profile.get("sample_problems") or [])
        raise QualityGateError("QUALITY_GATE_FAILED: " + "; ".join(detail[:6]))

    return {
        "passed": True,
        "total_rows": total,
        "invalid_count": invalid,
        "duplicate_event_ids": duplicates,
        "max_invalid_ratio": threshold,
        "checks": checks,
        "columns": sorted(columns),
        "min_event_time": profile.get("min_event_time"),
        "max_event_time": profile.get("max_event_time"),
    }


def profile_and_validate(paths: Sequence[str | Path]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Atajo usado por el runner local: perfil + gate en una sola llamada."""

    profile = inspect_parquet(paths)
    return profile, evaluate_quality(profile)


__all__ = [
    "QualityGateError",
    "REQUIRED_COLUMNS",
    "evaluate_quality",
    "inspect_parquet",
    "profile_and_validate",
    "resolve_parquet_paths",
]
