"""Agregación de KPIs y escritura atómica del reporte de la Semana 3."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from sla.rules import (
    FINAL_STATUSES,
    evaluar_alertas,
    latest_events,
    reference_time,
    to_iso,
)

from semana3.quality import resolve_parquet_paths

REPORT_FILENAME = "sla_semana3_latest.json"


def _as_minutes(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_late(record: dict[str, Any]) -> bool:
    if bool(record.get("is_late")):
        return True
    if record.get("sla_status") == "FUERA_SLA":
        return True
    minutes_late = _as_minutes(record.get("minutes_late"))
    return minutes_late is not None and minutes_late > 0


def summarize_records(
    rows: Iterable[dict[str, Any]],
    now: Any = None,
) -> dict[str, Any]:
    """Calcula los KPIs con el último estado de cada pedido.

    Se ordena por ``event_time``, no por el orden en que llegaron los archivos:
    una lectura de Parquet puede devolver las filas en cualquier orden.
    """

    records = [dict(row) for row in rows]
    latest = latest_events(records)

    delivered = [record for record in latest if record.get("status") == "delivered"]
    cancelled = [record for record in latest if record.get("status") == "cancelled"]
    in_progress = [
        record for record in latest if record.get("status") not in FINAL_STATUSES
    ]

    durations: list[float] = []
    late = 0
    for record in delivered:
        minutes = _as_minutes(record.get("delivery_minutes"))
        if minutes is None:
            continue
        durations.append(minutes)
        if _is_late(record):
            late += 1

    reference = now if now is not None else reference_time(records)
    alerts = evaluar_alertas(records, now=reference)

    delivered_count = len(durations)
    return {
        "eventos_totales": len(records),
        "pedidos_unicos": len(latest),
        "pedidos_entregados": delivered_count,
        "pedidos_en_curso": len(in_progress),
        "pedidos_cancelados": len(cancelled),
        "tiempo_promedio_min": (
            round(sum(durations) / delivered_count, 2) if delivered_count else None
        ),
        "porcentaje_fuera_sla": (
            round(100.0 * late / delivered_count, 2) if delivered_count else 0.0
        ),
        "pedidos_retrasados": late,
        "alertas_sla_excedido": sum(
            1 for alert in alerts if alert["alert_code"] == "SLA_EXCEEDED"
        ),
        "alertas_delivery_stopped": sum(
            1 for alert in alerts if alert["alert_code"] == "DELIVERY_STOPPED"
        ),
        "reloj_referencia": to_iso(reference),
    }


def summarize_sla(
    processed_paths: Iterable[str | Path],
    now: Any = None,
) -> dict[str, Any]:
    """Lee la capa ``processed`` y devuelve los KPIs agregados."""

    import pyarrow.parquet as pq

    rows: list[dict[str, Any]] = []
    files = resolve_parquet_paths(processed_paths)
    if not files:
        raise ValueError("No hay archivos Parquet para calcular KPIs")
    for path in files:
        rows.extend(pq.read_table(path).to_pylist())
    return summarize_records(rows, now=now)


def build_report(
    metrics: dict[str, Any],
    quality: dict[str, Any],
    generated_at: str | None = None,
    source_files: Iterable[str | Path] | None = None,
) -> dict[str, Any]:
    """Construye el JSON que se publica como salida del DAG."""

    moment = generated_at or datetime.now(timezone.utc).isoformat(timespec="seconds")
    sources = [str(path) for path in (source_files or [])]
    return {
        "generated_at": moment,
        "metrics": metrics,
        "quality": quality,
        "source_files": sources,
    }


def save_report(report: dict[str, Any], output_dir: str | Path) -> dict[str, Any]:
    """Escribe el reporte de forma atómica: archivo temporal y ``os.replace``."""

    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / REPORT_FILENAME
    payload = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)

    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=directory,
        prefix=".sla_semana3_",
        suffix=".tmp",
        delete=False,
    )
    try:
        with handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(handle.name, destination)
    finally:
        if os.path.exists(handle.name):
            os.unlink(handle.name)

    return {
        "path": str(destination),
        "size_bytes": destination.stat().st_size,
        "generated_at": report.get("generated_at"),
    }


def read_report(path: str | Path) -> dict[str, Any]:
    """Lee el reporte publicado; útil para la tarea de publicación y la CLI."""

    return json.loads(Path(path).read_text(encoding="utf-8"))


__all__ = [
    "REPORT_FILENAME",
    "build_report",
    "read_report",
    "save_report",
    "summarize_records",
    "summarize_sla",
]
