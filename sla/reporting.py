"""KPIs, alertas y reporte de la variante SLA.

El módulo tiene dos entradas:

* **Lote JSON**: ``build_batch_report`` y ``format_report`` para validar el
  contrato y revisar los KPIs sin Kafka ni Spark. Se ejecuta con
  ``python -m sla.reporting archivo.json``.
* **Capa Parquet del Data Lake**: ``summarize_sla``, ``build_pipeline_report``
  y ``save_report`` para las tareas de Airflow, que leen ``processed`` y
  publican un JSON de forma atómica.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from sla.contract import load_events
from sla.quality import resolve_parquet_paths
from sla.rules import (
    FINAL_STATUSES,
    calcular_kpis,
    evaluar_alertas,
    latest_events,
    reference_time,
    to_iso,
)

REPORT_FILENAME = "sla_latest.json"


# ---------------------------------------------------------------------------
# Reporte de un lote JSON (sin Spark ni Data Lake)
# ---------------------------------------------------------------------------

def build_batch_report(events: list[dict[str, Any]], now: str | None = None) -> dict[str, Any]:
    """Calcula KPIs y alertas de un lote de eventos."""

    clock = now or reference_time(events)
    return {
        "generado_para": str(clock),
        "kpis": calcular_kpis(events, now=clock),
        "alertas": evaluar_alertas(events, now=clock),
    }


def format_batch_report(report: dict[str, Any]) -> str:
    """Devuelve una representación legible del reporte de lote."""

    kpis = report["kpis"]
    alerts = report["alertas"]
    lines = [
        f"Eventos analizados: {kpis['eventos']}",
        f"Pedidos entregados: {kpis['pedidos_entregados']}",
        (
            "Tiempo promedio de entrega: "
            f"{kpis['tiempo_promedio_min']} min "
            f"(objetivo <= {kpis['umbral_sla_min']:.0f} min)"
        ),
        (
            "Porcentaje fuera de SLA: "
            f"{kpis['porcentaje_fuera_sla']:.2f}% "
            f"(objetivo <= {kpis['umbral_fuera_sla_pct']:.0f}%)"
        ),
        f"Pedidos retrasados: {kpis['pedidos_retrasados']}",
        f"Alertas SLA_EXCEEDED: {kpis['alertas_sla_excedido']}",
        (
            "Alertas DELIVERY_STOPPED: "
            f"{kpis['alertas_delivery_stopped']} "
            f"(umbral > {kpis['umbral_detencion_min']:.0f} min)"
        ),
        f"Eventos inválidos descartados: {kpis['eventos_invalidos']}",
        f"Reloj de referencia: {report['generado_para']}",
    ]

    if alerts:
        lines.append("")
        lines.append("Detalle de alertas:")
        for alert in alerts:
            lines.append(
                f"  - {alert['alert_code']} pedido={alert['order_id']} "
                f"zona={alert['zone']} minutos={alert.get('minutes', 0)} "
                f"severidad={alert['severity']}"
            )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# KPIs y reporte para la capa processed / Airflow
# ---------------------------------------------------------------------------

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


def build_pipeline_report(
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
        prefix=".sla_latest_",
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


# ---------------------------------------------------------------------------
# CLI del reporte de lote
# ---------------------------------------------------------------------------

def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Calcula KPIs y alertas de un lote de eventos")
    parser.add_argument("file", help="Archivo JSON/NDJSON con eventos")
    parser.add_argument(
        "--now",
        default=None,
        help="Reloj de referencia ISO-8601. Por defecto usa el máximo event_time.",
    )
    parser.add_argument("--json", action="store_true", help="Salida JSON completa")
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        events = load_events(args.file)
        report = build_batch_report(events, now=args.now)
    except Exception as error:  # noqa: BLE001 - la CLI debe mostrar el error real
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(format_batch_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "REPORT_FILENAME",
    "build_batch_report",
    "build_pipeline_report",
    "format_batch_report",
    "read_report",
    "save_report",
    "summarize_records",
    "summarize_sla",
]
