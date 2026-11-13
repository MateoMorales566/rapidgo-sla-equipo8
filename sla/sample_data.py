"""Genera una capa ``processed`` Parquet a partir de un lote de eventos.

Se usa para demos locales y para el fixture de CI, no para reemplazar a Spark.
El esquema imita las columnas que produce ``spark/transforms.py`` para que el
DAG de Airflow y el quality gate trabajen con datos realistas.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import date
from pathlib import Path
from typing import Any, Iterable

from sla.rules import (
    TEAM_ID,
    VALID_STATUSES,
    calcular_tiempo_entrega,
    parse_timestamp,
    to_iso,
)


def _timestamp(value: Any):
    try:
        return parse_timestamp(value)
    except (TypeError, ValueError):
        return None


def build_processed_rows(events: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Transforma eventos crudos al esquema enriquecido de Spark."""

    rows: list[dict[str, Any]] = []
    skipped = 0

    for event in events:
        event_time = _timestamp(event.get("event_time"))
        promised_at = _timestamp(event.get("promised_at"))
        picked_up_at = _timestamp(event.get("picked_up_at"))
        delivered_at = _timestamp(event.get("delivered_at"))

        if (
            event.get("team_id") != TEAM_ID
            or event.get("status") not in VALID_STATUSES
            or event_time is None
            or promised_at is None
            or not str(event.get("event_id") or "").strip()
            or not str(event.get("order_id") or "").strip()
            or not str(event.get("zone") or "").strip()
        ):
            skipped += 1
            continue

        delivery_minutes = calcular_tiempo_entrega(picked_up_at, delivered_at)
        minutes_late = (
            round((delivered_at - promised_at).total_seconds() / 60.0, 2)
            if delivered_at is not None
            else None
        )

        if event.get("status") == "cancelled":
            sla_status = "CANCELADO"
        elif delivered_at is None:
            sla_status = "PENDIENTE"
        elif delivered_at <= promised_at:
            sla_status = "CUMPLE"
        else:
            sla_status = "FUERA_SLA"

        row = dict(event)
        row.update(
            {
                "event_time": event_time,
                "promised_at": promised_at,
                "picked_up_at": picked_up_at,
                "delivered_at": delivered_at,
                "delivery_minutes": delivery_minutes,
                "minutes_late": minutes_late,
                "sla_status": sla_status,
                "is_late": sla_status == "FUERA_SLA",
                "event_date": event_time.date() if event_time else date.min,
            }
        )
        rows.append(row)

    return rows, skipped


def write_processed(
    rows: Iterable[dict[str, Any]],
    output_dir: str | Path,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Escribe ``<output>/processed/part-00000.parquet``."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    output = Path(output_dir)
    processed = output / "processed"
    if processed.exists() and any(processed.glob("*.parquet")):
        if not overwrite:
            raise FileExistsError(
                f"Ya existen Parquet en {processed}; use --overwrite para reemplazarlos"
            )
        shutil.rmtree(processed)

    processed.mkdir(parents=True, exist_ok=True)
    payload = list(rows)
    if not payload:
        raise ValueError("No hay filas válidas para escribir")

    table = pa.Table.from_pylist(payload)
    destination = processed / "part-00000.parquet"
    pq.write_table(table, destination, compression="snappy")

    return {
        "output": str(output),
        "processed": str(destination),
        "rows": table.num_rows,
        "size_bytes": destination.stat().st_size,
    }


def load_events(path: str | Path) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("El archivo de entrada debe contener una lista de eventos")
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Genera la capa processed Parquet desde un lote JSON"
    )
    parser.add_argument("--input", required=True, help="Lote JSON de eventos")
    parser.add_argument(
        "--output",
        default="reports/sample-datalake",
        help="Directorio raíz del Data Lake de ejemplo",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Reemplaza los Parquet existentes en <output>/processed",
    )
    args = parser.parse_args(argv)

    try:
        events = load_events(args.input)
        rows, skipped = build_processed_rows(events)
        result = write_processed(rows, args.output, overwrite=args.overwrite)
    except Exception as error:  # noqa: BLE001 - la CLI debe mostrar el error real
        print(f"ERROR: {error}", file=sys.stderr)
        return 2

    print(
        f"Eventos leídos: {len(events)}; escritos: {result['rows']}; "
        f"descartados: {skipped}"
    )
    print(f"Parquet: {result['processed']} ({result['size_bytes']} bytes)")
    if rows:
        print(f"Rango temporal: {to_iso(rows[0]['event_time'])} -> {to_iso(rows[-1]['event_time'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
