from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq


def summarize_records(rows: list[dict]) -> dict:
    latest_by_order: dict[str, dict] = {}
    for row in rows:
        order_id = row.get("order_id")
        if order_id is not None:
            latest_by_order[str(order_id)] = row

    unique_records = list(latest_by_order.values())
    delivered = [record for record in unique_records if record.get("status") == "delivered"]
    delivery_minutes = [record.get("delivery_minutes", 0) for record in delivered]
    late_count = sum(1 for record in delivered if record.get("is_late"))

    delivered_count = len(delivered)
    avg_delivery = (sum(delivery_minutes) / delivered_count) if delivered_count else 0
    late_pct = (late_count * 100 / delivered_count) if delivered_count else 0

    return {
        "pedidos_unicos": len(unique_records),
        "tiempo_promedio_min": round(avg_delivery),
        "porcentaje_fuera_sla": round(late_pct),
    }


def summarize_sla(processed_paths: list[str]) -> dict:
    rows: list[dict] = []
    for path in processed_paths:
        table = pq.read_table(path)
        rows.extend(table.to_pylist())
    return summarize_records(rows)


def save_report(report: dict, output_dir: str | Path) -> dict:
    path = Path(output_dir) / "sla_semana3_latest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    path.write_text(payload, encoding="utf-8")
    return {"path": str(path), "size_bytes": path.stat().st_size}
