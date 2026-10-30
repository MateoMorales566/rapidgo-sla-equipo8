"""Reporte de KPIs y alertas SLA a partir de un lote de eventos."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sla.contract import load_events
from sla.rules import calcular_kpis, evaluar_alertas, reference_time


def build_report(events: list[dict], now: str | None = None) -> dict:
    clock = now or reference_time(events)
    kpis = calcular_kpis(events, now=clock)
    alerts = evaluar_alertas(events, now=clock)
    return {
        "generado_para": str(clock),
        "kpis": kpis,
        "alertas": alerts,
    }


def format_report(report: dict) -> str:
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
            minutes = alert.get("minutes", 0)
            lines.append(
                f"  - {alert['alert_code']} pedido={alert['order_id']} "
                f"zona={alert['zone']} minutos={minutes} severidad={alert['severity']}"
            )

    return "\n".join(lines)


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

    events = load_events(args.file)
    report = build_report(events, now=args.now)

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(format_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
