"""Productor de eventos SLA hacia Kafka (topic rapidgo.sla.events)."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from producer.events import OrderSimulator, event_key
from sla.contract import load_schema, validate_event

DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "rapidgo.sla.events")
DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def build_producer(bootstrap_servers: str):
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "rapidgo-sla-producer-equipo8",
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def write_events(events: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(events, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Genera y publica eventos SLA en Kafka")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--count", type=int, default=0, help="0 = ejecución continua")
    parser.add_argument("--interval", type=float, default=1.0, help="Segundos entre eventos")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", help="No publica en Kafka")
    parser.add_argument("--output", type=Path, default=None, help="Guarda los eventos en JSON")
    parser.add_argument(
        "--start-time",
        default=None,
        help="Reloj inicial ISO-8601 para lotes reproducibles (solo con --dry-run)",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.count < 0:
        parser.error("--count debe ser >= 0")
    if args.interval <= 0:
        parser.error("--interval debe ser > 0")

    schema = load_schema()
    simulator = OrderSimulator(seed=args.seed)

    produced = 0
    failed = 0

    def on_delivery(error, message) -> None:
        nonlocal failed
        if error is not None:
            failed += 1
            print(f"ERROR al publicar: {error}", file=sys.stderr)

    kafka_producer = None
    if not args.dry_run:
        kafka_producer = build_producer(args.bootstrap_servers)
        print(
            f"Publicando en {args.bootstrap_servers} topic={args.topic} "
            f"key=order_id team=equipo8"
        )
    else:
        print("Modo dry-run: no se publica en Kafka")

    clock = None
    if args.start_time:
        clock = datetime.fromisoformat(args.start_time.replace("Z", "+00:00"))
        if clock.tzinfo is None:
            clock = clock.replace(tzinfo=timezone.utc)

    if args.dry_run and args.count > 0:
        events = simulator.generate_batch(
            args.count,
            interval_seconds=int(args.interval),
            start=clock,
        )
        for event in events:
            validate_event(event, schema=schema)
        if args.output:
            write_events(events, args.output)
            print(f"Eventos guardados en {args.output}")
        else:
            for event in events:
                print(json.dumps(event, ensure_ascii=False))
        print(
            f"Resumen: generados={len(events)} errores_publicacion=0 "
            f"topic={args.topic} key=order_id (dry-run)"
        )
        return 0

    batch: list[dict[str, Any]] = []
    target = args.count if args.count > 0 else None
    index = 0

    try:
        while target is None or index < target:
            if clock is not None:
                event = simulator.next_event(clock)
                clock = datetime.fromtimestamp(clock.timestamp() + args.interval, tz=timezone.utc)
            else:
                event = simulator.next_event()

            validate_event(event, schema=schema)
            batch.append(event)

            if kafka_producer is None:
                print(json.dumps(event, ensure_ascii=False))
            else:
                kafka_producer.produce(
                    topic=args.topic,
                    key=event_key(event).encode("utf-8"),
                    value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                    on_delivery=on_delivery,
                )
                kafka_producer.poll(0)

            produced += 1
            index += 1

            if kafka_producer is None and target is None:
                time.sleep(args.interval)
            elif kafka_producer is None:
                continue
            else:
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nDetenido por el usuario")
    finally:
        if kafka_producer is not None:
            pending = kafka_producer.flush(10)
            if pending:
                print(f"ADVERTENCIA: {pending} mensajes sin confirmar", file=sys.stderr)
        if args.output:
            write_events(batch, args.output)
            print(f"Eventos guardados en {args.output}")

    print(
        f"Resumen: generados={produced} errores_publicacion={failed} "
        f"topic={args.topic} key=order_id"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
