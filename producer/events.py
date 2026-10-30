"""Generación sintética de eventos SLA.

Se separa del cliente Kafka para poder probar la generación y el contrato
sin levantar un broker.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sla.rules import TEAM_ID, to_iso

ZONES = ("Norte", "Sur", "Este", "Oeste", "Centro")


@dataclass
class OrderState:
    order_id: str
    zone: str
    distance_km: float
    promised_at: datetime
    status: str = "created"
    picked_up_at: datetime | None = None
    delivered_at: datetime | None = None


def event_key(event: dict[str, Any]) -> str:
    """La key de Kafka es el order_id: preserva el orden de estados del pedido."""
    return str(event["order_id"])


class OrderSimulator:
    """Simula pedidos que avanzan por created -> picked_up -> in_transit -> delivered."""

    def __init__(
        self,
        seed: int | None = None,
        max_active_orders: int = 25,
        cancellation_rate: float = 0.05,
    ) -> None:
        self.rng = random.Random(seed)
        self.max_active_orders = max_active_orders
        self.cancellation_rate = cancellation_rate
        self.active: dict[str, OrderState] = {}
        self._sequence = 0

    def _new_order(self, now: datetime) -> OrderState:
        self._sequence += 1
        # En ~15% de los pedidos el compromiso ya nace vencido: así el pipeline
        # también recibe casos de SLA_EXCEEDED sin esperar horas.
        delay_minutes = (
            self.rng.randint(-20, -1)
            if self.rng.random() < 0.15
            else self.rng.randint(20, 60)
        )
        state = OrderState(
            order_id=f"order-{now:%Y%m%d}-{self._sequence:05d}",
            zone=self.rng.choice(ZONES),
            distance_km=round(self.rng.uniform(0.5, 15.0), 1),
            promised_at=now + timedelta(minutes=delay_minutes),
        )
        self.active[state.order_id] = state
        return state

    def _snapshot(self, state: OrderState, now: datetime) -> dict[str, Any]:
        return {
            "event_id": f"evt-{uuid.uuid4().hex[:12]}",
            "team_id": TEAM_ID,
            "event_type": "delivery_status",
            "event_time": to_iso(now),
            "order_id": state.order_id,
            "zone": state.zone,
            "distance_km": state.distance_km,
            "promised_at": to_iso(state.promised_at),
            "picked_up_at": to_iso(state.picked_up_at) if state.picked_up_at else None,
            "delivered_at": to_iso(state.delivered_at) if state.delivered_at else None,
            "status": state.status,
        }

    def next_event(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(timezone.utc)

        should_create = not self.active or (
            len(self.active) < self.max_active_orders and self.rng.random() < 0.40
        )
        if should_create:
            state = self._new_order(now)
            return self._snapshot(state, now)

        state = self.rng.choice(list(self.active.values()))

        if state.status == "created":
            state.status = "picked_up"
            state.picked_up_at = now - timedelta(minutes=self.rng.uniform(3, 50))
        elif state.status == "picked_up":
            state.status = "in_transit"
        elif state.status == "in_transit":
            if self.rng.random() < self.cancellation_rate:
                state.status = "cancelled"
            else:
                state.status = "delivered"
                state.delivered_at = now
            self.active.pop(state.order_id, None)

        return self._snapshot(state, now)

    def _plan_order(self, base: datetime) -> tuple[OrderState, list[tuple[datetime, str]]]:
        """Genera un pedido con una línea de tiempo realista y sus pasos."""
        self._sequence += 1
        created_at = base + timedelta(minutes=self.rng.randint(0, 90))
        promised_at = created_at + timedelta(minutes=self.rng.randint(35, 100))
        picked_up_at = created_at + timedelta(minutes=self.rng.randint(2, 15))
        transit_at = picked_up_at + timedelta(minutes=self.rng.randint(3, 15))
        delivered_at = picked_up_at + timedelta(minutes=self.rng.randint(15, 60))

        state = OrderState(
            order_id=f"order-{created_at:%Y%m%d}-{self._sequence:05d}",
            zone=self.rng.choice(ZONES),
            distance_km=round(self.rng.uniform(0.5, 15.0), 1),
            promised_at=promised_at,
        )

        steps: list[tuple[datetime, str]] = [
            (created_at, "created"),
            (picked_up_at, "picked_up"),
            (transit_at, "in_transit"),
        ]
        if self.rng.random() < self.cancellation_rate:
            steps.append((transit_at + timedelta(minutes=self.rng.randint(2, 20)), "cancelled"))
        else:
            steps.append((delivered_at, "delivered"))
        return state, steps

    def generate_batch(
        self,
        count: int,
        interval_seconds: int = 300,
        start: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Genera un lote reproducible con timestamps coherentes entre sí.

        A diferencia del modo en vivo, aquí se planifica la línea de tiempo
        completa de cada pedido y luego se ordenan los pasos cronológicamente.
        """
        if count <= 0:
            return []

        base = start or datetime.now(timezone.utc)
        scheduled: list[tuple[datetime, str, OrderState]] = []
        orders_needed = max(1, count // 4)

        for _ in range(orders_needed):
            state, steps = self._plan_order(base)
            for moment, status in steps:
                scheduled.append((moment, status, state))

        scheduled.sort(key=lambda item: (item[0], item[2].order_id))
        events: list[dict[str, Any]] = []
        for moment, status, state in scheduled[:count]:
            state.status = status
            if status == "picked_up":
                state.picked_up_at = moment
            elif status == "delivered":
                state.delivered_at = moment
            events.append(self._snapshot(state, moment))

        # Deja algunos pedidos en curso para que DELIVERY_STOPPED también
        # aparezca en el lote de evidencia, sin inventar eventos posteriores.
        if len(events) >= 6:
            final_time = events[-1]["event_time"]
            for index in (-2, -4, -6):
                event = events[index]
                if event["status"] == "delivered":
                    event["status"] = "in_transit"
                    event["delivered_at"] = None
                    event["event_time"] = final_time
                    # Compromiso vencido hace una hora: dispara DELIVERY_STOPPED.
                    event["promised_at"] = to_iso(
                        datetime.fromisoformat(final_time.replace("Z", "+00:00"))
                        - timedelta(minutes=60)
                    )

        return events
