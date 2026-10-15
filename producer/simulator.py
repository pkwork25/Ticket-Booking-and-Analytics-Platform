"""Synthetic ticket-booking + payment event generator.

Each booking is a *journey* scheduled into the future (seat hold -> payment attempt(s) ->
confirmation / expiry -> optional cancellation + refund). ``tick(now)`` returns the events that
are due, so the producer streams them in realistic order and timing.

Built-in data-quality chaos for testing the pipeline:
  * duplicates   (same event_id sent twice - at-least-once producers)
  * late events  (emitted 30-120 s after their event_time)
  * bad events   (missing keys / negative amounts -> must land in quarantine)
  * gateway incident (one gateway's failure rate spikes -> must raise an alert)
"""

from __future__ import annotations

import heapq
import itertools
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

CITIES = ["Mumbai", "Pune", "Bengaluru", "Delhi", "Hyderabad", "Chennai", "Kolkata", "Ahmedabad"]
CATALOG = {
    "MOVIE": (["Midnight Express Reloaded", "The Monsoon Heist", "Galaxy Run 3", "Paper Flowers (Restored Classic)"], (180, 450)),
    "CONCERT": (["Indie Nights Live", "Sufi Under The Stars", "EDM Arena Tour", "Bollywood Unplugged"], (1499, 6999)),
    "SPORTS": (["T20 League - Match 14", "Pro Kabaddi - Semi Final", "Football Super Cup", "Badminton Open Finals"], (800, 4500)),
    "COMEDY": (["Stand-up Saturday", "Improv Jam Session"], (499, 1499)),
    "THEATRE": (["Hamlet (Hindi Adaptation)", "The Last Train - Musical"], (600, 2500)),
}
CATEGORY_WEIGHTS = {"MOVIE": 0.55, "CONCERT": 0.15, "SPORTS": 0.15, "COMEDY": 0.10, "THEATRE": 0.05}
METHODS = {"UPI": 0.62, "CARD": 0.22, "NETBANKING": 0.08, "WALLET": 0.08}
GATEWAYS = {"razorpay": 0.45, "payu": 0.25, "cashfree": 0.20, "ccavenue": 0.10}
BASE_FAILURE = {"UPI": 0.06, "CARD": 0.08, "NETBANKING": 0.11, "WALLET": 0.04}
FAILURE_CODES = {
    "UPI": ["UPI_DEBIT_FAILED", "UPI_COLLECT_EXPIRED", "BANK_TIMEOUT", "UPI_PIN_INCORRECT"],
    "CARD": ["INSUFFICIENT_FUNDS", "3DS_AUTH_FAILED", "ISSUER_DECLINED", "CARD_EXPIRED"],
    "NETBANKING": ["BANK_TIMEOUT", "USER_ABORTED", "BANK_DOWNTIME"],
    "WALLET": ["INSUFFICIENT_BALANCE", "WALLET_LIMIT_EXCEEDED"],
}
INCIDENT_CODES = ["GATEWAY_TIMEOUT", "GATEWAY_5XX"]


def _weighted(rng: random.Random, weights: dict[str, float]) -> str:
    return rng.choices(list(weights), weights=list(weights.values()), k=1)[0]


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class ChaosConfig:
    duplicate_rate: float = 0.02
    late_rate: float = 0.02
    bad_rate: float = 0.005
    incident_gateway: str | None = None
    incident_failure_rate: float = 0.6


@dataclass
class TicketEventSimulator:
    seed: int | None = None
    chaos: ChaosConfig = field(default_factory=ChaosConfig)
    hold_seconds: int = 120  # seat hold before an unpaid booking expires (compressed for demos)

    def __post_init__(self) -> None:
        self.rng = random.Random(self.seed)
        self._queue: list[tuple[datetime, int, dict]] = []
        self._seq = itertools.count()
        self.stats = {"bookings": 0, "events": 0, "duplicates": 0, "late": 0, "bad": 0}

    # ------------------------------------------------------------------ helpers
    def _id(self, prefix: str) -> str:
        return f"{prefix}-{uuid.UUID(int=self.rng.getrandbits(128)).hex[:12].upper()}"

    def _schedule(self, due: datetime, event: dict) -> None:
        heapq.heappush(self._queue, (due, next(self._seq), event))

    def _envelope(self, etype: str, ts: datetime) -> dict:
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "event_type": etype,
            "event_time": _iso(ts),
            "schema_version": 1,
            "source": "booking-service" if etype.startswith("booking") else "payment-service",
        }

    def _booking_event(self, etype: str, ts: datetime, booking: dict, status: str) -> dict:
        return {**self._envelope(etype, ts), **booking, "booking_status": status}

    def _payment_event(self, etype: str, ts: datetime, payment: dict, status: str, **extra) -> dict:
        return {**self._envelope(etype, ts), **payment, "payment_status": status,
                "failure_code": None, "refund_amount": None, **extra}  # fmt: skip

    def _fails(self, method: str, gateway: str) -> bool:
        p = BASE_FAILURE[method]
        if self.chaos.incident_gateway and gateway == self.chaos.incident_gateway:
            p = max(p, self.chaos.incident_failure_rate)
        return self.rng.random() < p

    # ------------------------------------------------------------------ journeys
    def start_booking(self, now: datetime) -> None:
        self.stats["bookings"] += 1
        category = _weighted(self.rng, CATEGORY_WEIGHTS)
        names, (lo, hi) = CATALOG[category]
        seats = self.rng.choices([1, 2, 3, 4, 6], weights=[30, 40, 12, 14, 4], k=1)[0]
        price = round(self.rng.uniform(lo, hi) / 10) * 10
        booking = {
            "booking_id": self._id("BKG"),
            "user_id": f"USR-{self.rng.randint(1, 50000):06d}",
            "event_name": self.rng.choice(names),
            "event_category": category,
            "venue_city": self.rng.choice(CITIES),
            "show_time": _iso(now + timedelta(days=self.rng.randint(0, 30), hours=self.rng.randint(1, 12))),
            "seats": seats,
            "ticket_amount": float(seats * price),
        }
        self._schedule(now, self._booking_event("booking_created", now, booking, "PENDING_PAYMENT"))

        t = now + timedelta(seconds=self.rng.uniform(3, 30))
        method = _weighted(self.rng, METHODS)
        succeeded = False
        for attempt in range(2):  # one retry, sometimes with another method
            gateway = _weighted(self.rng, GATEWAYS)
            payment = {
                "payment_id": self._id("PAY"),
                "booking_id": booking["booking_id"],
                "user_id": booking["user_id"],
                "payment_method": method,
                "gateway": gateway,
                "amount": booking["ticket_amount"],
                "currency": "INR",
            }
            self._schedule(t, self._payment_event("payment_initiated", t, payment, "INITIATED"))
            t += timedelta(seconds=self.rng.uniform(2, 20))
            if self._fails(method, gateway):
                incident = self.chaos.incident_gateway == gateway and self.rng.random() < 0.7
                code = self.rng.choice(INCIDENT_CODES if incident else FAILURE_CODES[method])
                self._schedule(t, self._payment_event("payment_failed", t, payment, "FAILED", failure_code=code))
                if attempt == 0 and self.rng.random() < 0.55:
                    t += timedelta(seconds=self.rng.uniform(5, 25))
                    if self.rng.random() < 0.3:
                        method = _weighted(self.rng, METHODS)
                    continue
                break
            self._schedule(t, self._payment_event("payment_succeeded", t, payment, "SUCCEEDED"))
            succeeded = True
            break

        if succeeded:
            t += timedelta(seconds=self.rng.uniform(1, 4))
            self._schedule(t, self._booking_event("booking_confirmed", t, booking, "CONFIRMED"))
            if self.rng.random() < 0.04:
                t_cancel = t + timedelta(seconds=self.rng.uniform(30, 180))
                self._schedule(t_cancel, self._booking_event("booking_cancelled", t_cancel, booking, "CANCELLED"))
                t_refund = t_cancel + timedelta(seconds=self.rng.uniform(5, 40))
                refund = round(booking["ticket_amount"] * 0.9, 2)  # 10% cancellation fee
                self._schedule(
                    t_refund,
                    self._payment_event("refund_processed", t_refund, payment, "REFUNDED", refund_amount=refund),
                )
        else:
            t_exp = now + timedelta(seconds=self.hold_seconds)
            self._schedule(t_exp, self._booking_event("booking_expired", t_exp, booking, "EXPIRED"))

    # ------------------------------------------------------------------ emission
    def _corrupt(self, event: dict) -> dict:
        bad = dict(event)
        choice = self.rng.choice(["booking_id", "amount", "event_type", "event_time"])
        if choice == "booking_id":
            bad["booking_id"] = None
        elif choice == "amount":
            bad["amount" if "amount" in bad else "ticket_amount"] = -1.0
        elif choice == "event_type":
            bad["event_type"] = "booking_teleported"
        else:
            bad["event_time"] = "not-a-timestamp"
        return bad

    def tick(self, now: datetime) -> list[dict]:
        """Pop every event due at or before ``now`` (plus chaos)."""
        out: list[dict] = []
        while self._queue and self._queue[0][0] <= now:
            _, _, event = heapq.heappop(self._queue)
            if event.get("_late"):
                event = {k: v for k, v in event.items() if k != "_late"}
            elif self.rng.random() < self.chaos.late_rate:
                self.stats["late"] += 1
                self._schedule(now + timedelta(seconds=self.rng.uniform(30, 120)), {**event, "_late": True})
                continue
            if self.rng.random() < self.chaos.bad_rate:
                self.stats["bad"] += 1
                event = self._corrupt(event)
            out.append(event)
            if self.rng.random() < self.chaos.duplicate_rate:
                self.stats["duplicates"] += 1
                out.append(dict(event))
        self.stats["events"] += len(out)
        return out

    def pending(self) -> int:
        return len(self._queue)

    def drain(self, now: datetime, horizon_seconds: int = 3600) -> list[dict]:
        """Emit everything still scheduled, including re-queued late events (tests / --dry-run)."""
        out: list[dict] = []
        t = now + timedelta(seconds=horizon_seconds)
        while self._queue:
            out += self.tick(t)
            t += timedelta(seconds=300)
        return out
