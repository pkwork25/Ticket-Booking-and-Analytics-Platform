"""Event contract shared by the producer, the Glue job and the tests.

Every event is a flat JSON envelope. Booking events carry the full booking snapshot and payment
events the full payment snapshot (event-carried state transfer), so current state can be rebuilt
from the latest event per key without joins.
"""

from __future__ import annotations

from datetime import datetime

SCHEMA_VERSION = 1

BOOKING_EVENT_TYPES = frozenset(
    {
        "booking_created",  # seats held, awaiting payment
        "booking_confirmed",  # payment captured, tickets issued
        "booking_cancelled",  # user cancelled after confirmation
        "booking_expired",  # hold released because payment never succeeded
    }
)
PAYMENT_EVENT_TYPES = frozenset(
    {
        "payment_initiated",
        "payment_succeeded",
        "payment_failed",
        "refund_processed",
    }
)
EVENT_TYPES = BOOKING_EVENT_TYPES | PAYMENT_EVENT_TYPES

BOOKING_STATUSES = frozenset({"PENDING_PAYMENT", "CONFIRMED", "CANCELLED", "EXPIRED"})
PAYMENT_STATUSES = frozenset({"INITIATED", "SUCCEEDED", "FAILED", "REFUNDED"})
PAYMENT_METHODS = frozenset({"UPI", "CARD", "NETBANKING", "WALLET"})

ENVELOPE_FIELDS = ["event_id", "event_type", "event_time", "schema_version", "source"]
BOOKING_FIELDS = [
    "booking_id",
    "user_id",
    "event_name",
    "event_category",
    "venue_city",
    "show_time",
    "seats",
    "ticket_amount",
    "booking_status",
]
PAYMENT_FIELDS = [
    "payment_id",
    "booking_id",
    "user_id",
    "payment_method",
    "gateway",
    "payment_status",
    "amount",
    "currency",
    "failure_code",
    "refund_amount",
]
ALL_FIELDS = ENVELOPE_FIELDS + BOOKING_FIELDS + [f for f in PAYMENT_FIELDS if f not in BOOKING_FIELDS]

# Column order of the Redshift tables (redshift/01_tables.sql). The Glue job selects exactly these.
BOOKING_EVENTS_COLUMNS = [
    "event_id", "event_type", "event_time", "ingest_time", "source",
    "booking_id", "user_id", "event_name", "event_category", "venue_city", "show_time",
    "seats", "ticket_amount", "booking_status",
]  # fmt: skip
PAYMENT_EVENTS_COLUMNS = [
    "event_id", "event_type", "event_time", "ingest_time", "source",
    "payment_id", "booking_id", "user_id", "payment_method", "gateway", "payment_status",
    "amount", "currency", "failure_code", "refund_amount",
]  # fmt: skip
BOOKINGS_CURRENT_COLUMNS = [
    "booking_id", "user_id", "event_name", "event_category", "venue_city", "show_time",
    "seats", "ticket_amount", "booking_status", "last_event_id", "last_event_type",
    "last_event_time", "updated_at",
]  # fmt: skip
PAYMENTS_CURRENT_COLUMNS = [
    "payment_id", "booking_id", "user_id", "payment_method", "gateway", "payment_status",
    "amount", "currency", "failure_code", "refund_amount", "last_event_id", "last_event_type",
    "last_event_time", "updated_at",
]  # fmt: skip


def _parse_ts(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def validate_event(event: dict) -> list[str]:
    """Return a list of rule violations (empty list = valid).

    The same rules are expressed as Spark column expressions in ``transforms.py``; the tests
    check both implementations agree.
    """
    errors: list[str] = []
    etype = event.get("event_type")
    if not event.get("event_id"):
        errors.append("missing_event_id")
    if etype not in EVENT_TYPES:
        errors.append("unknown_event_type")
    if _parse_ts(event.get("event_time")) is None:
        errors.append("bad_event_time")
    if not event.get("booking_id"):
        errors.append("missing_booking_id")

    if etype in BOOKING_EVENT_TYPES:
        if event.get("booking_status") not in BOOKING_STATUSES:
            errors.append("bad_booking_status")
        seats = event.get("seats")
        if not isinstance(seats, int) or not 1 <= seats <= 20:
            errors.append("bad_seats")
        amt = event.get("ticket_amount")
        if not isinstance(amt, (int, float)) or amt < 0:
            errors.append("bad_ticket_amount")
    elif etype in PAYMENT_EVENT_TYPES:
        if not event.get("payment_id"):
            errors.append("missing_payment_id")
        if event.get("payment_status") not in PAYMENT_STATUSES:
            errors.append("bad_payment_status")
        if event.get("payment_method") not in PAYMENT_METHODS:
            errors.append("bad_payment_method")
        amt = event.get("amount")
        if not isinstance(amt, (int, float)) or amt <= 0:
            errors.append("bad_amount")
    return errors
