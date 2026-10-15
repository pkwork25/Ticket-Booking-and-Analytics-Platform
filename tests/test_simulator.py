from collections import Counter
from datetime import datetime, timezone

from simulator import ChaosConfig, TicketEventSimulator
from ticket_etl.schema import BOOKING_EVENT_TYPES, validate_event

T0 = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)


def generate(n=300, **chaos):
    sim = TicketEventSimulator(seed=7, chaos=ChaosConfig(**{"duplicate_rate": 0, "late_rate": 0, "bad_rate": 0, **chaos}))
    for _ in range(n):
        sim.start_booking(T0)
    return sim, sim.drain(T0)


def test_clean_events_satisfy_the_contract():
    _, events = generate()
    assert events
    bad = [(e, validate_event(e)) for e in events if validate_event(e)]
    assert bad == []


def test_every_booking_reaches_a_terminal_state():
    _, events = generate()
    by_booking = {}
    for e in events:
        if e["event_type"] in BOOKING_EVENT_TYPES:
            by_booking.setdefault(e["booking_id"], set()).add(e["event_type"])
    assert len(by_booking) == 300
    for types in by_booking.values():
        assert "booking_created" in types
        assert types & {"booking_confirmed", "booking_expired"}, types


def test_payment_ids_are_reused_only_within_one_attempt():
    _, events = generate()
    owner = {}
    for e in events:
        if e.get("payment_id"):
            assert owner.setdefault(e["payment_id"], e["booking_id"]) == e["booking_id"]


def test_bad_events_fail_validation():
    _, events = generate(n=50, bad_rate=1.0)
    assert all(validate_event(e) for e in events)


def test_duplicates_share_event_id():
    sim, events = generate(n=100, duplicate_rate=0.5)
    counts = Counter(e["event_id"] for e in events)
    assert sum(c - 1 for c in counts.values()) == sim.stats["duplicates"] > 0


def test_incident_raises_failure_rate_for_one_gateway():
    _, events = generate(n=800, incident_gateway="payu", incident_failure_rate=0.8)
    outcome = Counter((e["gateway"], e["event_type"]) for e in events if e["event_type"] in ("payment_succeeded", "payment_failed"))

    def rate(gw):
        f, s = outcome[(gw, "payment_failed")], outcome[(gw, "payment_succeeded")]
        return f / (f + s)

    assert rate("payu") > 0.6
    assert rate("razorpay") < 0.2


def test_late_events_keep_their_original_event_time():
    sim, events = generate(n=200, late_rate=0.3)
    assert sim.stats["late"] > 0
    assert all("_late" not in e for e in events)
    assert all(validate_event(e) == [] for e in events)
