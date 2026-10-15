"""PySpark tests for the micro-batch transforms (run in CI; skipped if pyspark is not installed)."""

from datetime import datetime, timezone

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from simulator import ChaosConfig, TicketEventSimulator  # noqa: E402
from ticket_etl import transforms as TR  # noqa: E402
from ticket_etl.schema import validate_event  # noqa: E402

# Same columns / types as the Glue catalog table over Kinesis (infra/03-glue-streaming.yaml).
CATALOG_SCHEMA = T.StructType(
    [T.StructField(c, T.StringType()) for c in ["event_id", "event_type", "event_time"]]
    + [T.StructField("schema_version", T.IntegerType()), T.StructField("source", T.StringType())]
    + [T.StructField(c, T.StringType()) for c in
       ["booking_id", "user_id", "event_name", "event_category", "venue_city", "show_time"]]
    + [T.StructField("seats", T.IntegerType()), T.StructField("ticket_amount", T.DoubleType()),
       T.StructField("booking_status", T.StringType())]
    + [T.StructField(c, T.StringType()) for c in
       ["payment_id", "payment_method", "gateway", "payment_status"]]
    + [T.StructField("amount", T.DoubleType()), T.StructField("currency", T.StringType()),
       T.StructField("failure_code", T.StringType()), T.StructField("refund_amount", T.DoubleType())]
)
T0 = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("ticketpulse-tests")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def to_df(spark, events):
    rows = [tuple(e.get(f.name) for f in CATALOG_SCHEMA.fields) for e in events]
    return spark.createDataFrame(rows, CATALOG_SCHEMA)


def events(n=150, **chaos):
    sim = TicketEventSimulator(seed=11, chaos=ChaosConfig(**{"duplicate_rate": 0, "late_rate": 0, "bad_rate": 0, **chaos}))
    for _ in range(n):
        sim.start_booking(T0)
    return sim.drain(T0)


def test_spark_validation_matches_python_contract(spark):
    evs = events(bad_rate=0.2)
    checked = TR.with_validation(TR.normalize(to_df(spark, evs))).select("event_id", "dq_errors").collect()
    spark_bad = {r.event_id for r in checked if r.dq_errors}
    py_bad = {e["event_id"] for e in evs if validate_event(e)}
    assert spark_bad == py_bad and py_bad


def test_dedupe_and_latest_state(spark):
    evs = events(duplicate_rate=0.3)
    valid, _ = TR.split_valid(TR.with_validation(TR.normalize(to_df(spark, evs))))
    deduped = TR.dedupe_events(valid)
    assert deduped.count() == len({e["event_id"] for e in evs})

    current = TR.bookings_current(TR.booking_events(deduped)).collect()
    assert len(current) == 150
    assert {r.booking_status for r in current} <= {"CONFIRMED", "EXPIRED", "CANCELLED"}
    assert all(r.last_event_type != "booking_created" for r in current)


def test_tie_break_prefers_later_lifecycle_step(spark):
    base = {"booking_id": "B1", "payment_id": "P1", "payment_method": "UPI", "gateway": "payu", "amount": 500.0,
            "event_time": "2026-10-15T12:00:00.000Z", "schema_version": 1}
    evs = [{**base, "event_id": "1", "event_type": "payment_initiated", "payment_status": "INITIATED"},
           {**base, "event_id": "2", "event_type": "payment_succeeded", "payment_status": "SUCCEEDED"}]
    df = TR.normalize(to_df(spark, evs))
    (row,) = TR.payments_current(TR.payment_events(df)).collect()
    assert row.payment_status == "SUCCEEDED"


def test_outcome_stats_per_gateway(spark):
    evs = events(n=400, incident_gateway="payu", incident_failure_rate=0.9)
    df = TR.dedupe_events(TR.split_valid(TR.with_validation(TR.normalize(to_df(spark, evs))))[0])
    stats = {r.gateway: r for r in TR.payment_outcome_stats(TR.payment_events(df), "gateway").collect()}
    payu = stats["payu"]
    assert payu.failures / payu.attempts > 0.7
    assert payu.top_failure_code in ("GATEWAY_TIMEOUT", "GATEWAY_5XX")
