import json
from datetime import datetime, timezone

from ticket_etl.alerts import Thresholds, detect_alerts, to_sqs_entries, window_bucket

TS = datetime(2026, 10, 15, 12, 0, 42, tzinfo=timezone.utc)
T = Thresholds(failure_rate=0.25, min_payments=20, high_value_amount=25000)


def test_failure_spike_detected_and_critical_when_double_threshold():
    gw = [
        {"gateway": "payu", "attempts": 40, "failures": 24, "failed_amount": 50000, "top_failure_code": "GATEWAY_TIMEOUT"},
        {"gateway": "razorpay", "attempts": 100, "failures": 6, "failed_amount": 3000, "top_failure_code": "BANK_TIMEOUT"},
    ]
    alerts = detect_alerts(gw, [], [], TS, 60, T)
    assert [a.dimension for a in alerts] == ["gateway=payu"]
    assert alerts[0].severity == "critical"
    assert alerts[0].metrics["failure_rate"] == 0.6


def test_low_volume_is_ignored():
    gw = [{"gateway": "ccavenue", "attempts": 5, "failures": 5}]
    assert detect_alerts(gw, [], [], TS, 60, T) == []


def test_method_degradation_rule():
    pm = [{"payment_method": "UPI", "attempts": 200, "failures": 60}]
    (a,) = detect_alerts([], pm, [], TS, 60, T)
    assert a.rule == "PAYMENT_METHOD_DEGRADED" and a.severity == "warning"


def test_high_value_failure():
    failed = [
        {"payment_id": "PAY-1", "booking_id": "BKG-1", "gateway": "payu", "amount": 41994.0, "failure_code": "3DS_AUTH_FAILED"},
        {"payment_id": "PAY-2", "booking_id": "BKG-2", "gateway": "payu", "amount": 900.0, "failure_code": "X"},
    ]
    (a,) = detect_alerts([], [], failed, TS, 60, T)
    assert a.rule == "HIGH_VALUE_PAYMENT_FAILED" and a.metrics["payment_id"] == "PAY-1"


def test_alert_id_is_deterministic_per_window():
    gw = [{"gateway": "payu", "attempts": 40, "failures": 30}]
    a1 = detect_alerts(gw, [], [], TS, 60, T)[0]
    a2 = detect_alerts(gw, [], [], TS.replace(second=5), 60, T)[0]  # same minute bucket
    a3 = detect_alerts(gw, [], [], TS.replace(minute=1), 60, T)[0]  # next bucket
    assert a1.alert_id == a2.alert_id != a3.alert_id


def test_window_bucket():
    assert window_bucket(TS, 60) == "2026-10-15T12:00:00+00:00"
    assert window_bucket(TS.replace(tzinfo=None), 300) == "2026-10-15T12:00:00+00:00"


def test_sqs_entries_are_fifo_and_chunked():
    failed = [{"payment_id": f"PAY-{i}", "amount": 30000, "gateway": "payu"} for i in range(23)]
    chunks = to_sqs_entries(detect_alerts([], [], failed, TS, 60, T))
    assert [len(c) for c in chunks] == [10, 10, 3]
    e = chunks[0][0]
    assert e["MessageGroupId"] == "HIGH_VALUE_PAYMENT_FAILED"
    assert e["MessageDeduplicationId"] == json.loads(e["MessageBody"])["alert_id"]
    assert all(len({x["Id"] for x in c}) == len(c) for c in chunks)
