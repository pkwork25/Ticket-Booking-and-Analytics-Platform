import json
from datetime import datetime, timezone

from conftest import load_module

kpi = load_module("kpi_snapshot_handler", "lambdas/kpi_snapshot/handler.py")
NOW = datetime(2026, 10, 15, 13, 0, tzinfo=timezone.utc)


def fake_results():
    return {
        "totals": [{"attempts": 1000, "succeeded": 900, "failed": 100, "success_rate_pct": 90.0, "captured_inr": 1.2e6}],
        "previous_window": [{"attempts": 950, "success_rate_pct": 94.25, "captured_inr": 1.1e6}],
        "by_gateway": [{"gateway": "payu", "attempts": 250, "success_rate_pct": 70.0, "failed_inr": 90000.0}],
        "by_method": [],
        "top_failure_codes": [],
        "funnel": [{"created": 800, "confirmed": 700, "expired": 90, "cancelled": 10}],
        "top_cities": [],
        "refunds": [{"refunds": 4, "refunded_inr": 8000.0}],
    }


def test_build_snapshot_derives_deltas_and_conversion():
    s = kpi.build_snapshot(fake_results(), 60, NOW)
    assert s["type"] == "KPI_SNAPSHOT" and s["snapshot_id"] == "kpi-20261015T1300-60m"
    assert s["success_rate_delta_pts"] == -4.25
    assert s["booking_conversion_pct"] == 87.5
    assert isinstance(s["totals"], dict) and isinstance(s["by_gateway"], list)


def test_handler_enqueues_fifo_message(monkeypatch):
    monkeypatch.setenv("ALERTS_QUEUE_URL", "https://sqs/q")
    monkeypatch.setenv("LOOKBACK_MINUTES", "60")
    sent = {}

    class FakeSqs:
        def send_message(self, **kw):
            sent.update(kw)

    results = fake_results()
    out = kpi.lambda_handler({}, None, query=lambda sql, lb: results[next(k for k, v in kpi.QUERIES.items() if v == sql)], sqs=FakeSqs())
    body = json.loads(sent["MessageBody"])
    assert sent["MessageGroupId"] == "KPI_SNAPSHOT"
    assert sent["MessageDeduplicationId"] == out["snapshot_id"] == body["snapshot_id"]
    assert body["totals"]["attempts"] == 1000


def test_rows_from_data_api_result():
    res = {
        "ColumnMetadata": [{"name": "gateway"}, {"name": "attempts"}, {"name": "rate"}, {"name": "code"}],
        "Records": [[{"stringValue": "payu"}, {"longValue": 12}, {"stringValue": "93.50"}, {"isNull": True}]],
    }
    assert kpi.rows_from_result(res) == [{"gateway": "payu", "attempts": 12, "rate": 93.5, "code": None}]


def test_queries_are_parameterised_not_formatted():
    for sql in kpi.QUERIES.values():
        assert ":lookback" in sql
