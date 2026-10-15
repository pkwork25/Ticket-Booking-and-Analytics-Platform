import json

from conftest import load_module

fwd = load_module("sqs_to_n8n_handler", "lambdas/sqs_to_n8n/handler.py")

CFG = {"alerts_url": "https://n8n/a", "insights_url": "https://n8n/i", "header_name": "X-T", "token": "secret"}


def rec(mid, body, group="G"):
    return {"messageId": mid, "body": json.dumps(body) if isinstance(body, dict) else body,
            "attributes": {"MessageGroupId": group, "ApproximateReceiveCount": "1"}}


def test_routes_by_type_and_sends_auth_and_idempotency_headers():
    calls = []

    def post(url, payload, headers, timeout):
        calls.append((url, headers))
        return 200

    out = fwd.process_records(
        [rec("1", {"type": "ALERT", "alert_id": "abc"}), rec("2", {"type": "KPI_SNAPSHOT", "snapshot_id": "kpi-1"})],
        CFG, post=post,
    )
    assert out == []
    assert calls[0][0] == "https://n8n/a" and calls[1][0] == "https://n8n/i"
    assert calls[0][1]["X-T"] == "secret" and calls[0][1]["Idempotency-Key"] == "abc"
    assert calls[1][1]["Idempotency-Key"] == "kpi-1"


def test_failed_message_blocks_rest_of_its_fifo_group_only():
    def post(url, payload, headers, timeout):
        return 503 if payload["n"] == 1 else 200

    records = [rec("a", {"type": "ALERT", "n": 1}, "G1"), rec("b", {"type": "ALERT", "n": 2}, "G1"),
               rec("c", {"type": "ALERT", "n": 3}, "G2")]
    out = fwd.process_records(records, CFG, post=post)
    assert out == [{"itemIdentifier": "a"}, {"itemIdentifier": "b"}]


def test_network_error_is_retried_and_poison_message_dropped():
    def post(*_):
        raise TimeoutError("boom")

    out = fwd.process_records([rec("x", "not json"), rec("y", {"type": "ALERT"}, "G9")], CFG, post=post)
    assert out == [{"itemIdentifier": "y"}]
