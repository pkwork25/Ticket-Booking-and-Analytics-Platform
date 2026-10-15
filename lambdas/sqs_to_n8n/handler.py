"""SQS FIFO -> n8n webhook forwarder.

* Routes ALERT messages to the alerts workflow and KPI_SNAPSHOT messages to the insights workflow.
* Authenticates to n8n with a shared header token kept in Secrets Manager (cached for 5 min).
* Partial batch failures: only failed messages go back to the queue; after 5 receives they move
  to the DLQ. For FIFO ordering, once a message in a group fails, the rest of that group in the
  batch is returned too.
* Sends an Idempotency-Key header so n8n (or anything downstream) can drop redeliveries.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from typing import Callable

log = logging.getLogger()
log.setLevel(logging.INFO)

_SECRET_CACHE: dict = {"value": None, "expires": 0.0}
SECRET_TTL_SECONDS = 300


def get_n8n_config() -> dict:
    if _SECRET_CACHE["value"] and time.time() < _SECRET_CACHE["expires"]:
        return _SECRET_CACHE["value"]
    import boto3

    raw = boto3.client("secretsmanager").get_secret_value(SecretId=os.environ["N8N_SECRET_ARN"])["SecretString"]
    cfg = json.loads(raw)
    _SECRET_CACHE.update(value=cfg, expires=time.time() + SECRET_TTL_SECONDS)
    return cfg


def http_post(url: str, payload: dict, headers: dict, timeout: float) -> int:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (URL comes from our secret)
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code


def route(message: dict, cfg: dict) -> str:
    if message.get("type") == "KPI_SNAPSHOT":
        return cfg["insights_url"]
    return cfg["alerts_url"]


def idempotency_key(message: dict, sqs_message_id: str) -> str:
    return message.get("alert_id") or message.get("snapshot_id") or sqs_message_id


def process_records(
    records: list[dict],
    cfg: dict,
    post: Callable[[str, dict, dict, float], int] = http_post,
    timeout: float = 10.0,
) -> list[dict]:
    """Return SQS batchItemFailures."""
    failures: list[dict] = []
    failed_groups: set[str] = set()
    for rec in records:
        msg_id = rec["messageId"]
        group = rec.get("attributes", {}).get("MessageGroupId", "")
        if group in failed_groups:
            failures.append({"itemIdentifier": msg_id})
            continue
        try:
            body = json.loads(rec["body"])
        except json.JSONDecodeError:
            log.error("dropping non-JSON message %s", msg_id)  # poison message: never retry
            continue
        headers = {
            cfg.get("header_name", "X-TicketPulse-Token"): cfg["token"],
            "Idempotency-Key": idempotency_key(body, msg_id),
            "X-Receive-Count": rec.get("attributes", {}).get("ApproximateReceiveCount", "1"),
        }
        try:
            status = post(route(body, cfg), body, headers, timeout)
        except Exception as exc:  # network errors, timeouts
            log.warning("delivery error for %s: %s", msg_id, exc)
            status = 0
        if 200 <= status < 300:
            log.info("delivered %s type=%s status=%s", msg_id, body.get("type"), status)
            continue
        log.warning("delivery failed for %s (status %s); will retry", msg_id, status)
        failures.append({"itemIdentifier": msg_id})
        failed_groups.add(group)
    return failures


def lambda_handler(event, context):
    cfg = get_n8n_config()
    timeout = float(os.environ.get("HTTP_TIMEOUT_SECONDS", "10"))
    failures = process_records(event.get("Records", []), cfg, timeout=timeout)
    return {"batchItemFailures": failures}
