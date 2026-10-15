"""Scheduled KPI snapshot: Redshift Serverless (Data API) -> SQS FIFO -> n8n -> LLM insights.

Runs a handful of aggregate queries over the last LOOKBACK_MINUTES (plus the previous window for
comparison), assembles a compact JSON snapshot and enqueues it. The LLM only ever sees these
pre-computed aggregates - never raw rows or personal data - which keeps prompts small, cheap and
free of PII.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Callable

log = logging.getLogger()
log.setLevel(logging.INFO)

WINDOW = "event_time >= DATEADD(minute, -CAST(:lookback AS INT), GETDATE())"
PREV_WINDOW = (
    "event_time >= DATEADD(minute, -2 * CAST(:lookback AS INT), GETDATE()) "
    "AND event_time < DATEADD(minute, -CAST(:lookback AS INT), GETDATE())"
)
OUTCOMES = "event_type IN ('payment_succeeded', 'payment_failed')"

QUERIES: dict[str, str] = {
    "totals": f"""
        SELECT COUNT(*) AS attempts,
               SUM(CASE WHEN event_type = 'payment_succeeded' THEN 1 ELSE 0 END) AS succeeded,
               SUM(CASE WHEN event_type = 'payment_failed' THEN 1 ELSE 0 END) AS failed,
               ROUND(100.0 * SUM(CASE WHEN event_type = 'payment_succeeded' THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*), 0), 2) AS success_rate_pct,
               SUM(CASE WHEN event_type = 'payment_succeeded' THEN amount ELSE 0 END) AS captured_inr
        FROM tp.payment_events WHERE {OUTCOMES} AND {WINDOW}""",
    "previous_window": f"""
        SELECT COUNT(*) AS attempts,
               ROUND(100.0 * SUM(CASE WHEN event_type = 'payment_succeeded' THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*), 0), 2) AS success_rate_pct,
               SUM(CASE WHEN event_type = 'payment_succeeded' THEN amount ELSE 0 END) AS captured_inr
        FROM tp.payment_events WHERE {OUTCOMES} AND {PREV_WINDOW}""",
    "by_gateway": f"""
        SELECT gateway, COUNT(*) AS attempts,
               ROUND(100.0 * SUM(CASE WHEN event_type = 'payment_succeeded' THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*), 0), 2) AS success_rate_pct,
               SUM(CASE WHEN event_type = 'payment_failed' THEN amount ELSE 0 END) AS failed_inr
        FROM tp.payment_events WHERE {OUTCOMES} AND {WINDOW}
        GROUP BY gateway ORDER BY attempts DESC""",
    "by_method": f"""
        SELECT payment_method, COUNT(*) AS attempts,
               ROUND(100.0 * SUM(CASE WHEN event_type = 'payment_succeeded' THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*), 0), 2) AS success_rate_pct
        FROM tp.payment_events WHERE {OUTCOMES} AND {WINDOW}
        GROUP BY payment_method ORDER BY attempts DESC""",
    "top_failure_codes": f"""
        SELECT gateway, payment_method, failure_code, COUNT(*) AS failures
        FROM tp.payment_events WHERE event_type = 'payment_failed' AND {WINDOW}
        GROUP BY 1, 2, 3 ORDER BY failures DESC LIMIT 8""",
    "funnel": f"""
        SELECT SUM(CASE WHEN event_type = 'booking_created' THEN 1 ELSE 0 END) AS created,
               SUM(CASE WHEN event_type = 'booking_confirmed' THEN 1 ELSE 0 END) AS confirmed,
               SUM(CASE WHEN event_type = 'booking_expired' THEN 1 ELSE 0 END) AS expired,
               SUM(CASE WHEN event_type = 'booking_cancelled' THEN 1 ELSE 0 END) AS cancelled
        FROM tp.booking_events WHERE {WINDOW}""",
    "top_cities": f"""
        SELECT venue_city, event_category, COUNT(*) AS confirmed_bookings, SUM(ticket_amount) AS gross_inr
        FROM tp.booking_events WHERE event_type = 'booking_confirmed' AND {WINDOW}
        GROUP BY 1, 2 ORDER BY gross_inr DESC LIMIT 5""",
    "refunds": f"""
        SELECT COUNT(*) AS refunds, COALESCE(SUM(refund_amount), 0) AS refunded_inr
        FROM tp.payment_events WHERE event_type = 'refund_processed' AND {WINDOW}""",
}

SINGLE_ROW = {"totals", "previous_window", "funnel", "refunds"}


def _cell(cell: dict):
    if cell.get("isNull"):
        return None
    for key in ("longValue", "doubleValue", "booleanValue"):
        if key in cell:
            return cell[key]
    value = cell.get("stringValue")
    try:  # DECIMAL columns come back as strings
        return float(Decimal(value)) if value is not None and value.replace(".", "", 1).lstrip("-").isdigit() else value
    except Exception:
        return value


def rows_from_result(result: dict) -> list[dict]:
    cols = [c["name"] for c in result.get("ColumnMetadata", [])]
    return [dict(zip(cols, (_cell(c) for c in rec))) for rec in result.get("Records", [])]


def run_query(sql: str, lookback: int, client=None) -> list[dict]:
    import boto3

    client = client or boto3.client("redshift-data")
    stmt = client.execute_statement(
        WorkgroupName=os.environ["REDSHIFT_WORKGROUP"],
        Database=os.environ["REDSHIFT_DB"],
        SecretArn=os.environ["REDSHIFT_SECRET_ARN"],
        Sql=sql,
        Parameters=[{"name": "lookback", "value": str(lookback)}],
    )
    deadline = time.time() + 90
    while True:
        desc = client.describe_statement(Id=stmt["Id"])
        if desc["Status"] == "FINISHED":
            break
        if desc["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(f"query failed: {desc.get('Error')}")
        if time.time() > deadline:
            raise TimeoutError("Redshift Data API statement timed out")
        time.sleep(0.5)
    if not desc.get("HasResultSet"):
        return []
    rows, token = [], None
    while True:
        kwargs = {"Id": stmt["Id"], **({"NextToken": token} if token else {})}
        res = client.get_statement_result(**kwargs)
        rows += rows_from_result(res)
        token = res.get("NextToken")
        if not token:
            return rows


def build_snapshot(results: dict[str, list[dict]], lookback: int, now: datetime) -> dict:
    snap = {
        "type": "KPI_SNAPSHOT",
        "snapshot_id": f"kpi-{now:%Y%m%dT%H%M}-{lookback}m",
        "window_start": (now - timedelta(minutes=lookback)).isoformat(),
        "window_end": now.isoformat(),
        "lookback_minutes": lookback,
        "currency": "INR",
    }
    for name, rows in results.items():
        snap[name] = (rows[0] if rows else {}) if name in SINGLE_ROW else rows

    cur = snap.get("totals", {}).get("success_rate_pct")
    prev = snap.get("previous_window", {}).get("success_rate_pct")
    snap["success_rate_delta_pts"] = round(cur - prev, 2) if cur is not None and prev is not None else None
    funnel = snap.get("funnel", {})
    created = funnel.get("created") or 0
    snap["booking_conversion_pct"] = round(100.0 * (funnel.get("confirmed") or 0) / created, 2) if created else None
    return snap


def lambda_handler(event, context, query: Callable[[str, int], list[dict]] | None = None, sqs=None):
    lookback = int(os.environ.get("LOOKBACK_MINUTES", "60"))
    query = query or run_query
    results = {name: query(sql, lookback) for name, sql in QUERIES.items()}
    snapshot = build_snapshot(results, lookback, datetime.now(timezone.utc))

    if sqs is None:
        import boto3

        sqs = boto3.client("sqs")
    sqs.send_message(
        QueueUrl=os.environ["ALERTS_QUEUE_URL"],
        MessageBody=json.dumps(snapshot, default=str),
        MessageGroupId="KPI_SNAPSHOT",
        MessageDeduplicationId=snapshot["snapshot_id"],
    )
    log.info("enqueued %s attempts=%s", snapshot["snapshot_id"], snapshot.get("totals", {}).get("attempts"))
    return {"snapshot_id": snapshot["snapshot_id"]}
