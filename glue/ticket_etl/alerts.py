"""Real-time alert rules evaluated on every micro-batch.

Pure Python on purpose: the Glue job aggregates in Spark, collects the (tiny) per-gateway /
per-method stats to the driver and calls these functions, so the rules are unit-testable
without Spark or AWS.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True)
class Thresholds:
    failure_rate: float = 0.25
    min_payments: int = 20
    high_value_amount: float = 25000.0


@dataclass
class Alert:
    rule: str
    severity: str  # "critical" | "warning"
    dimension: str  # e.g. "gateway=payu" or "payment_id=PAY-..."
    window_start: str  # ISO-8601, event-time bucket the alert belongs to
    summary: str
    metrics: dict = field(default_factory=dict)
    type: str = "ALERT"

    @property
    def alert_id(self) -> str:
        # Deterministic: the same data in the same event-time window always yields the same id,
        # so a replayed micro-batch is de-duplicated by the SQS FIFO queue.
        raw = f"{self.rule}|{self.dimension}|{self.window_start}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def to_message(self) -> dict:
        body = asdict(self)
        body["alert_id"] = self.alert_id
        return body


def window_bucket(ts: datetime, window_seconds: int) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    epoch = int(ts.timestamp())
    start = epoch - (epoch % window_seconds)
    return datetime.fromtimestamp(start, tz=timezone.utc).isoformat()


def _rate_alerts(stats: list[dict], key: str, rule: str, window: str, t: Thresholds) -> list[Alert]:
    alerts = []
    for row in stats:
        attempts = int(row.get("attempts") or 0)
        failures = int(row.get("failures") or 0)
        if attempts < t.min_payments:
            continue  # too little traffic to be statistically meaningful
        rate = failures / attempts
        if rate <= t.failure_rate:
            continue
        severity = "critical" if rate >= min(2 * t.failure_rate, 0.9) else "warning"
        alerts.append(
            Alert(
                rule=rule,
                severity=severity,
                dimension=f"{key}={row[key]}",
                window_start=window,
                summary=(
                    f"{row[key]} payment failure rate {rate:.0%} "
                    f"({failures}/{attempts}) exceeds {t.failure_rate:.0%}"
                ),
                metrics={
                    "attempts": attempts,
                    "failures": failures,
                    "failure_rate": round(rate, 4),
                    "top_failure_code": row.get("top_failure_code"),
                    "failed_amount_inr": round(float(row.get("failed_amount") or 0), 2),
                },
            )
        )
    return alerts


def detect_alerts(
    gateway_stats: list[dict],
    method_stats: list[dict],
    failed_payments: list[dict],
    batch_max_event_time: datetime,
    window_seconds: int,
    thresholds: Thresholds = Thresholds(),
) -> list[Alert]:
    """Evaluate all rules for one micro-batch.

    gateway_stats / method_stats: rows with attempts, failures, failed_amount, top_failure_code
    and the dimension column (gateway / payment_method). Attempts = terminal payment outcomes
    (succeeded + failed) seen in the batch.
    failed_payments: payment_failed events (payment_id, booking_id, gateway, amount, failure_code).
    """
    window = window_bucket(batch_max_event_time, window_seconds)
    alerts = _rate_alerts(gateway_stats, "gateway", "PAYMENT_FAILURE_SPIKE", window, thresholds)
    alerts += _rate_alerts(method_stats, "payment_method", "PAYMENT_METHOD_DEGRADED", window, thresholds)

    for p in failed_payments:
        amount = float(p.get("amount") or 0)
        if amount < thresholds.high_value_amount:
            continue
        alerts.append(
            Alert(
                rule="HIGH_VALUE_PAYMENT_FAILED",
                severity="warning",
                dimension=f"payment_id={p['payment_id']}",
                window_start=window,
                summary=f"High-value payment of INR {amount:,.0f} failed via {p.get('gateway')} "
                f"({p.get('failure_code')})",
                metrics={
                    "payment_id": p["payment_id"],
                    "booking_id": p.get("booking_id"),
                    "gateway": p.get("gateway"),
                    "payment_method": p.get("payment_method"),
                    "amount_inr": amount,
                    "failure_code": p.get("failure_code"),
                },
            )
        )
    return alerts


def to_sqs_entries(alerts: list[Alert]) -> list[list[dict]]:
    """SendMessageBatch entries, chunked by 10 (the SQS batch limit)."""
    entries = [
        {
            "Id": str(i),
            "MessageBody": json.dumps(a.to_message(), default=str),
            "MessageGroupId": a.rule,
            "MessageDeduplicationId": a.alert_id,
        }
        for i, a in enumerate(alerts)
    ]
    chunks = [entries[i : i + 10] for i in range(0, len(entries), 10)]
    for chunk in chunks:  # Ids must be unique within a batch request only
        for j, e in enumerate(chunk):
            e["Id"] = str(j)
    return chunks
