"""Stream simulated booking + payment events into Kinesis Data Streams.

Examples
  python producer/send_to_kinesis.py --stream ticketpulse-events --rate 5 --duration 600
  python producer/send_to_kinesis.py --stream ticketpulse-events --rate 8 --incident-gateway payu
  python producer/send_to_kinesis.py --dry-run --rate 20 --duration 30 > sample_events.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from simulator import ChaosConfig, TicketEventSimulator  # noqa: E402

log = logging.getLogger("producer")

MAX_RECORDS_PER_PUT = 500
MAX_BYTES_PER_PUT = 5 * 1024 * 1024


def to_record(event: dict) -> dict:
    # Partition by booking_id so all events of one booking land on the same shard, in order.
    key = event.get("booking_id") or event.get("event_id") or "unknown"
    return {"Data": json.dumps(event, separators=(",", ":")).encode(), "PartitionKey": key}


def chunk_records(records: list[dict]) -> list[list[dict]]:
    chunks, current, size = [], [], 0
    for r in records:
        r_size = len(r["Data"]) + len(r["PartitionKey"].encode())
        if current and (len(current) >= MAX_RECORDS_PER_PUT or size + r_size > MAX_BYTES_PER_PUT):
            chunks.append(current)
            current, size = [], 0
        current.append(r)
        size += r_size
    if current:
        chunks.append(current)
    return chunks


def put_with_retries(client, stream: str, records: list[dict], max_attempts: int = 6) -> int:
    """PutRecords is not all-or-nothing: resend only the failed entries with backoff + jitter."""
    pending = records
    for attempt in range(max_attempts):
        resp = client.put_records(StreamName=stream, Records=pending)
        if resp.get("FailedRecordCount", 0) == 0:
            return len(records)
        pending = [rec for rec, res in zip(pending, resp["Records"]) if "ErrorCode" in res]
        sleep = min(0.1 * 2**attempt, 5) + random.uniform(0, 0.1)
        log.warning("throttled: %d records failed, retrying in %.2fs", len(pending), sleep)
        time.sleep(sleep)
    raise RuntimeError(f"{len(pending)} records still failing after {max_attempts} attempts")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--stream", default="ticketpulse-events")
    p.add_argument("--region", default=None)
    p.add_argument("--rate", type=float, default=5.0, help="new bookings per second")
    p.add_argument("--duration", type=int, default=300, help="seconds to run (0 = forever)")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--duplicate-rate", type=float, default=0.02)
    p.add_argument("--late-rate", type=float, default=0.02)
    p.add_argument("--bad-rate", type=float, default=0.005)
    p.add_argument("--incident-gateway", default=None, help="simulate an outage at this gateway (e.g. payu)")
    p.add_argument("--incident-failure-rate", type=float, default=0.6)
    p.add_argument("--dry-run", action="store_true", help="print JSON lines instead of sending")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)

    sim = TicketEventSimulator(
        seed=a.seed,
        chaos=ChaosConfig(a.duplicate_rate, a.late_rate, a.bad_rate, a.incident_gateway, a.incident_failure_rate),
    )
    client = None
    if not a.dry_run:
        import boto3

        client = boto3.client("kinesis", region_name=a.region)

    started = time.monotonic()
    sent, carry, last_log = 0, 0.0, started
    try:
        while a.duration == 0 or time.monotonic() - started < a.duration:
            loop_start = time.monotonic()
            now = datetime.now(timezone.utc)
            carry += a.rate
            for _ in range(int(carry)):
                sim.start_booking(now)
            carry -= int(carry)

            events = sim.tick(now)
            if events:
                if a.dry_run:
                    for e in events:
                        sys.stdout.write(json.dumps(e) + "\n")
                    sent += len(events)
                else:
                    for chunk in chunk_records([to_record(e) for e in events]):
                        sent += put_with_retries(client, a.stream, chunk)
            if time.monotonic() - last_log >= 10:
                log.info("sent=%d pending=%d stats=%s", sent, sim.pending(), sim.stats)
                last_log = time.monotonic()
            time.sleep(max(0.0, 1.0 - (time.monotonic() - loop_start)))
    except KeyboardInterrupt:
        log.info("interrupted")
    log.info("done: sent=%d stats=%s", sent, sim.stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
